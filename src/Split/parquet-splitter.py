import os
import sys
import json
import logging
from io import BytesIO
from typing import List, Set, Dict
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
from botocore.config import Config
import pandas as pd
import time

# 環境変数の取得
def get_arg(name: str, default=None):
    if f"--{name}" in sys.argv:
        return sys.argv[sys.argv.index(f"--{name}") + 1]
    return os.environ.get(name, default)

# 環境変数
KIND          = get_arg("KIND")          # 識別子
IN_BUCKET     = get_arg("IN_BUCKET")     # 入力のS3バケット名
OUT_BUCKET    = get_arg("OUT_BUCKET")    # 出力のS3バケット名
IN_PREFIX     = get_arg("IN_PREFIX")     # 入力データのパス
OUT_PREFIX    = get_arg("OUT_PREFIX")    # 出力データのパス
MARKER_PREFIX = get_arg("MARKER_PREFIX") # マーカー格納パス
MAX_WORKERS   = int(get_arg("MAX_WORKERS", "24")) # s3.put_object の並列数（24〜48程度）

# ログ取得
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True,
)
log = logging.getLogger(__name__)

# boto3 client（リトライ/コネクション設定）
boto_cfg = Config(
    retries={'max_attempts': 10},
    max_pool_connections=64,
    connect_timeout=10,
    read_timeout=120,
)
s3 = boto3.client("s3", config=boto_cfg)

# S3バケット内のオブジェクトリストを取得 (suffixで 入力 or 完了マーカー を判定)
# -> .json / .parquet ファイルの両方に対応（他拡張子 OK）
def list_keys_with_suffix(bucket: str, prefix: str, suffix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            # 形式が引数(suffix)と一致するオブジェクトのキーを取得
            if k.endswith(suffix):
                keys.append(k)
    return keys

# 入力データのオブジェクトリストからファイル名（=日付）を抽出
# -> ファイル例: 20251107.parquet, 20251114.parquet, 20251121.parquet
# -> 出力: [20251107, 20251114, 20251121]
def extract_dates_from_keys(keys: List[str]) -> Set[str]:
    dates: Set[str] = set()
    # リスト要素から末尾8文字（.parquet）を除去 ("YYYYMMDD.parquet" -> "YYYYMMDD")
    for k in keys:
        base = k.rsplit("/", 1)[-1]
        if base.endswith(".parquet"):
            dates.add(base[:-8])
    return dates

# 完了マーカーのオブジェクトリストからファイル名（=日付）を抽出
# -> ファイル例: 20251107.json, 20251114.json, 20251121.json
# -> 出力: [20251107, 20251114, 20251121]
def extract_dates_from_marker_keys(keys: List[str]) -> Set[str]:
    dates: Set[str] = set()
    # リスト要素から末尾5文字（.json）を除去 ("YYYYMMDD.json" -> "YYYYMMDD")
    for k in keys:
        base = k.rsplit("/", 1)[-1]
        if base.endswith(".json"):
            dates.add(base[:-5])
    return dates

# Parquetの読込（read_parquet）
def read_parquet_s3(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    # pyarrowによるI/O高速化
    return pd.read_parquet(BytesIO(body), engine="pyarrow")

# 分割後のParquetをS3バケットに格納
def put_parquet_bytes_(bucket: str, key: str, data: bytes):
    s3.put_object(Bucket=bucket, Key=key, Body=data)

# データフレームをParquetに変換
def df_to_parquet_bytes_(df: pd.DataFrame) -> bytes:
    # 圧縮snappy、インデックス無効、pyarrow明示
    buf = BytesIO()
    df.to_parquet(buf, engine="pyarrow", compression="snappy", index=False)
    return buf.getvalue()

# s3.put_object の並列実行 (戻り値の out_key を 完了マーカー用に収集)
def write_one_partition(Item_ID_df: pd.DataFrame, out_bucket: str, out_key: str) -> str:
    payload = df_to_parquet_bytes_(Item_ID_df)
    put_parquet_bytes_(out_bucket, out_key, payload)
    return out_key

# 完了マーカーのjsonをバケットに格納
def put_json(bucket: str, key: str, obj: dict):
    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(obj, ensure_ascii=False).encode("utf-8"),
        ContentType="application/json",
    )

# データ分割処理
def process_kind(kind: str, in_bucket: str, in_prefix: str, out_bucket: str, out_prefix: str, marker_prefix: str) -> Dict:
    # 末尾スラッシュ正規化 (安全策)
    in_prefix     = in_prefix.rstrip('/') + '/'
    out_prefix    = out_prefix.rstrip('/') + '/'
    marker_prefix = marker_prefix.rstrip('/') + '/'
    
    # 入力オブジェクトリストの取得
    start      = time.perf_counter() # timer
    in_keys    = list_keys_with_suffix(in_bucket, in_prefix, ".parquet")
    elapsed_in = time.perf_counter() - start
    log.info(f"[list] in_keys total={len(in_keys)} objects, elapsed={elapsed_in:.2f} sec")
    # 完了マーカーオブジェクトリストの取得
    start       = time.perf_counter() # timer
    marker_keys = list_keys_with_suffix(out_bucket, marker_prefix, ".json")
    elapsed_out = time.perf_counter() - start
    log.info(f"[list] marker_keys total={len(marker_keys)} objects, elapsed={elapsed_out:.2f} sec")

    # リストを日付形式に変換
    input_dates   = extract_dates_from_keys(in_keys)          # 入力データの日付
    done_dates  = extract_dates_from_marker_keys(marker_keys) # 完了マーカーの日付
    # 日付の差分（=処理の対象オブジェクト名）を取得
    missing_dates = sorted(input_dates - done_dates)

    log.info(f"[{kind}] input={len(input_dates)} output={len(done_dates)} missing={len(missing_dates)}")

    # データ分割処理の実行
    processed = 0
    for date in missing_dates:
        in_key = f"{in_prefix}{date}.parquet"
        # 対象Parquetの読込
        try:
            df = read_parquet_s3(in_bucket, in_key)
        # キーが存在しない場合のエラーハンドリング
        except s3.exceptions.NoSuchKey:
            log.warning(f"[{kind}] not found: s3://{in_bucket}/{in_key}")
            continue
        # データが空だった場合のエラーハンドリング
        if df.empty:
            log.info(f"[{kind}] empty: {in_key}")
            continue
        # "Item_ID"列が存在しない場合のエラーハンドリング
        if "Item_ID" not in df.columns:
            raise ValueError(f"'Item_ID' column missing in {in_key}")

        # s3.put_object 並列実行
        outputs_written: List[str] = [] # 分割後のキーを格納するリスト
        futures = []
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
            # Item_IDコードごとにグループ化し、各グループを個別のファイルに保存
            for Item_ID, Item_ID_df in df.groupby("Item_ID"):
                out_key = f"{out_prefix}{Item_ID}/{date}.parquet"
                futures.append(ex.submit(write_one_partition, Item_ID_df, out_bucket, out_key))
            for f in as_completed(futures):
                out_key = f.result() # 例外伝播 & out_key の取得
                outputs_written.append(f"s3://{out_bucket}/{out_key}")
        
        log.info(f"[OK] {kind} {date}: {df['Item_ID'].nunique()} Item_IDs -> parallel PUT x{len(futures)}")

        # 完了マーカーを JSON で作成
        marker_key = f"{marker_prefix}{date}.json"
        marker_doc = {
            "kind": kind,
            "date": date,
            "input_key": f"s3://{in_bucket}/{in_key}",
            "outputs": outputs_written, # この日付の全ての分割オブジェクトのフルS3キーの一覧
            "output_count": len(outputs_written),
            "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        put_json(out_bucket, marker_key, marker_doc)
        log.info(f"[MARK] written -> s3://{out_bucket}/{marker_key}")

        processed += 1

    return {
        "kind": kind,                       # pyshell
        "input_files": len(input_dates),    # 入力ファイル数
        "output_files": len(done_dates),    # 出力ファイル数
        "missing_dates": missing_dates,     # 入力-出力の差分ファイル（日付）
        "processed_days": processed,        # 分割対象ファイル数
    }

def main():
    log.info("Glue job invoked!!!")
    result = process_kind(KIND, IN_BUCKET, IN_PREFIX, OUT_BUCKET, OUT_PREFIX, MARKER_PREFIX)
    log.info(f"{result}")

if __name__ == "__main__":
    main()
