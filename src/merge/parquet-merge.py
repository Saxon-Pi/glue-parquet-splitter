import os
import sys
import logging
from io import BytesIO
from typing import List
import boto3
from botocore.config import Config
import pandas as pd
import ray 

# 環境変数の取得
def get_arg(name: str, default=None):
    if f"--{name}" in sys.argv:
        return sys.argv[sys.argv.index(f"--{name}") + 1]
    return os.environ.get(name, default)

# 環境変数
IN_BUCKET  = get_arg("IN_BUCKET")  # 入力のS3バケット名
OUT_BUCKET = get_arg("OUT_BUCKET") # 出力のS3バケット名
IN_PREFIX  = get_arg("IN_PREFIX")  # 入力データのパス
OUT_PREFIX = get_arg("OUT_PREFIX") # 出力データのパス
MAX_WORKERS = int(get_arg("MAX_WORKERS", 24))  # Ray の最大タスク数

# ログ取得
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
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

# S3バケット内のオブジェクトリストを取得
def list_parquet_keys(bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    # Parquet形式のオブジェクトのキーを取得
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    return keys

# Parquetの読込（データフレーム化）
def read_parquet(bucket: str, key: str) -> pd.DataFrame:
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"].read()
    # pyarrowによるI/O高速化
    return pd.read_parquet(BytesIO(body), engine="pyarrow")

# Ray 用のリモート関数
# -> Glue Ray のワーカー毎にこの関数が実行される
@ray.remote
def read_parquet_remote(bucket: str, key: str) -> pd.DataFrame:
    # データ読込処理の read_parquet() をワーカーで並列実行
    return read_parquet(bucket, key)

# マージ後のcsvファイルをS3バケットに格納
def save_csv_to_s3(df: pd.DataFrame, bucket: str, key: str):
    # DataFrame to CSV（UTF-8 BOM付き）
    csv_bytes = df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")
    s3.put_object(Bucket=bucket, Key=key, Body=csv_bytes)

# マージ対象dir
# (input prefix, output prefix, output filename)
# -> IN_BUCKET, OUT_BUCKET 配下にフォルダが存在するケースを想定
DIR_LIST = [
   #("in-hoge/fuga",               "out-hoge/fuga",                "merge.csv"),
    ("pyshell/0000000001",         "pyshell/0000000001",           "merge.csv"),
    ("ray/0000000001",             "ray/0000000001",               "merge.csv"),
]

def main():
    log.info("Glue Ray job invoked!!!")

    # Glue Ray クラスタに接続（基本的に address="auto" でOK）
    ray.init(address="auto", logging_level=logging.ERROR)
    log.info(f"Ray initialized. MAX_WORKERS={MAX_WORKERS}")

    try:
        for src_name, dest_name, aggregated_file in DIR_LIST:
            src_prefix = f"{IN_PREFIX.rstrip('/')}/{src_name}/"                    # inputのS3 prefix
            dest_key   = f"{OUT_PREFIX.rstrip('/')}/{dest_name}/{aggregated_file}" # outputのS3 key

            # マージ対象Parquetリストの作成
            keys = list_parquet_keys(IN_BUCKET, src_prefix)
            log.info(f"[{src_name}] parquet files: {len(keys)} (example: {keys[0] if keys else 'N/A'})")
            if not keys:
                log.info(f"[{src_name}] no files -> skip")
                continue

            # *** ここから Ray で並列読込 ***

            # 全キーに対して remote タスク（read_parquet）を実行する
            # -> futures は ObjectRef（将来の読込結果を取得するための ID みたいなもの）で返ってくる
            futures = [read_parquet_remote.remote(IN_BUCKET, key) for key in keys]

            df_list: List[pd.DataFrame] = [] # DF 格納用配列
            total = len(futures)

            # # すべてのParquetファイルを逐次読込で結合
            # df_list: List[pd.DataFrame] = []
            # for i, key in enumerate(keys, 1):
            #     df_list.append(read_parquet(IN_BUCKET, key)) # ParquetのDFをlistにappend
            #     if i % 50 == 0:
            #         log.info(f"[{src_name}] read {i}/{len(keys)} files")

            # MAX_WORKERS ごとにまとめて ray.get していく（一度にメモリ上に展開する DF 数に上限を設けて安定化）
            # -> バッチ分の ObjectRef を解決して読込結果の pd.DataFrame のリストにする
            for i in range(0, total, MAX_WORKERS):
                batch_futs = futures[i:i + MAX_WORKERS]
                batch_df_list = ray.get(batch_futs)
                df_list.extend(batch_df_list)

            # ****************************

            df = pd.concat(df_list, ignore_index=True) # listのDFを単一のDFにマージ

            # 作成したデータフレームをソート
            # date でソート
            if "date" in df.columns:
                df.sort_values(by=["date"], ascending=False, inplace=True)

            # データフレームをCSVとして出力
            log.info(f"[{src_name}] write -> s3://{OUT_BUCKET}/{dest_key}")
            save_csv_to_s3(df, OUT_BUCKET, dest_key)

        log.info("Done.")

    finally:
        # Ray クラスタから切断
        ray.shutdown()

if __name__ == "__main__":
    main()
