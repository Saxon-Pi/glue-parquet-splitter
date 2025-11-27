import os
import json
from urllib.parse import urlparse
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# 完了マーカーイメージ（YYYYMMDD.json）
# {"kind": "pyshell", "date": "20251107", "input_key": "s3://glue-split-job-saxon/data/input/pyshell/20251107.parquet",
#  "outputs": ["s3://glue-split-job-saxon/data/output/pyshell/0000000002/20251107.parquet",
#              ... ,
#              "s3://glue-split-job-saxon/data/output/pyshell/0000000020/20251107.parquet"],
#  "output_count": 30, "generated_at": "2025-11-22T03:25:16Z"}

# オブジェクト削除はバケット名とキーを別々に渡す必要があるためパースが必要
# s3.delete_object(
#   Bucket="glue-split-job-saxon",
#   Key="data/output/pyshell/0000000020/20251107.parquet",
# )

# 環境変数
MARKER_BUCKET_NAME = os.environ.get("MARKER_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    config=Config(
        retries={"max_attempts": 10},
    ),
)

# 完了マーカーに記載のオブジェクトURLをバケット名とキーに分離
def parse_s3_url(s3_url: str):
    if not s3_url.startswith("s3://"):
        raise ValueError(f"Invalid S3 URL (must start with s3://): {s3_url}")
    parsed = urlparse(s3_url)
    """
    s3_url = "s3://glue-split-job-saxon/data/output/pyshell/0000000020/20251107.parquet"
    上記の s3_url なら urlparse の結果は以下のようになる

    ParseResult(
    scheme='s3',
    netloc='glue-split-job-saxon',
    path='/data/output/pyshell/0000000020/20251107.parquet',
    params='',
    query='',
    fragment=''
    )
    """
    bucket = parsed.netloc # バケット名の抽出
    # path から先頭の '/' を削る
    key = parsed.path.lstrip("/")
    # バケット名とキーの取得ができなかった場合はエラー
    if not bucket or not key:
        raise ValueError(f"Invalid S3 URL (bucket or key missing): {s3_url}")
    return bucket, key

# 完了マーカーの読み込み（キー: data/markers/{kind}/{YYYYMMDD}.json）
def load_marker(bucket: str, kind: str, date_str: str) -> dict:
    # 引数の kind と date からキーを作成
    marker_key = f"data/markers/{kind}/{date_str}.json"
    # キーからオブジェクトを取得（get）
    try:
        resp = s3.get_object(Bucket=bucket, Key=marker_key)
    except ClientError as e:
        raise RuntimeError(
            f"Failed to load marker: s3://{bucket}/{marker_key} ({e})"
        ) from e
    # get結果から S3オブジェクトの中身（jsonファイルの中身）を読み込む
    body = resp["Body"].read()
    try:
        # body の json文字列を pythonオブジェクト（dict）に変換
        marker = json.loads(body)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Marker is not valid JSON: s3://{bucket}/{marker_key} ({e})"
        ) from e
    return marker, marker_key

# "outputs" リスト内の S3 URL オブジェクトを一括削除
def delete_objects_from_outputs(outputs):
    # バケットごとにまとめる（異なるバケットが混ざっている可能性も考慮）
    bucket_to_keys = {}
    for url in outputs:
        bucket, key = parse_s3_url(url) # URL からバケット名とキーを取得
        bucket_to_keys.setdefault(bucket, []).append(key)
    """
    bucket_to_keys = {
    "glue-split-job-saxon": [
        {"Key": "data/split/pyshell/0000000001/20251107.parquet"},
        {"Key": "data/split/pyshell/0000000002/20251107.parquet"},
        # ...
    ],
    "another-bucket": [
        {"Key": "data/split/ray/0000000001/20251107.parquet"},
        # ...
    ],
    """
    total_deleted = 0
    errors = []

    # bucket_to_keys のアイテムごとにオブジェクトを削除
    for bucket, keys in bucket_to_keys.items():
        # 1000 件ずつバッチに分割
        for i in range(0, len(keys), 1000):       # i=0, i=1000, i=2000, ...
            chunk = keys[i : i + 1000]            # 0~999, 1000~1999, 2000~2999, ...
            objects = [{"Key": k} for k in chunk] # 削除対象オブジェクトの作成
            """
            objects = [
                {"Key": "data/split/pyshell/0000000001/20251107.parquet"},
                {"Key": "data/split/pyshell/0000000002/20251107.parquet"},
                ...
            ]
            """
            # オブジェクト削除
            try:
                resp = s3.delete_objects(
                    Bucket=bucket,
                    Delete={"Objects": objects, "Quiet": True},
                )
            except ClientError as e:
                errors.append(
                    f"delete_objects failed for bucket={bucket}, keys(sample)={chunk[:3]}: {e}"
                )
                continue

            # 削除成功件数のカウント
            deleted_list = resp.get("Deleted", [])
            total_deleted += len(deleted_list)
            
            # エラーリスト
            err_list = resp.get("Errors", [])
            if err_list:
                # ログ出力のために 3件 だけエラーを記録
                errors.append(
                    f"S3 reported delete errors for bucket={bucket}: {err_list[:3]}"
                )
    return total_deleted, errors

"""
Lambda 実行 event は以下を想定
    {
    "kind": "pyshell" or "ray",
    "date": "20251107"  // YYYYMMDD
    }
"""
def lambda_handler(event, context):
    kind = event.get("kind")
    date_str = event.get("date")

    # event 内容に不備がある場合はエラー
    if not kind or not date_str:
        raise ValueError("event.kind と event.date (YYYYMMDD) は必須です")

    if kind not in ("pyshell", "ray"):
        raise ValueError(f"kind は 'pyshell' か 'ray' にしてください. 入力値: {kind}")

    if not MARKER_BUCKET_NAME:
        raise RuntimeError("環境変数 MARKER_BUCKET_NAME が設定されていません")

    # 完了マーカーの読み込み
    marker, marker_key = load_marker(MARKER_BUCKET_NAME, kind, date_str)

    # marker（dict）から "outputs" を取得（削除対象オブジェクトURLのリスト）
    outputs = marker.get("outputs")
    # outputs がリスト or 存在しない場合は何もせず終了
    if not isinstance(outputs, list) or not outputs:
        return {
            "markerBucket": MARKER_BUCKET_NAME,
            "markerKey": marker_key,
            "deleted": 0,
            "mode": "no_outputs",
        }

    # 対象オブジェクトの削除
    total_deleted, errors = delete_objects_from_outputs(outputs)

    result = {
        "markerBucket": MARKER_BUCKET_NAME,
        "markerKey": marker_key,
        "kind": kind,
        "date": date_str,
        "outputsCount": len(outputs),
        "deleted": total_deleted,
        "errors": errors,
    }

    # ログにも出しておく
    print(json.dumps(result, ensure_ascii=False))

    return result
