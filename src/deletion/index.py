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

    body = resp["Body"].read()
    try:
        marker = json.loads(body)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"Marker is not valid JSON: s3://{bucket}/{marker_key} ({e})"
        ) from e

    return marker, marker_key


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

    # json の "outputs" を取得（削除対象オブジェクトURLのリスト）
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

    return result
