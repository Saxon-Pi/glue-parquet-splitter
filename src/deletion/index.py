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
