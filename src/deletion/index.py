import os
import json
from urllib.parse import urlparse
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

# 環境変数
MARKER_BUCKET_NAME = os.environ.get("MARKER_BUCKET_NAME")

s3 = boto3.client(
    "s3",
    config=Config(
        retries={"max_attempts": 10},
    ),
)
