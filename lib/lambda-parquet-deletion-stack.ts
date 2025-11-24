import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

// 環境変数
const bucketName           = "glue-split-job-saxon";  // S3バケット名
const scriptName           = "parquet-splitter.py";   // Glue で実行するスクリプト名
const scriptPrefix         = "src/glue/split/";        // スクリプトの S3 prefix
const inPrefixPyshell      = "data/input/pyshell";    // 入力データの S3 prefix (Pyshell)
const inPrefixRay          = "data/input/ray";        // 入力データの S3 prefix (Ray)
const outPrefixPyshell     = "data/split/pyshell";    // 出力データの S3 prefix (Pyshell)
const outPrefixRay         = "data/split/ray";        // 出力データの S3 prefix (Ray)
const markerPrefixPyshell  = "data/markers/pyshell";  // マーカー（分割情報）の S3 prefix (Pyshell)
const markerPrefixRay      = "data/markers/ray";      // マーカー（分割情報）の S3 prefix (Ray)


// オブジェクト削除は不可は掛からないため Lambda で実装する
export class LambdaParquetDeletionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);



  }
}
