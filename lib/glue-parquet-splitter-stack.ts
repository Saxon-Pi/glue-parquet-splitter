import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

// 環境変数
const bucketName         = "glue-split-job-saxon";  // S3バケット名
const scriptName         = "parquet-splitter.py";   // Glue で実行するスクリプト名
const scriptPrefix       = "src/glue/";             // スクリプトの S3 prefix
const inPrefix           = "data/input/";           // 入力データの S3 prefix
const outPrefix          = "data/output/";          // 出力データの S3 prefix
const markerPrefix       = "data/markers/";         // マーカー（分割情報）の S3 prefix

export class GlueParquetSplitterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 バケットは既存のものを使用
    const Bucket = s3.Bucket.fromBucketName(this, 'OutBucket', bucketName);

    // Glue 実行用ロール
    const role = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    // CloudWatch Logs 権限
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents'],
      resources: ['*'],
    }));
    // S3 アクセス権限（List）
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:ListBucket'],
      resources: [Bucket.bucketArn],
      conditions: {
        StringLike: { 's3:prefix': [
          `${inPrefix}*`, `${outPrefix}*`,
          `${scriptPrefix}*`, `${scriptPrefix}jobs/*`, 
          `${markerPrefix}`, `${markerPrefix}*`,
        ] }
      },
    }));
    // S3 アクセス権限（Get）
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject'],
      resources: [
        Bucket.arnForObjects(`${inPrefix}*`),
        Bucket.arnForObjects(`${scriptPrefix}${scriptName}`),
        Bucket.arnForObjects(`${markerPrefix}*`),
      ],
    }));
    // S3 アクセス権限（Put）
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [
        Bucket.arnForObjects(`${outPrefix}*`),
        Bucket.arnForObjects(`${markerPrefix}*`),
      ],
    }));

    // Glue PythonShell ジョブ
    const job = new glue.CfnJob(this, 'ParquetSplitterPyshellJob', {
      name: 'parquet-splitter-pyshell',
      role: role.roleArn,
      command: {
        name: 'pythonshell',
        pythonVersion: '3.9',
        scriptLocation: `s3://${bucketName}/${scriptPrefix}${scriptName}`,
      },
      maxCapacity: 1, // 1 DPU
      timeout: 120,   // 2 hour
      defaultArguments: {
        '--KIND': 'pyshell',
        '--IN_BUCKET': Bucket.bucketName,
        '--OUT_BUCKET': Bucket.bucketName,
        '--IN_PREFIX': inPrefix,
        '--OUT_PREFIX': outPrefix,
        '--MARKER_PREFIX': markerPrefix,
        '--MAX_WORKERS': '24',
        '--additional-python-modules': [
          'numpy==1.26.4',
          'pandas==2.2.2',
          'pyarrow==14.0.2',
          'awswrangler==3.7.2'
        ].join(','),
      },
    });

  }
}
