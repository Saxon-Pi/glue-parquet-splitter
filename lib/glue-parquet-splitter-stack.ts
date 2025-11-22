import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

// 環境変数
// python スクリプトは pyshell, ray 共通、入出力と完了マーカーは別 prefix に格納
const bucketName           = "glue-split-job-saxon";  // S3バケット名
const scriptName           = "parquet-splitter.py";   // Glue で実行するスクリプト名
const scriptPrefix         = "src/glue/";             // スクリプトの S3 prefix
const inPrefixPyshell      = "data/input/pyshell";    // 入力データの S3 prefix (Pyshell)
const inPrefixRay          = "data/input/ray";        // 入力データの S3 prefix (Ray)
const outPrefixPyshell     = "data/output/pyshell";   // 出力データの S3 prefix (Pyshell)
const outPrefixRay         = "data/output/ray";       // 出力データの S3 prefix (Ray)
const markerPrefixPyshell  = "data/markers/pyshell";  // マーカー（分割情報）の S3 prefix (Pyshell)
const markerPrefixRay      = "data/markers/ray";      // マーカー（分割情報）の S3 prefix (Ray)

export class GlueParquetSplitterStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // S3 バケットは既存のものを使用
    const Bucket = s3.Bucket.fromBucketName(this, 'OutBucket', bucketName);

    // Glue(PythonShell) 実行用ロール
    const rolePyshell = new iam.Role(this, 'GlueJobRolePyshell', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    // CloudWatch Logs 権限
    rolePyshell.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents'],
      resources: ['*'],
    }));
    // S3 アクセス権限（List）
    rolePyshell.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:ListBucket'],
      resources: [Bucket.bucketArn],
      conditions: {
        StringLike: { 's3:prefix': [
          `${inPrefixPyshell}*`, `${outPrefixPyshell}*`,
          `${scriptPrefix}*`, `${scriptPrefix}jobs/*`, 
          `${markerPrefixPyshell}`, `${markerPrefixPyshell}*`,
        ] }
      },
    }));
    // S3 アクセス権限（Get）
    rolePyshell.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject'],
      resources: [
        Bucket.arnForObjects(`${inPrefixPyshell}*`),
        Bucket.arnForObjects(`${scriptPrefix}${scriptName}`),
        Bucket.arnForObjects(`${markerPrefixPyshell}*`),
      ],
    }));
    // S3 アクセス権限（Put）
    rolePyshell.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [
        Bucket.arnForObjects(`${outPrefixPyshell}*`),
        Bucket.arnForObjects(`${markerPrefixPyshell}*`),
      ],
    }));

    // Glue PythonShell ジョブ
    const jobPyshell = new glue.CfnJob(this, 'ParquetSplitterPyshellJob', {
      name: 'parquet-splitter-pyshell',
      role: rolePyshell.roleArn,
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
        '--IN_PREFIX': inPrefixPyshell,
        '--OUT_PREFIX': outPrefixPyshell,
        '--MARKER_PREFIX': markerPrefixPyshell,
        '--MAX_WORKERS': '24',
        '--additional-python-modules': [
          'numpy==1.26.4',
          'pandas==2.2.2',
          'pyarrow==14.0.2',
          'awswrangler==3.7.2'
        ].join(','),
      },
    });

    // Glue(Ray) 実行用ロール
    const roleRay = new iam.Role(this, 'GlueJobRoleRay', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    roleRay.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents'],
      resources: ['*'],
    }));
    roleRay.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:ListBucket'],
      resources: [Bucket.bucketArn],
      conditions: {
        StringLike: { 's3:prefix': [
          `${inPrefixRay}*`, `${outPrefixRay}*`,
          `${scriptPrefix}*`, `${scriptPrefix}jobs/*`, 
          `${markerPrefixRay}`, `${markerPrefixRay}*`,
        ] }
      },
    }));
    roleRay.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject'],
      resources: [
        Bucket.arnForObjects(`${inPrefixRay}*`),
        Bucket.arnForObjects(`${scriptPrefix}${scriptName}`),
        Bucket.arnForObjects(`${markerPrefixRay}*`),
      ],
    }));
    roleRay.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [
        Bucket.arnForObjects(`${outPrefixRay}*`),
        Bucket.arnForObjects(`${scriptPrefix}jobs/*`), 
        Bucket.arnForObjects(`${markerPrefixRay}*`),
      ],
    }));

    // Glue Rayジョブ
    const jobRay = new glue.CfnJob(this, 'JanSplitRayJob', {
      name: 'parquet-splitter-ray',
      role: roleRay.roleArn,
      glueVersion: '4.0',       // コンソールでは4.0固定のため
      workerType: 'Z.2X',       // Ray専用 workerType
      numberOfWorkers: 2,       // 2以上を設定
      command: {
        name: 'glueray',
        runtime: 'Ray2.4',
        scriptLocation: `s3://${bucketName}/${scriptPrefix}${scriptName}`,
      },
      defaultArguments: {
        '--KIND': 'ray',
        '--IN_BUCKET': Bucket.bucketName,
        '--OUT_BUCKET': Bucket.bucketName,
        '--IN_PREFIX': inPrefixRay,
        '--OUT_PREFIX': outPrefixRay,
        '--MARKER_PREFIX': markerPrefixRay,
        '--MAX_WORKERS': '24',
        '--pip-install': [
          'numpy==1.26.4',
          'pandas==2.2.2',
          'pyarrow==14.0.2',
          'awswrangler==3.7.2'
        ].join(','),
      },
    });

  }
}
