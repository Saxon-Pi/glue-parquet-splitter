import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { GlueSplitConfig } from '../config/config';

// *****************************************************************************************
// input の .parquet ファイルを分割する Glue job を作成するスタック
// 入力データの 'item_id' ごとに個別のファイルを作成して split/<item_id> フォルダに格納する
// 分割したファイルごとに markers ファイルを作成し、分割後のオブジェクト URI を記載している（削除処理で使用）
// 処理済みのデータサンプルは sample-data フォルダに格納している
// 入力ファイルのサイズに合わせて pyshell / ray の両方でジョブを作成している（処理は同じ）
// *****************************************************************************************

export interface GlueParquetSplitterProps extends StackProps {
  config: GlueSplitConfig;
}  

export class GlueParquetSplitterStack extends Stack {
  constructor(scope: Construct, id: string, props: GlueParquetSplitterProps) {
    super(scope, id, props);

    // python スクリプトは pyshell, ray 共通、入出力と完了マーカーは別 prefix に格納
    const {
      bucketName,          // S3バケット名
      scriptName,          // Glue で実行するスクリプト名
      scriptPrefix,        // スクリプトの S3 prefix
      inPrefixPyshell,     // 入力データの S3 prefix (Pyshell)
      inPrefixRay,         // 入力データの S3 prefix (Ray)
      outPrefixPyshell,    // 出力データの S3 prefix (Pyshell)
      outPrefixRay,        // 出力データの S3 prefix (Ray)
      markerPrefixPyshell, // マーカー（分割情報）の S3 prefix (Pyshell)
      markerPrefixRay,     // マーカー（分割情報）の S3 prefix (Ray)
    } = props.config;

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
    const jobRay = new glue.CfnJob(this, 'ParquetSplitterRayJob', {
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
