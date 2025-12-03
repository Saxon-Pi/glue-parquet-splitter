import { Stack, StackProps } from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as glue from 'aws-cdk-lib/aws-glue';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { GlueMergeConfig } from '../config/config';

export interface GlueParquetMergeStackProps extends StackProps {
  config: GlueMergeConfig;
}          

export class GlueParquetMergeStack extends Stack {
  constructor(scope: Construct, id: string, props: GlueParquetMergeStackProps) {
    super(scope, id, props);

    const {
      bucketName,     // S3バケット名
      scriptName,     // Glue で実行するスクリプト名
      scriptPrefix,   // スクリプトの S3 prefix
      inPrefix,       // 入力データの S3 prefix
      outPrefix,      // 出力データの S3 prefix
    } = props.config;

    const Bucket = s3.Bucket.fromBucketName(this, 'OutBucket', bucketName);

    const role = new iam.Role(this, 'GlueJobRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
    });
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['logs:CreateLogGroup','logs:CreateLogStream','logs:PutLogEvents'],
      resources: ['*'],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:ListBucket'],
      resources: [Bucket.bucketArn],
      conditions: {
        StringLike: { 's3:prefix': [
          `${inPrefix}*`, `${outPrefix}*`,
          `${scriptPrefix}*`, `${scriptPrefix}jobs/*`, 
        ] }
      },
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:GetObject'],
      resources: [
        Bucket.arnForObjects(`${inPrefix}*`),
        Bucket.arnForObjects(`${scriptPrefix}${scriptName}`),
      ],
    }));
    role.addToPolicy(new iam.PolicyStatement({
      actions: ['s3:PutObject'],
      resources: [
        Bucket.arnForObjects(`${outPrefix}*`),
        Bucket.arnForObjects(`${scriptPrefix}jobs/*`), 
      ],
    }));

    /// Glue Rayジョブ
    const job = new glue.CfnJob(this, 'ParquetMergeRayJob', {
      name: 'parquet-merge-ray',
      role: role.roleArn,
      glueVersion: '4.0',       // コンソールでは4.0固定のため
      workerType: 'Z.2X',       // Ray専用 workerType
      numberOfWorkers: 2,       // 2以上を設定
      command: {
        name: 'glueray',
        runtime: 'Ray2.4',
        scriptLocation: `s3://${bucketName}/${scriptPrefix}${scriptName}`,
      },
      defaultArguments: {
        '--IN_BUCKET': Bucket.bucketName,
        '--OUT_BUCKET': Bucket.bucketName,
        '--IN_PREFIX': inPrefix,
        '--OUT_PREFIX': outPrefix,
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
