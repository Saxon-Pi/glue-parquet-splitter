import * as cdk from 'aws-cdk-lib/core';
import { Construct } from 'constructs';
import { Stack, StackProps, Duration, Tags } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';

// 環境変数
const bucketName = "glue-split-job-saxon";  // S3バケット名

export class LambdaParquetDeletionStack extends Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    const configBucket = s3.Bucket.fromBucketName(
      this,
      'ConfigBucket',
      bucketName,
    );

    const parquetDeletionFunc = new lambda.Function(this, 'parquetDeletionFunc',
      {
        functionName: 'parquetDeletionFunc',
        code: lambda.Code.fromAsset('./src/deletion'),
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: 'index.lambda_handler',
        timeout: Duration.minutes(15),
        environment: {
          MARKER_BUCKET_NAME: bucketName,
        },
      },
    );
    parquetDeletionFunc.addToRolePolicy(
      new iam.PolicyStatement({
        effect: iam.Effect.ALLOW,
        actions: ['s3:GetObject', 's3:DeleteObject'],
        resources: [
          // 完了マーカー prefix
          configBucket.arnForObjects('data/markers/*'),
          // 削除対象オブジェクト prefix
          configBucket.arnForObjects('data/split/*'),
        ],
      }),
    );
  }
}
