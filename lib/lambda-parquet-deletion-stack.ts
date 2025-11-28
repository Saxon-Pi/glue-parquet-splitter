import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import { Stack, StackProps, Duration, Tags } from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as s3 from 'aws-cdk-lib/aws-s3';
import { LambdaDeletionConfig } from '../config/config';

export interface LambdaParquetDeletionStackProps extends cdk.StackProps {
  config: LambdaDeletionConfig;
}

export class LambdaParquetDeletionStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: LambdaParquetDeletionStackProps) {
    super(scope, id, props);

    const {
      markerBucketName
    } = props.config

    const configBucket = s3.Bucket.fromBucketName(
      this,
      'ConfigBucket',
      markerBucketName,
    );

    const parquetDeletionFunc = new lambda.Function(this, 'parquetDeletionFunc',
      {
        functionName: 'parquetDeletionFunc',
        code: lambda.Code.fromAsset('./src/deletion'),
        runtime: lambda.Runtime.PYTHON_3_13,
        handler: 'index.lambda_handler',
        timeout: Duration.minutes(15),
        environment: {
          MARKER_BUCKET_NAME: markerBucketName,
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
