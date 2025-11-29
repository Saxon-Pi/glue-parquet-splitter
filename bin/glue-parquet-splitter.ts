#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { GlueParquetSplitterStack } from '../lib/glue-parquet-splitter-stack';
import { GlueParquetMergeStack} from '../lib/glue-parquet-merge-stack'
import { LambdaParquetDeletionStack } from '../lib/lambda-parquet-deletion-stack';
import { appConfigByStage, AppConfig, Stage } from '../config/config';

const app = new cdk.App();

/*
# stg 環境でデプロイ
cdk deploy -c stage=stg
# prod 環境でデプロイ
cdk deploy -c stage=prod
*/

// context から stage を取る（デフォルトは dev）
const stageFromCtx = (app.node.tryGetContext('stage') ?? 'dev') as Stage;
// 型チェック（dev / stg / prod 以外は NG）
if (!['dev', 'stg', 'prod'].includes(stageFromCtx)) {
  throw new Error(`Unknown stage: ${stageFromCtx}. Use one of: dev, stg, prod`);
}
// appConfig（dev / stg / prod から選択）
const appConfig: AppConfig = appConfigByStage[stageFromCtx];
console.log(appConfig)

new GlueParquetSplitterStack(app, 'GlueParquetSplitterStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  config: appConfig.glueSplit,
});

new GlueParquetMergeStack(app, 'GlueParquetMergeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  config: appConfig.glueMerge,
});

new LambdaParquetDeletionStack(app, 'LambdaParquetMergeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  config: appConfig.lambdaDeletion,
});
