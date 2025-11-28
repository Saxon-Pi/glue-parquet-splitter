#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib';
import { GlueParquetSplitterStack } from '../lib/glue-parquet-splitter-stack';
import { GlueParquetMergeStack} from '../lib/glue-parquet-merge-stack'
import { LambdaParquetDeletionStack } from '../lib/lambda-parquet-deletion-stack';
import { glueMergeConfigByStage, Stage } from '../config/config';

const app = new cdk.App();

/*
# stg 環境でデプロイ
cdk deploy -c stage=stg
# prod 環境でデプロイ
cdk deploy -c stage=prod
*/

// context から stage を取る（デフォルトは dev）
const stageFromCtx = app.node.tryGetContext('stage') ?? 'dev';
// 型チェック（dev / stg / prod 以外は NG）
if (!['dev', 'stg', 'prod'].includes(stageFromCtx)) {
  throw new Error(`Unknown stage: ${stageFromCtx}. Use one of: dev, stg, prod`);
}
// Stage 型にキャスト
const stage = stageFromCtx as Stage;
// config
const config = glueMergeConfigByStage[stage];

new GlueParquetSplitterStack(app, 'GlueParquetSplitterStack', {

});

new GlueParquetMergeStack(app, 'GlueParquetMergeStack', {
  env: {
    account: process.env.CDK_DEFAULT_ACCOUNT,
    region: process.env.CDK_DEFAULT_REGION,
  },
  config,
});

new LambdaParquetDeletionStack(app, 'LambdaParquetMergeStack', {

});
