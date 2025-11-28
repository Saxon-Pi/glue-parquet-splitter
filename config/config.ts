export type Stage = 'dev' | 'stg' | 'prod';

export interface GlueSplitConfig {

}

export interface GlueMergeConfig {
  bucketName: string;
  scriptName: string;
  scriptPrefix: string;
  inPrefix: string;
  outPrefix: string;
}

export interface LambdaDeletionConfig {
  markerBucketName: string;
}

// 全てのスタックの config を統括
export interface AppConfig {
  stage: Stage;
  glueSplit: GlueSplitConfig;
  glueMerge: GlueMergeConfig;
  lambdaDeletion: LambdaDeletionConfig;
}

export const appConfigByStage: Record<Stage, AppConfig> = {
  dev: {
    stage: 'dev',
    glueSplit: {

    },
    glueMerge: {
      bucketName: 'glue-split-job-saxon', // S3バケット名
      scriptName: 'parquet-merge.py',     // Glue で実行するスクリプト名
      scriptPrefix: 'src/glue/merge/',    // スクリプトの S3 prefix
      inPrefix: 'data/split',             // 入力データの S3 prefix
      outPrefix: 'data/merge',            // 出力データの S3 prefix
    },
    lambdaDeletion: {
      markerBucketName: 'glue-split-job-dev',
    },
  },
  // stg, prod サンプル
  stg: {
    stage: 'stg',
    glueSplit: {

    },
    glueMerge: {
      bucketName: 'glue-split-job-stg',
      scriptName: 'parquet-merge.py',
      scriptPrefix: 'src/glue/merge/',
      inPrefix: 'data/split',
      outPrefix: 'data/merge',
    },
    lambdaDeletion: {
      markerBucketName: 'glue-split-job-stg',
    },
  },
  prod: {
    stage: 'prod',
    glueSplit: {

    },
    glueMerge: {
      bucketName: 'glue-split-job-prod',
      scriptName: 'parquet-merge.py',
      scriptPrefix: 'src/glue/merge/',
      inPrefix: 'data/split',
      outPrefix: 'data/merge',
    },
    lambdaDeletion: {
      markerBucketName: 'glue-split-job-prod',
    },
  },
};
