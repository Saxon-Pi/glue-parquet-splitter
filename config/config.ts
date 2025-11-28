export type Stage = 'dev' | 'stg' | 'prod';

export interface GlueMergeConfig {
  bucketName: string;
  scriptName: string;
  scriptPrefix: string;
  inPrefix: string;
  outPrefix: string;
}

export const glueMergeConfigByStage: Record<Stage, GlueMergeConfig> = {
  dev: {
    bucketName: 'glue-split-job-saxon', // S3バケット名
    scriptName: 'parquet-merge.py',     // Glue で実行するスクリプト名
    scriptPrefix: 'src/glue/merge/',    // スクリプトの S3 prefix
    inPrefix: 'data/split',             // 入力データの S3 prefix
    outPrefix: 'data/merge',            // 出力データの S3 prefix
  },
  // stg, prod サンプル
  stg: {
    bucketName: 'glue-split-job-saxon-stg',
    scriptName: 'parquet-merge.py',
    scriptPrefix: 'src/glue/merge/',
    inPrefix: 'data/split',
    outPrefix: 'data/merge',
  },
  prod: {
    bucketName: 'glue-split-job-saxon-prod',
    scriptName: 'parquet-merge.py',
    scriptPrefix: 'src/glue/merge/',
    inPrefix: 'data/split',
    outPrefix: 'data/merge',
  },
};
