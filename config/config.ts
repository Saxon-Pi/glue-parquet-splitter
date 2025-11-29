export type Stage = 'dev' | 'stg' | 'prod';

export interface GlueSplitConfig {
  bucketName          : string;
  scriptName          : string;
  scriptPrefix        : string;
  inPrefixPyshell     : string;
  inPrefixRay         : string;
  outPrefixPyshell    : string;
  outPrefixRay        : string;
  markerPrefixRay     : string;
  markerPrefixPyshell : string;
}

export interface GlueMergeConfig {
  bucketName    : string;
  scriptName    : string;
  scriptPrefix  : string;
  inPrefix      : string;
  outPrefix     : string;
}

export interface LambdaDeletionConfig {
  markerBucketName: string;  // 完了マーカー格納バケット
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
      bucketName          : "glue-split-job-saxon",  // S3バケット名
      scriptName          : "parquet-splitter.py",   // Glue で実行するスクリプト名
      scriptPrefix        : "src/glue/split/",       // スクリプトの S3 prefix
      inPrefixPyshell     : "data/input/pyshell",    // 入力データの S3 prefix (Pyshell)
      inPrefixRay         : "data/input/ray",        // 入力データの S3 prefix (Ray)
      outPrefixPyshell    : "data/split/pyshell",    // 出力データの S3 prefix (Pyshell)
      outPrefixRay        : "data/split/ray",        // 出力データの S3 prefix (Ray)
      markerPrefixRay     : "data/markers/ray",      // マーカー（分割情報）の S3 prefix (Ray)
      markerPrefixPyshell : "data/markers/pyshell",  // マーカー（分割情報）の S3 prefix (Pyshell)
    },
    glueMerge: {
      bucketName    : 'glue-split-job-saxon', // S3バケット名
      scriptName    : 'parquet-merge.py',     // Glue で実行するスクリプト名
      scriptPrefix  : 'src/glue/merge/',      // スクリプトの S3 prefix
      inPrefix      : 'data/split',           // 入力データの S3 prefix
      outPrefix     : 'data/merge',           // 出力データの S3 prefix
    },
    lambdaDeletion: {
      markerBucketName: 'glue-split-job-dev',
    },
  },
  // stg, prod サンプル
  stg: {
    stage: 'stg',
    glueSplit: {
      bucketName          : "glue-split-job-saxon",
      scriptName          : "parquet-splitter.py",
      scriptPrefix        : "src/glue/split/",
      inPrefixPyshell     : "data/input/pyshell",
      inPrefixRay         : "data/input/ray",
      outPrefixPyshell    : "data/split/pyshell",
      outPrefixRay        : "data/split/ray",
      markerPrefixRay     : "data/markers/ray",
      markerPrefixPyshell : "data/markers/pyshell",
    },
    glueMerge: {
      bucketName    : 'glue-split-job-stg',
      scriptName    : 'parquet-merge.py',
      scriptPrefix  : 'src/glue/merge/',
      inPrefix      : 'data/split',
      outPrefix     : 'data/merge',
    },
    lambdaDeletion: {
      markerBucketName: 'glue-split-job-stg',
    },
  },
  prod: {
    stage: 'prod',
    glueSplit: {
      bucketName          : "glue-split-job-saxon",
      scriptName          : "parquet-splitter.py",
      scriptPrefix        : "src/glue/split/",
      inPrefixPyshell     : "data/input/pyshell",
      inPrefixRay         : "data/input/ray",
      outPrefixPyshell    : "data/split/pyshell",
      outPrefixRay        : "data/split/ray",
      markerPrefixRay     : "data/markers/ray",
      markerPrefixPyshell : "data/markers/pyshell",
    },
    glueMerge: {
      bucketName    : 'glue-split-job-prod',
      scriptName    : 'parquet-merge.py',
      scriptPrefix  : 'src/glue/merge/',
      inPrefix      : 'data/split',
      outPrefix     : 'data/merge',
    },
    lambdaDeletion: {
      markerBucketName: 'glue-split-job-prod',
    },
  },
};
