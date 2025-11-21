#!/usr/bin/env node
import * as cdk from 'aws-cdk-lib/core';
import { GlueParquetSplitterStack } from '../lib/glue-parquet-splitter-stack';

const app = new cdk.App();
new GlueParquetSplitterStack(app, 'GlueParquetSplitterStack', {

});
