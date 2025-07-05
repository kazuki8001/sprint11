import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame  # ← これが必要

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# DynamoDB から読み込み
AmazonDynamoDB_node1751675450738 = glueContext.create_dynamic_frame.from_catalog(
    database="dynamodb_data_sprint11",
    table_name="inquirytable",
    transformation_ctx="AmazonDynamoDB_node1751675450738"
)

# データ品質チェック（変更なし）
EvaluateDataQuality().process_rows(
    frame=AmazonDynamoDB_node1751675450738,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node1751675408596",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# ↓↓↓ ここから追加：1ファイルにまとめる処理 ↓↓↓

# DynamicFrame → DataFrame に変換
df = AmazonDynamoDB_node1751675450738.toDF()

# 1ファイルにまとめる
df_single = df.coalesce(1)

# DataFrame → DynamicFrame に戻す
dyf_single = DynamicFrame.fromDF(df_single, glueContext, "dyf_single")

# S3 に書き込み
AmazonS3_node1751675457490 = glueContext.write_dynamic_frame.from_options(
    frame=dyf_single,
    connection_type="s3",
    format="glueparquet",  # parquet形式（glueparquetは内部的には同じ）
    connection_options={"path": "s3://my-output-bucket20250704/dynamodb-export/", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1751675457490"
)

# ↑↑↑ ここまで追加 ↑↑↑

job.commit()
