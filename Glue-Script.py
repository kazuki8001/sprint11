import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# ジョブ引数の取得
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# デフォルトのデータ品質ルールセット
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# DynamoDB から読み込み
AmazonDynamoDB_node = glueContext.create_dynamic_frame.from_catalog(
    database="dynamodb_data_sprint11",
    table_name="inquirytable",
    transformation_ctx="AmazonDynamoDB_node"
)

# データ品質チェック
EvaluateDataQuality().process_rows(
    frame=AmazonDynamoDB_node,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQualityContext",
        "enableDataQualityResultsPublishing": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL"
    }
)

# DynamicFrame → DataFrame に変換
df = AmazonDynamoDB_node.toDF()

# id カラムを先頭に移動
columns = df.columns
if "id" in columns:
    reordered_columns = ["id"] + [col for col in columns if col != "id"]
    df = df.select(reordered_columns)

# 1ファイルにまとめる
df_single = df.coalesce(1)

# DataFrame → DynamicFrame に戻す
dyf_single = DynamicFrame.fromDF(df_single, glueContext, "dyf_single")

# S3 に Parquet 形式で書き込み
AmazonS3_node = glueContext.write_dynamic_frame.from_options(
    frame=dyf_single,
    connection_type="s3",
    format="glueparquet",  # Parquet形式
    connection_options={
        "path": "s3://my-output-bucket20250704/dynamodb-export/",
        "partitionKeys": []
    },
    transformation_ctx="AmazonS3_node"
)

# ジョブの完了
job.commit()
