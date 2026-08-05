import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs
from pyspark.sql.types import DateType, DoubleType, IntegerType, BooleanType, StringType

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Data Quality Ruleset
DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0,
        IsComplete "order_id",
        IsComplete "sales",
        IsUnique "row_id",
        ColumnValues "discount" between 0.0 and 1.0,
        ColumnValues "shipping_days" >= 0,
        CustomSql "select count(*) from primary where profit >= sales" = 0
    ]
"""

# Extract
df = spark.read \
    .option("header", "true") \
    .option("encoding", "ISO-8859-1") \
    .option("quote", "\"") \
    .option("escape", "\"") \
    .option("multiLine", "true") \
    .option("inferSchema", "false") \
    .csv("s3://od-superstore-etl/raw/")

# Snake_case
rename_map = {
    "Row ID": "row_id",
    "Order ID": "order_id",
    "Order Date": "order_date",
    "Ship Date": "ship_date",
    "Ship Mode": "ship_mode",
    "Customer ID": "customer_id",
    "Customer Name": "customer_name",
    "Segment": "segment",
    "Country": "country",
    "City": "city",
    "State": "state",
    "Postal Code": "postal_code",
    "Region": "region",
    "Product ID": "product_id",
    "Category": "category",
    "Sub-Category": "sub_category",
    "Product Name": "product_name",
    "Sales": "sales",
    "Quantity": "quantity",
    "Discount": "discount",
    "Profit": "profit"
}
for old_name, new_name in rename_map.items():
    if old_name in df.columns:
        df = df.withColumnRenamed(old_name, new_name)

# Column types
df = df.withColumn("order_date", SqlFuncs.to_date("order_date", "M/d/yyyy")) \
       .withColumn("ship_date", SqlFuncs.to_date("ship_date", "M/d/yyyy")) \
       .withColumn("postal_code", df["postal_code"].cast(StringType())) \
       .withColumn("sales", df["sales"].cast(DoubleType())) \
       .withColumn("quantity", df["quantity"].cast(IntegerType())) \
       .withColumn("discount", df["discount"].cast(DoubleType())) \
       .withColumn("profit", df["profit"].cast(DoubleType()))

# Fill null postal codes with "00000"
df = df.withColumn("postal_code", SqlFuncs.when(df["postal_code"].isNull(), "00000").otherwise(df["postal_code"]))

# Features
df = df.withColumn("shipping_days", SqlFuncs.datediff("ship_date", "order_date")) \
       .withColumn("order_year", SqlFuncs.year("order_date")) \
       .withColumn("order_month", SqlFuncs.month("order_date")) \
       .withColumn("order_quarter", SqlFuncs.quarter("order_date")) \
       .withColumn("profit_margin", SqlFuncs.round(df["profit"] / df["sales"], 4)) \
       .withColumn("is_profitable", df["profit"] > 0) \
       .withColumn(
           "shipping_speed",
           SqlFuncs.when(SqlFuncs.col("shipping_days") <= 2, "fast")
                    .when(SqlFuncs.col("shipping_days") <= 5, "normal")
                    .otherwise("slow")
       ) \
       .withColumn(
           "is_high_season",
           SqlFuncs.col("order_quarter") == 4
       )

DynamicFrame_transformed = DynamicFrame.fromDF(df, glueContext, "DynamicFrame_transformed")

# Duplicates
DropDuplicates_node = DynamicFrame.fromDF(
    DynamicFrame_transformed.toDF().dropDuplicates(["row_id"]),
    glueContext,
    "DropDuplicates_node"
)

# Data Quality Evaluation
dq_results = EvaluateDataQuality().process_rows(
    frame=DropDuplicates_node,
    ruleset=DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "EvaluateDataQuality_node",
        "enableDataQualityResultsPublishing": True,
        "enableDataQualityCloudWatchMetrics": True
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
        "performanceTuning.caching": "CACHE_NOTHING"
    }
)

# Quarantine
# Extract DynamicFrame for rows
row_level_outcomes_dyf = dq_results.select("rowLevelOutcomes")

# Converting to df
df_dq = row_level_outcomes_dyf.toDF()

# Separating passed and failed records
good_records_df = df_dq.filter(SqlFuncs.col("DataQualityEvaluationResult") == "Passed").drop("DataQualityEvaluationResult")

bad_records_df = df_dq.filter(SqlFuncs.col("DataQualityEvaluationResult") == "Failed")

# Converting into DynamicFrame
GoodRecords_DynamicFrame = DynamicFrame.fromDF(good_records_df, glueContext, "good_records")
BadRecords_DynamicFrame = DynamicFrame.fromDF(bad_records_df, glueContext, "bad_records")

# GoodRecords
if GoodRecords_DynamicFrame.count() > 0:
    GoodRecords_DynamicFrame = GoodRecords_DynamicFrame.coalesce(1)
    
    AmazonS3_Processed = glueContext.write_dynamic_frame.from_options(
        frame=GoodRecords_DynamicFrame,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://od-superstore-etl/processed/",
            "partitionKeys": ["order_year", "region"]
        },
        format_options={"compression": "snappy"},
        transformation_ctx="AmazonS3_Processed"
    )

# Failed records -> Quarantine
if BadRecords_DynamicFrame.count() > 0:
    BadRecords_DynamicFrame = BadRecords_DynamicFrame.coalesce(1)
    
    AmazonS3_Quarantine = glueContext.write_dynamic_frame.from_options(
        frame=BadRecords_DynamicFrame,
        connection_type="s3",
        format="glueparquet",
        connection_options={
            "path": "s3://od-superstore-etl/quarantine/",
            "partitionKeys": ["order_year", "region"]
        },
        format_options={"compression": "snappy"},
        transformation_ctx="AmazonS3_Quarantine"
    )

job.commit()
