
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
  
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
import datetime
import os
import boto3
from boto3.dynamodb.conditions import Key
# More efficient for single item retrieval
def get_job_info_from_dynamodb(job_name):
    dynamodb = boto3.resource('dynamodb', region_name='ap-south-1')
    table = dynamodb.Table('pipeline_metadata')
    response = table.get_item(
        Key={'job_name': job_name}
    )
    
    return response

# PySpark / Glue contexts
from pyspark.conf import SparkConf

# Spark SQL core imports (fixed typos in original: SparkSession / StringType etc.)
from pyspark.sql import SparkSession
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql.types import StringType
import pyspark.sql.functions as F 
def get_contexts():
    """Create SparkContext, GlueContext, and SparkSession with safe configs."""
    conf = (
        SparkConf()
        # Corrected legacy rebase/date/time parser policies
        .set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .set("spark.sql.legacy.timeParserPolicy", "LEGACY")
        # Memory & perf tuning toggles (adjust per cluster size)
        .set("spark.memory.offHeap.enabled", "true")
        .set("spark.memory.offHeap.size", "16g")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .set("spark.shuffle.useOldFetchProtocol", "true")
    )

    # Get or create contexts
    spark_context = SparkContext.getOrCreate(conf=conf)
    glue_context = GlueContext(spark_context)
    spark = glue_context.spark_session  # same object as SparkSession.builder.getOrCreate()

    return spark_context, glue_context, spark
def _write_dynamic_frame(df: "pyspark.sql.DataFrame", write_path: str):
    """Write a Spark DataFrame to S3 via Glue DynamicFrame as CSV, fallback Parquet."""
    # Convert to DynamicFrame
    df_dyn = DynamicFrame.fromDF(df, glue_context, "dyn")

    try:
        print("Writing CSV to:", write_path)
        glue_context.write_dynamic_frame.from_options(
            frame=df_dyn,
            connection_type="s3",
            connection_options={"path": write_path},
            format="csv",
            format_options={
                "withHeader": True,
                "separator": "|",
            },
            transformation_ctx="write_csv",
        )
    except Exception as e1:
        print("CSV write failed, falling back to Parquet:", str(e1))
        glue_context.write_dynamic_frame.from_options(
            frame=df_dyn,
            connection_type="s3",
            connection_options={"path": write_path},
            format="parquet",
            format_options={},
            transformation_ctx="write_parquet",
        )
def write_file(input_df, extn: str, S3_STAGE_PATH, S3_BUCKET, keep_existing: bool = False):
    if S3_STAGE_PATH is None:
        raise ValueError("S3_STAGE_PATH is required unless default is set")
    if S3_BUCKET is None:
        raise ValueError("S3_BUCKET is required unless default is set")

    print(f"Writing {extn} to {S3_STAGE_PATH} in bucket {S3_BUCKET}")
    # Your write logic here

    """(1) cleans the target folder, (2) writes the dataframe to S3."""
    # s3 = boto3.client("s3")
    s3 = boto3.resource('s3')

    # Build s3 path like s3://bucket/Tableau/Run_Recap/
    write_path = f"{S3_STAGE_PATH}{extn}/"

    # Optionally delete existing files to keep a clean folder
    if not keep_existing:
        bucket = s3.Bucket(S3_BUCKET)

        # Delete all objects in a folder
        for obj in bucket.objects.filter(Prefix=f'stage/{extn}/'):
            print(f"Deleting {obj.key}")
            obj.delete()
        # s3.delete_object(Bucket=TARGET_BUCKET, Key='stage/monthly_counts/run-1755965847305-part-r-00000')
        # print("Cleaning target prefix before write")
        # paginator = s3.get_paginator("list_objects_v2")
        # for page in paginator.paginate(Bucket=TARGET_BUCKET, Prefix=f"{TARGET_PREFIX}/{extn}/"):
        #     for obj in page.get("Contents", []):
        #         s3.delete_object(Bucket=TARGET_BUCKET, Key=obj["Key"])

    # Write the new extract
    _write_dynamic_frame(input_df, write_path)

    # Print the first object key just as a sanity echo (optional)
    # page = s3.list_objects_v2(Bucket=TARGET_BUCKET, Prefix=f"{TARGET_PREFIX}/{extn}/")
    # if "Contents" in page and page["Contents"]:
    #     print("Wrote:", page["Contents"][0]["Key"])  # first file in the folder


# ==== Data readers ===========================================================
def data_read(S3_SOURCE_PATH, TEMP_VIEW_NAME):
    df = spark.read.csv(S3_SOURCE_PATH, header=True)
    # Register temp views so Spark SQL can query them later
    df.createOrReplaceTempView(TEMP_VIEW_NAME)
    
    return df


# ==== Main report builder ====================================================

def build_reports(SQL_QUERY, JOB_NAME, S3_STAGE_PATH, S3_BUCKET):
    df = spark.sql(SQL_QUERY)
    # Add a simple header row like the original (using union with a tiny DF)
    df = df.withColumn("Report", F.lit("1"))
    PROCESS_DATE_YYYYMMDD = datetime.datetime.now()
    header = spark.createDataFrame(
        [(f'PROC Date: {PROCESS_DATE_YYYYMMDD}', '', 0, '1')],
        ["acct_typ", "delvry_pref", "cnt", "Report"],
    )

    # Persist to S3 for Tableau conversion
    write_file(df.coalesce(1), JOB_NAME, S3_STAGE_PATH, S3_BUCKET)

    # Define a simple schema for Hyper (adjust to match actual columns/types)
    df_schema = "acct_typ text, delvry_pref text, cnt bigint, Report text"

    return [(JOB_NAME, df_schema)]

args = getResolvedOptions(sys.argv, ['job_name', 'env'])
    
glue_context = glueContext
job_name = args['job_name']
response = get_job_info_from_dynamodb(job_name)
if 'Item' in response:
    item = response['Item']

    # Extract fields from DynamoDB item
    JOB_NAME = item.get('job_name')
    ENV = item.get('env')
    S3_BUCKET = item.get('bucket')
    KEY = item.get('key')
    S3_SOURCE_PATH = item.get('s3_source_path')
    S3_STAGE_PATH = item.get('s3_stage_path')
    SQL_QUERY = item.get('business_sql_query')
    TEMP_VIEW_NAME = item.get('temp_view_name')

else:
    raise ValueError("Metadata for job 'monthly_counts' not found")

data_read(S3_SOURCE_PATH, TEMP_VIEW_NAME)
build_reports(SQL_QUERY, JOB_NAME, S3_STAGE_PATH, S3_BUCKET)

# testing git comments

job.commit()