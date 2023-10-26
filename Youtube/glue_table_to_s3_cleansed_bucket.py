import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)


from awsglue.dynamicframe import DynamicFrame
predicate_pushdown = "region in ('ca','gb','us')"
		
# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1698142154821 = glueContext.create_dynamic_frame.from_catalog(
    database="de-youtube-database",
    table_name="raw_statistics",
    transformation_ctx="AWSGlueDataCatalog_node1698142154821",
    push_down_predicate = predicate_pushdown
)

# Script generated for node Amazon S3
AmazonS3_node1698142179225 = glueContext.write_dynamic_frame.from_options(
    frame=AWSGlueDataCatalog_node1698142154821,
    connection_type="s3",
    format="glueparquet",
    connection_options={
        "path": "s3://de-youtube-cleansed-data-s3/youtube/raw_statistics/",
        "partitionKeys": ["region"],
    },
    format_options={"compression": "snappy"},
    transformation_ctx="AmazonS3_node1698142179225",
)

job.commit()
