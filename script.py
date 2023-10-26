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

# Script generated for node Amazon S3
AmazonS3_node1698278645304 = glueContext.create_dynamic_frame.from_options(
    format_options={
        "quoteChar": '"',
        "withHeader": True,
        "separator": ",",
        "optimizePerformance": False,
    },
    connection_type="s3",
    format="csv",
    connection_options={"paths": ["s3://cc-covid-input-data-lr"], "recurse": True},
    transformation_ctx="AmazonS3_node1698278645304",
)

# Script generated for node Change Schema
ChangeSchema_node1698278691774 = ApplyMapping.apply(
    frame=AmazonS3_node1698278645304,
    mappings=[
        ("Date_reported", "string", "Date_reported", "string"),
        ("Country_code", "string", "Country_code", "string"),
        ("Country", "string", "Country", "string"),
        ("WHO_region", "string", "WHO_region", "string"),
        ("New_cases", "string", "New_cases", "string"),
        ("New_deaths", "string", "New_deaths", "string"),
    ],
    transformation_ctx="ChangeSchema_node1698278691774",
)

# Script generated for node Amazon S3
AmazonS3_node1698278816956 = glueContext.write_dynamic_frame.from_options(
    frame=ChangeSchema_node1698278691774,
    connection_type="s3",
    format="csv",
    connection_options={"path": "s3://cc-covid-output-data-lr", "partitionKeys": []},
    transformation_ctx="AmazonS3_node1698278816956",
)

job.commit()
