import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Set up Spark and Glue Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Step 1: Read data from the source S3 location (customer_landing)
source_dynamic_frame = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/customer/landing/"], 
        "recurse": True,
    },
    transformation_ctx="source_dynamic_frame",
)

# Step 2: Filter data (apply any business logic as needed)
filtered_dynamic_frame = Filter.apply(
    frame=source_dynamic_frame,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),  
    transformation_ctx="filtered_dynamic_frame",
)

# Step 3: Write the transformed data to the customer_trusted S3 path
glueContext.write_dynamic_frame.from_options(
    frame=filtered_dynamic_frame,
    connection_type="s3",
    format="json",  # Output format (adjust if you need a different format)
    connection_options={
        "path": "s3://stedi-data-lake-kam/customer/trusted/",  
        "partitionKeys": [],  
    },
    transformation_ctx="write_dynamic_frame",
)

# Commit the job to signal that it's complete
job.commit()
