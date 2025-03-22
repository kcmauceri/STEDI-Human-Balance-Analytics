import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Get job parameters
args = getResolvedOptions(sys.argv, ["JOB_NAME"])

# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# 1. Read Accelerometer Landing Data from S3
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_landing"
)

# 2. Read Customer Trusted Data from S3

customer_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/customer/trusted/"],
        "recurse": True
    },
    transformation_ctx="customer_trusted"
)

# 3. Join Accelerometer Data with Customer Trusted Data
# Join on the accelerometer field "user" with the customer field "email"
joined_data = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="joined_data"
)


accelerometer_trusted = SelectFields.apply(
    frame=joined_data,
    paths=["user", "timestamp", "x", "y", "z"],
    transformation_ctx="accelerometer_trusted"
)

# 5. Write the transformed data to the accelerometer trusted S3 location
glueContext.write_dynamic_frame.from_options(
    frame=accelerometer_trusted,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-kam/accelerometer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="write_accelerometer_trusted"
)

job.commit()
