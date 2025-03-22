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

# 1. Read Customer Trusted Data 
customer_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/customer/trusted/"],
        "recurse": True
    },
    transformation_ctx="customer_trusted"
)

# 2. Read Accelerometer Data (Landing Zone)
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_landing"
)

# 3. Join Customer Data with Accelerometer Data on "user" (customer email) and "email"
joined_data = Join.apply(
    frame1=accelerometer_landing,
    frame2=customer_trusted,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="joined_data"
)

# 4. Select Relevant Fields for customers_curated 
customers_curated = SelectFields.apply(
    frame=joined_data,
    paths=["user", "email", "customername", "serialnumber", "timestamp", "x", "y", "z"],
    transformation_ctx="customers_curated"
)

# 5. Write the curated data to the Curated Zone 
glueContext.write_dynamic_frame.from_options(
    frame=customers_curated,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-kam/customer/curated/",
        "partitionKeys": []
    },
    transformation_ctx="write_customers_curated"
)

job.commit()
