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

# 1. Read Customer Curated Data 
customers_curated = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/customer/curated/"],
        "recurse": True
    },
    transformation_ctx="customers_curated"
)

# 2. Read Step Trainer Data (Landing Zone)
step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/step_trainer/landing/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_landing"
)

# 3. Join Step Trainer Data with Customer Curated Data on "email" 
joined_data = Join.apply(
    frame1=step_trainer_landing,
    frame2=customers_curated,
    keys1=["serialNumber"],  
    keys2=["email"],  
    transformation_ctx="joined_data"
)

# 4. Select Relevant Fields for step_trainer_trusted
step_trainer_trusted = SelectFields.apply(
    frame=joined_data,
    paths=["serialNumber", "sensorReadingTime", "distanceFromObject", "email"],
    transformation_ctx="step_trainer_trusted"
)

# 5. Write the Step Trainer Data to the Trusted Zone
glueContext.write_dynamic_frame.from_options(
    frame=step_trainer_trusted,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-kam/step_trainer/trusted/",
        "partitionKeys": []
    },
    transformation_ctx="write_step_trainer_trusted"
)

job.commit()
