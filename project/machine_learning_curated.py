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

# 2. Read Step Trainer Trusted Data (Trusted Zone)
step_trainer_trusted = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/step_trainer/trusted/"],
        "recurse": True
    },
    transformation_ctx="step_trainer_trusted"
)

# 3. Read Accelerometer Data (Landing Zone)
accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/accelerometer/landing/"],
        "recurse": True
    },
    transformation_ctx="accelerometer_landing"
)

# 4. Join Step Trainer Data with Accelerometer Data on Timestamp and Email
joined_data = Join.apply(
    frame1=step_trainer_trusted,
    frame2=accelerometer_landing,
    keys1=["sensorReadingTime", "email"],
    keys2=["timestamp", "user"],
    transformation_ctx="joined_data"
)

# 5. Filter Data to Only Include Customers Who Agreed to Share Data for Research
filtered_data = Filter.apply(
    frame=joined_data,
    f=lambda row: (row["shareWithResearchAsOfDate"] != 0),  # Only include customers who agreed
    transformation_ctx="filtered_data"
)

# 6. Select Relevant Fields for machine_learning_curated
machine_learning_curated = SelectFields.apply(
    frame=filtered_data,
    paths=["user", "email", "serialNumber", "sensorReadingTime", "step_count", "distanceFromObject", "x", "y", "z"],
    transformation_ctx="machine_learning_curated"
)

# 7. Write the Aggregated Data to the Machine Learning Curated Zone
glueContext.write_dynamic_frame.from_options(
    frame=machine_learning_curated,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-kam/machine_learning/curated/",
        "partitionKeys": []
    },
    transformation_ctx="write_machine_learning_curated"
)

job.commit()
