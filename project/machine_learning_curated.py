import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Load data
accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data_lake",
    table_name="accelerometer_trusted"
)

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data_lake",
    table_name="step_trainer_trusted"
)

# Join on timestamp = sensorReadingTime
joined = Join.apply(
    frame1=accelerometer_trusted,
    frame2=step_trainer_trusted,
    keys1=["timestamp"],
    keys2=["sensorReadingTime"],
    transformation_ctx="joined"
)

# Convert to DF and drop duplicates
joined_df = joined.toDF().dropDuplicates()
# Drop the user field (optional)
joined_df = joined_df.drop("user")

# Convert back to DynamicFrame
final_dyf = DynamicFrame.fromDF(joined_df, glueContext, "final_dyf")

# Write to curated zone
glueContext.write_dynamic_frame.from_options(
    frame=final_dyf,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-data-lake-kam/machine_learning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="ml_curated_output"
)

job.commit()
