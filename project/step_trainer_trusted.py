import sys
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

# Load step_trainer_landing
step_trainer_landing_df = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    connection_options={"paths": ["s3://stedi-data-lake-kam/step_trainer/landing/"], "recurse": True},
    format_options={"multiline": False}
).toDF()

# Load customer_curated
customer_curated_df = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_data_lake",
    table_name="customer_curated"
).toDF()

# Rename one of the duplicate columns to avoid ambiguity
customer_curated_df = customer_curated_df.withColumnRenamed("serialNumber", "customerSerialNumber")

# Join on serial number (step trainer side)
joined_df = step_trainer_landing_df.join(
    customer_curated_df,
    step_trainer_landing_df["serialNumber"] == customer_curated_df["customerSerialNumber"],
    "inner"
)

# Select only step trainer fields
final_df = joined_df.select(
    step_trainer_landing_df["serialNumber"],
    "sensorReadingTime",
    "distanceFromObject"
)

# Convert to DynamicFrame
from awsglue import DynamicFrame
step_trainer_trusted_dyf = DynamicFrame.fromDF(final_df, glueContext, "step_trainer_trusted_dyf")

# Write to S3 and catalog
sink = glueContext.getSink(
    path="s3://stedi-data-lake-kam/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="step_trainer_trusted_sink"
)
sink.setCatalogInfo(
    catalogDatabase="stedi_data_lake",
    catalogTableName="step_trainer_trusted"
)
sink.setFormat("json")
sink.writeFrame(step_trainer_trusted_dyf)

job.commit()
