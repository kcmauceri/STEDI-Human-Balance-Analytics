import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame

# Spark SQL join helper
def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = glueContext.spark_session.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)

# Glue job setup
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Read customer_trusted data
customer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="customer_trusted"
)

# Read accelerometer_trusted data
accelerometer_trusted = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-data-lake-kam/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="accelerometer_trusted"
)

# Perform SQL join to keep only customers with accelerometer records
SqlQuery = """
SELECT DISTINCT customer_trusted.*
FROM customer_trusted
JOIN accelerometer_trusted
ON customer_trusted.email = accelerometer_trusted.user
"""

customer_curated = sparkSqlQuery(
    glueContext,
    query=SqlQuery,
    mapping={
        "customer_trusted": customer_trusted,
        "accelerometer_trusted": accelerometer_trusted,
    },
    transformation_ctx="customer_curated"
)

# Write to customer_curated
customer_curated_sink = glueContext.getSink(
    path="s3://stedi-data-lake-kam/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="customer_curated_sink"
)
customer_curated_sink.setCatalogInfo(
    catalogDatabase="stedi_data_lake",
    catalogTableName="customer_curated"
)
customer_curated_sink.setFormat("json")
customer_curated_sink.writeFrame(customer_curated)

job.commit()
