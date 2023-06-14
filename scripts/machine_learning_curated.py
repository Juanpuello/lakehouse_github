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

# Script generated for node Accelerometer Trusted Data
AccelerometerTrustedData_node1686682186063 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_trusted",
        transformation_ctx="AccelerometerTrustedData_node1686682186063",
    )
)

# Script generated for node Step Trainer Trusted Data
StepTrainerTrustedData_node1686762502587 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="step_trainer_trusted",
        transformation_ctx="StepTrainerTrustedData_node1686762502587",
    )
)

# Script generated for node Join
Join_node1686679612097 = Join.apply(
    frame1=AccelerometerTrustedData_node1686682186063,
    frame2=StepTrainerTrustedData_node1686762502587,
    keys1=["timestamp"],
    keys2=["sensorreadingtime"],
    transformation_ctx="Join_node1686679612097",
)

# Script generated for node Drop Fields
DropFields_node1686679714890 = DropFields.apply(
    frame=Join_node1686679612097,
    paths=["sensorreadingtime"],
    transformation_ctx="DropFields_node1686679714890",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686679714890,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bucket-udacity-redshift/machine_leaning/curated/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
