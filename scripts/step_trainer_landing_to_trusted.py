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

# Script generated for node Customer Curated Data
CustomerCuratedData_node1686682186063 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_curated",
    transformation_ctx="CustomerCuratedData_node1686682186063",
)

# Script generated for node Step Trainer Landing Data
StepTrainerLandingData_node1686760929044 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://bucket-udacity-redshift/step_trainer/landing/"],
            "recurse": True,
        },
        transformation_ctx="StepTrainerLandingData_node1686760929044",
    )
)

# Script generated for node Join
Join_node1686679612097 = Join.apply(
    frame1=CustomerCuratedData_node1686682186063,
    frame2=StepTrainerLandingData_node1686760929044,
    keys1=["serialnumber"],
    keys2=["serialNumber"],
    transformation_ctx="Join_node1686679612097",
)

# Script generated for node Drop Fields
DropFields_node1686679714890 = DropFields.apply(
    frame=Join_node1686679612097,
    paths=[
        "email",
        "phone",
        "customername",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
        "timestamp",
        "birthday",
    ],
    transformation_ctx="DropFields_node1686679714890",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=DropFields_node1686679714890,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://bucket-udacity-redshift/step_trainer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="S3bucket_node3",
)

job.commit()
