import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Trusted Data
CustomerTrustedData_node1686682186063 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrustedData_node1686682186063",
)

# Script generated for node Accelerometer Landing Data
AccelerometerLandingData_node1686679494604 = (
    glueContext.create_dynamic_frame.from_catalog(
        database="stedi",
        table_name="accelerometer_landing",
        transformation_ctx="AccelerometerLandingData_node1686679494604",
    )
)

# Script generated for node Drop Duplicates
DropDuplicates_node1686682799777 = DynamicFrame.fromDF(
    CustomerTrustedData_node1686682186063.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1686682799777",
)

# Script generated for node Join
Join_node1686679612097 = Join.apply(
    frame1=AccelerometerLandingData_node1686679494604,
    frame2=DropDuplicates_node1686682799777,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="Join_node1686679612097",
)

# Script generated for node Drop Fields
DropFields_node1686679714890 = DropFields.apply(
    frame=Join_node1686679612097,
    paths=[
        "email",
        "phone",
        "customername",
        "birthdate",
        "serialnumber",
        "registrationdate",
        "lastupdatedate",
        "sharewithresearchasofdate",
        "sharewithpublicasofdate",
        "sharewithfriendsasofdate",
    ],
    transformation_ctx="DropFields_node1686679714890",
)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(
    path="s3://bucket-udacity-redshift/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="S3bucket_node3",
)
S3bucket_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="accelerometer_trusted"
)
S3bucket_node3.setFormat("json")
S3bucket_node3.writeFrame(DropFields_node1686679714890)
job.commit()
