import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame
import re
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://bucket-udacity-redshift/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1",
)

# Script generated for node Share With Research
ShareWithResearch_node1686628353695 = Filter.apply(
    frame=CustomerLanding_node1,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1686628353695",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1686681151869 = DynamicFrame.fromDF(
    ShareWithResearch_node1686628353695.toDF().dropDuplicates(["email"]),
    glueContext,
    "DropDuplicates_node1686681151869",
)

# Script generated for node Trusted Customer Zone
TrustedCustomerZone_node3 = glueContext.getSink(
    path="s3://bucket-udacity-redshift/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="TrustedCustomerZone_node3",
)
TrustedCustomerZone_node3.setCatalogInfo(
    catalogDatabase="stedi", catalogTableName="customer_trusted"
)
TrustedCustomerZone_node3.setFormat("json")
TrustedCustomerZone_node3.writeFrame(DropDuplicates_node1686681151869)
job.commit()
