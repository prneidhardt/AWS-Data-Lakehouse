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

# Script generated for node Accelerometer Landing Zone
AccelerometerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/accelerometer_landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLandingZone_node1",
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1688779969580 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1688779969580",
)

# Script generated for node Join Customer
JoinCustomer_node1688097807952 = Join.apply(
    frame1=AccelerometerLandingZone_node1,
    frame2=CustomerTrustedZone_node1688779969580,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1688097807952",
)

# Script generated for node Drop Fields
DropFields_node1688097883085 = DropFields.apply(
    frame=JoinCustomer_node1688097807952,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "email",
        "lastUpdateDate",
        "phone",
        "shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1688097883085",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1688097919698 = glueContext.getSink(
    path="s3://neidhpa-lakehouse-project/accelerometer_trusted/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1688097919698",
)
AccelerometerTrusted_node1688097919698.setCatalogInfo(
    catalogDatabase="project", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1688097919698.setFormat("json")
AccelerometerTrusted_node1688097919698.writeFrame(DropFields_node1688097883085)
job.commit()
