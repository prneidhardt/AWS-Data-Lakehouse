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
AccelerometerLandingZone_node1688779465280 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://neidhpa-lakehouse-project/accelerometer_landing/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerLandingZone_node1688779465280",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1685823057097 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1685823057097",
)

# Script generated for node Join Customer
JoinCustomer_node1685822949825 = Join.apply(
    frame1=CustomerTrustedZone_node1685823057097,
    frame2=AccelerometerLandingZone_node1688779465280,
    keys1=["email"],
    keys2=["user"],
    transformation_ctx="JoinCustomer_node1685822949825",
)

# Script generated for node Drop Fields
DropFields_node1685823281966 = DropFields.apply(
    frame=JoinCustomer_node1685822949825,
    paths=["user", "timeStamp", "x", "y", "z"],
    transformation_ctx="DropFields_node1685823281966",
)

# Script generated for node Customer Curated
CustomerCurated_node3 = glueContext.getSink(
    path="s3://neidhpa-lakehouse-project/customer_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node3",
)
CustomerCurated_node3.setCatalogInfo(
    catalogDatabase="project", catalogTableName="customer_curated"
)
CustomerCurated_node3.setFormat("json")
CustomerCurated_node3.writeFrame(DropFields_node1685823281966)
job.commit()
