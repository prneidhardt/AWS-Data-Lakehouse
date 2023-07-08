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

# Script generated for node Accelerometer Trusted Zone
AccelerometerTrustedZone_node1688102900198 = (
    glueContext.create_dynamic_frame.from_options(
        format_options={"multiline": False},
        connection_type="s3",
        format="json",
        connection_options={
            "paths": ["s3://neidhpa-lakehouse-project/accelerometer_trusted/"],
            "recurse": True,
        },
        transformation_ctx="AccelerometerTrustedZone_node1688102900198",
    )
)

# Script generated for node Customer Trusted Zone
CustomerTrustedZone_node1688782334053 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/customer_trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrustedZone_node1688782334053",
)

# Script generated for node Step Trainer Trusted Zone
StepTrainerTrustedZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/step_trainer_curated/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerTrustedZone_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1688782409638 = ApplyMapping.apply(
    frame=CustomerTrustedZone_node1688782334053,
    mappings=[
        ("serialNumber", "string", "customer_serialNumber", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "customer_shareWithPublicAsOfDate",
            "long",
        ),
        ("birthDay", "string", "customer_birthDay", "string"),
        ("registrationDate", "bigint", "customer_registrationDate", "long"),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "customer_shareWithResearchAsOfDate",
            "long",
        ),
        ("customerName", "string", "customer_customerName", "string"),
        ("email", "string", "customer_email", "string"),
        ("lastUpdateDate", "bigint", "customer_lastUpdateDate", "long"),
        ("phone", "string", "customer_phone", "string"),
        (
            "shareWithFriendsAsOfDate",
            "bigint",
            "customer_shareWithFriendsAsOfDate",
            "long",
        ),
    ],
    transformation_ctx="RenamedkeysforJoin_node1688782409638",
)

# Script generated for node Join
Join_node1688782324503 = Join.apply(
    frame1=StepTrainerTrustedZone_node1,
    frame2=RenamedkeysforJoin_node1688782409638,
    keys1=["serialNumber"],
    keys2=["customer_serialNumber"],
    transformation_ctx="Join_node1688782324503",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1688842698270 = DynamicFrame.fromDF(
    Join_node1688782324503.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1688842698270",
)

# Script generated for node Join Step Trainer
JoinStepTrainer_node1688103008151 = Join.apply(
    frame1=AccelerometerTrustedZone_node1688102900198,
    frame2=DropDuplicates_node1688842698270,
    keys1=["timeStamp", "user"],
    keys2=["sensorReadingTime", "customer_email"],
    transformation_ctx="JoinStepTrainer_node1688103008151",
)

# Script generated for node Drop Fields
DropFields_node1688103100583 = DropFields.apply(
    frame=JoinStepTrainer_node1688103008151,
    paths=[
        "customer_serialNumber",
        "customer_birthDay",
        "customer_customerName",
        "customer_email",
        "customer_phone",
        "customer_shareWithPublicAsOfDate",
        "customer_shareWithFriendsAsOfDate",
        "customer_lastUpdateDate",
        "user",
    ],
    transformation_ctx="DropFields_node1688103100583",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1688843613159 = DynamicFrame.fromDF(
    DropFields_node1688103100583.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1688843613159",
)

# Script generated for node Machine Learning Zone
MachineLearningZone_node3 = glueContext.getSink(
    path="s3://neidhpa-lakehouse-project/machine_learning_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningZone_node3",
)
MachineLearningZone_node3.setCatalogInfo(
    catalogDatabase="project", catalogTableName="machine_learning_curated"
)
MachineLearningZone_node3.setFormat("json")
MachineLearningZone_node3.writeFrame(DropDuplicates_node1688843613159)
job.commit()
