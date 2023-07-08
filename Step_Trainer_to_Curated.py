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

# Script generated for node Customer Curated
CustomerCurated_node1688102900198 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/customer_curated/"],
        "recurse": True,
    },
    transformation_ctx="CustomerCurated_node1688102900198",
)

# Script generated for node Step Trainer Landing Zone
StepTrainerLandingZone_node1 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://neidhpa-lakehouse-project/step_trainer/"],
        "recurse": True,
    },
    transformation_ctx="StepTrainerLandingZone_node1",
)

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1688103029163 = ApplyMapping.apply(
    frame=CustomerCurated_node1688102900198,
    mappings=[
        ("serialNumber", "string", "customer_serialNumber", "string"),
        ("z", "double", "customer_z", "double"),
        ("timeStamp", "bigint", "customer_timeStamp", "long"),
        ("birthDay", "string", "customer_birthDay", "string"),
        (
            "shareWithPublicAsOfDate",
            "bigint",
            "customer_shareWithPublicAsOfDate",
            "long",
        ),
        (
            "shareWithResearchAsOfDate",
            "bigint",
            "customer_shareWithResearchAsOfDate",
            "long",
        ),
        ("registrationDate", "bigint", "customer_registrationDate", "long"),
        ("customerName", "string", "customer_customerName", "string"),
        ("user", "string", "customer_user", "string"),
        ("y", "double", "customer_y", "double"),
        ("x", "double", "customer_x", "double"),
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
    transformation_ctx="RenamedkeysforJoin_node1688103029163",
)

# Script generated for node Join Step Trainer
JoinStepTrainer_node1688103008151 = Join.apply(
    frame1=StepTrainerLandingZone_node1,
    frame2=RenamedkeysforJoin_node1688103029163,
    keys1=["serialNumber"],
    keys2=["customer_serialNumber"],
    transformation_ctx="JoinStepTrainer_node1688103008151",
)

# Script generated for node Drop Fields
DropFields_node1688103100583 = DropFields.apply(
    frame=JoinStepTrainer_node1688103008151,
    paths=[
        "customer_serialNumber",
        "customer_customerName",
        "customer_email",
        "customer_phone",
        "customer_birthDay",
        "customer_z",
        "customer_user",
        "customer_y",
        "customer_x",
        "customer_timeStamp",
        "customer_shareWithPublicAsOfDate",
        "customer_shareWithResearchAsOfDate",
        "customer_registrationDate",
        "customer_lastUpdateDate",
        "customer_shareWithFriendsAsOfDate",
    ],
    transformation_ctx="DropFields_node1688103100583",
)

# Script generated for node Drop Duplicates
DropDuplicates_node1688840572934 = DynamicFrame.fromDF(
    DropFields_node1688103100583.toDF().dropDuplicates(),
    glueContext,
    "DropDuplicates_node1688840572934",
)

# Script generated for node Step Trainer Curated Zone
StepTrainerCuratedZone_node3 = glueContext.getSink(
    path="s3://neidhpa-lakehouse-project/step_trainer_curated/",
    connection_type="s3",
    updateBehavior="LOG",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerCuratedZone_node3",
)
StepTrainerCuratedZone_node3.setCatalogInfo(
    catalogDatabase="project", catalogTableName="step_trainer_curated"
)
StepTrainerCuratedZone_node3.setFormat("json")
StepTrainerCuratedZone_node3.writeFrame(DropDuplicates_node1688840572934)
job.commit()
