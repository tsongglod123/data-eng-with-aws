import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Customer Trusted
CustomerTrusted_node1734336774452 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1734336774452")

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1734336773578 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1734336773578")

# Script generated for node Join
Join_node1734336778032 = Join.apply(frame1=AccelerometerLanding_node1734336773578, frame2=CustomerTrusted_node1734336774452, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1734336778032")

# Script generated for node Drop Fields
DropFields_node1734336788726 = ApplyMapping.apply(frame=Join_node1734336778032, mappings=[("customername", "string", "customername", "string"), ("email", "string", "email", "string"), ("phone", "string", "phone", "string"), ("birthday", "string", "birthday", "string"), ("serialnumber", "string", "serialnumber", "string"), ("registrationdate", "long", "registrationdate", "long"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long")], transformation_ctx="DropFields_node1734336788726")

# Script generated for node Drop Duplicates
DropDuplicates_node1734338419657 =  DynamicFrame.fromDF(DropFields_node1734336788726.toDF().dropDuplicates(), glueContext, "DropDuplicates_node1734338419657")

# Script generated for node Customer Curated
EvaluateDataQuality().process_rows(frame=DropDuplicates_node1734338419657, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734336728252", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
CustomerCurated_node1734336797295 = glueContext.getSink(path="s3://stedi-lake-house/customer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="CustomerCurated_node1734336797295")
CustomerCurated_node1734336797295.setCatalogInfo(catalogDatabase="stedi",catalogTableName="customer_curated")
CustomerCurated_node1734336797295.setFormat("json")
CustomerCurated_node1734336797295.writeFrame(DropDuplicates_node1734338419657)
job.commit()