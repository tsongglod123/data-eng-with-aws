import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1734874382516 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1734874382516")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1734874382859 = glueContext.create_dynamic_frame.from_catalog(database="stedi_db", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1734874382859")

# Script generated for node Join
SqlQuery2254 = '''
select * from s
join a on s.sensorreadingtime = a.timestamp;
'''
Join_node1734874390518 = sparkSqlQuery(glueContext, query = SqlQuery2254, mapping = {"a":AccelerometerTrusted_node1734874382516, "s":StepTrainerTrusted_node1734874382859}, transformation_ctx = "Join_node1734874390518")

# Script generated for node Drop Fields
DropFields_node1734879067260 = DropFields.apply(frame=Join_node1734874390518, paths=["user"], transformation_ctx="DropFields_node1734879067260")

# Script generated for node Step Trainer Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1734879067260, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734874352935", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerCurated_node1734874396791 = glueContext.getSink(path="s3://stedi-lakehouse/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerCurated_node1734874396791")
StepTrainerCurated_node1734874396791.setCatalogInfo(catalogDatabase="stedi_db",catalogTableName="machine_learning_curated")
StepTrainerCurated_node1734874396791.setFormat("json")
StepTrainerCurated_node1734874396791.writeFrame(DropFields_node1734879067260)
job.commit()