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

# Script generated for node Customer Curated
CustomerCurated_node1734340298476 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1734340298476")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1734340298222 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1734340298222")

# Script generated for node Join
SqlQuery7442 = '''
select
    st.sensorreadingtime,
    st.serialnumber,
    st.distancefromobject
from st
join c on st.serialnumber = c.serialnumber;
'''
Join_node1734343721231 = sparkSqlQuery(glueContext, query = SqlQuery7442, mapping = {"st":StepTrainerLanding_node1734340298222, "c":CustomerCurated_node1734340298476}, transformation_ctx = "Join_node1734343721231")

# Script generated for node Step Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1734343721231, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1734340874373", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1734341188687 = glueContext.getSink(path="s3://stedi-lake-house/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1734341188687")
StepTrainerTrusted_node1734341188687.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1734341188687.setFormat("json")
StepTrainerTrusted_node1734341188687.writeFrame(Join_node1734343721231)
job.commit()