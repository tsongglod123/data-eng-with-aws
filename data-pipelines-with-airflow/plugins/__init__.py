from airflow.plugins_manager import AirflowPlugin

import operators

class ProjectPlugin(AirflowPlugin):
    name = "project_plugin"
    operators = [
        operators.StageToRedshiftOperator,
        operators.LoadFactOperator,
        operators.LoadDimensionOperator,
        operators.DataQualityOperator
    ]
    