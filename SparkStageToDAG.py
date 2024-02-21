# Databricks notebook source
dbutils.widgets.text("stage_id", "5")
dbutils.widgets.text("working_dir", "/Volumes/sternp/default/volume/tmp/stage_highlighter/Eventlogs/current/")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC # About this notebook
# MAGIC
# MAGIC When diagnosing performance issues in Spark it's common to quickly identify both the slowest stage and the slowest SQL query but then have no way to figure out where in the SQL DAG the slowest stage maps to. This script, written for Databricks, fixes this. Just plug in the ID of a stage, run the notebook SparkStageToDAG, and it will print out which nodes in the DAG the stage corresponds to.
# MAGIC
# MAGIC It needs to run on the same cluster as the stage/query you're trying to debug.
# MAGIC
# MAGIC ## How to use
# MAGIC
# MAGIC 1. Set the `stage_id` parameter to the ID of the stage you're interested in
# MAGIC 1. Set the `working_dir` parameter to a DBFS path or a Volume.  The script will copy logs from your cluster to this location & then process them.
# MAGIC 1. Run the `SparkStageToDAG` on the cluster that ran the query you're trying to profile.
# MAGIC
# MAGIC ## Interpreting the results
# MAGIC
# MAGIC In the last cell you will get a table that looks like this:
# MAGIC
# MAGIC |node_id|node_name|
# MAGIC |-------|---------|
# MAGIC |7|PhotonScan parquet sternp.tmp.flights_tmp|
# MAGIC |8|PhotonShuffleExchangeSink|
# MAGIC |9|PhotonShuffleMapStage|
# MAGIC
# MAGIC The `node_id` is the most important column.  It tells you the id of the node in the SQL DAG.  In the SQL DAG, this number will show up in the name of the node:  
# MAGIC
# MAGIC <img src="https://peterstern.blob.core.windows.net/publicfiles/get_stage_sql_dag.png" width=75%>
# MAGIC
# MAGIC The `node_id` is also used in the SQL Plan in the details under the DAG:
# MAGIC
# MAGIC <img src="https://peterstern.blob.core.windows.net/publicfiles/sql_physical_plan.png" width=75%>
# MAGIC
# MAGIC The node_name is just for informational purpose and may provide a clue before you even look at the SQL DAG.  
# MAGIC
# MAGIC ## Limitations
# MAGIC
# MAGIC * Doesn't work on shared clusters
# MAGIC * Only works on the same cluster as the stage/query you're trying to debug.
# MAGIC
# MAGIC ## Built By
# MAGIC
# MAGIC Peter Stern

# COMMAND ----------

# DBTITLE 1,Get Parameters
stage_id = int(dbutils.widgets.get('stage_id'))
working_dir = dbutils.widgets.get('working_dir') + '/current'

# COMMAND ----------

# DBTITLE 1,Remove old event logs
import os
os.system(f'rm -r {working_dir}')

# COMMAND ----------

# DBTITLE 1,Create working dir
os.system(f'mkdir -p {working_dir}')

# COMMAND ----------

# DBTITLE 1,Save event logs from this cluster to working dir
os.system(f'cp -r /databricks/driver/eventlogs/* {working_dir}')

# COMMAND ----------

from pyspark.sql.types import *
import json

eventlog_schema = None
with open('./schema.json', 'r') as f:
  eventlog_schema_json = f.read()
  eventlog_schema = StructType.fromJson(json.loads(eventlog_schema_json))

df = spark.read.json(f'{working_dir}/*', schema=eventlog_schema).cache()
df.createOrReplaceTempView('event_log')

# COMMAND ----------

# DBTITLE 1,Get accumulator IDs from Stage
accumulators = sql(f"""select id from(
                  select explode(`Stage Info`.Accumulables.ID) as id from event_log where event = 'SparkListenerStageCompleted' and `Stage Info`.`Stage ID` = {stage_id}
                )""").collect()

accumulator_ids = [row['id'] for row in accumulators]

# COMMAND ----------

# DBTITLE 1,Get SQL ID
sql_id = int(sql(f"select Properties.`spark.sql.execution.id` from event_log where event = 'SparkListenerStageSubmitted' and `Stage Info`.`Stage ID` = {stage_id}").first()['spark.sql.execution.id'])

# COMMAND ----------

# DBTITLE 1,Get SQL Plan
sql_plan_str = sql(f"""select string(sparkPlanInfo) from event_log where 
                  (event = 'org.apache.spark.sql.execution.ui.SparkListenerSQLAdaptiveExecutionUpdate' 
                    or event = 'org.apache.spark.sql.execution.ui.SparkListenerSQLExecutionStart') 
                  and executionId = {sql_id}
                  and sparkPlanInfo:simpleString != 'AdaptiveSparkPlan isFinalPlan=false'
                """).first()['sparkPlanInfo']

sql_plan = json.loads(sql_plan_str)

# COMMAND ----------

# DBTITLE 1,Find all SQL node IDs that have accumulators from Stage
matching_sql_nodes = []

def get_matching_nodes(sql_plan):
  for child in sql_plan['children']:
    get_matching_nodes(child)

  for metric in sql_plan['metrics']:
    if metric['accumulatorId'] in accumulator_ids:
      matching_sql_nodes.append((sql_plan['explainId'], sql_plan['nodeName']))
      break

get_matching_nodes(sql_plan)

# COMMAND ----------

# DBTITLE 1,Show matching nodes
matching_sql_nodes

matches_schema = StructType([       
    StructField('node_id', IntegerType(), True),
    StructField('node_name', StringType(), True)
])

matches_df = spark.createDataFrame(matching_sql_nodes, schema = matches_schema)
matches_df.display()

# COMMAND ----------

os.system(f'rm -r {working_dir}')
