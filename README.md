# SparkStageToDAG

When diagnosing performance issues in Spark, it's common to quickly identify both the slowest stage and the slowest SQL query but then have no way to figure out where in the SQL DAG the slowest stage maps to.  This script, written for Databricks, fixes this.  Just plug in the id of your slowest stage and run the notebook SparkStageToDAG, and it will give you print out which nodes in the DAG the stage corresponds to.

## How to use

1. Find the stage you're interested in & put its ID in the stage_id parameter
1. Choose or create a working directory that the script can work in.  It needs to either be in DBFS or a Volume.  It needs to be a location this notebook can write to and Spark can read from.  This notebook will save the current cluster's logs in this directory.
1. Run this notebook on the cluster that ran the query you're trying to profile.  It will use the cluster's logs to determine where in the SQL DAG the stage ran.

## Interpreting the results

In the last cell you will get a table that looks like this:

|node_id|node_name|
|-------|---------|
|7|PhotonScan parquet sternp.tmp.flights_tmp|
|8|PhotonShuffleExchangeSink|
|9|PhotonShuffleMapStage|

The `node_id` is the most important column.  It tells you the id of the node in the SQL DAG.  In the SQL DAG, this number will show up in the name of the node:  

<img src="https://peterstern.blob.core.windows.net/publicfiles/get_stage_sql_dag.png" width=75%>

The node_name is just for informational purpose and may provide a clue before you even look at the SQL DAG.  
