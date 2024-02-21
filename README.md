# SparkStageToDAG

When diagnosing performance issues in Spark it's common to quickly identify both the slowest stage and the slowest SQL query but then have no way to figure out where in the SQL DAG the slowest stage maps to. This script, written for Databricks, fixes this. Just plug in the ID of a stage, run the notebook SparkStageToDAG, and it will print out which nodes in the DAG the stage corresponds to.

It needs to run on the same cluster as the stage/query you're trying to debug.

## How to use

1. Set the `stage_id` parameter to the ID of the stage you're interested in
1. Set the `working_dir` parameter to a DBFS path or a Volume.  The script will copy logs from your cluster to this location & then process them.
1. Run the `SparkStageToDAG` on the cluster that ran the query you're trying to profile.

## Interpreting the results

In the last cell you will get a table that looks like this:

|node_id|node_name|
|-------|---------|
|7|PhotonScan parquet sternp.tmp.flights_tmp|
|8|PhotonShuffleExchangeSink|
|9|PhotonShuffleMapStage|

The `node_id` column is most important.  It tells you the id of the node in the SQL DAG.  This number will show up in the name of the node in the SQL DAG:  

<img src="https://peterstern.blob.core.windows.net/publicfiles/get_stage_sql_dag.png" width=75%>

The `node_id` is also used in the SQL Plan in the details under the DAG:

<img src="https://peterstern.blob.core.windows.net/publicfiles/sql_physical_plan.png" width=75%>

The node_name is just for informational purpose and may provide a clue before you even look at the SQL DAG.  

## Limitations

* Doesn't work on shared clusters
* Only works on the same cluster as the stage/query you're trying to debug.

## Built By

Peter Stern
