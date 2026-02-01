#########################################################################
# File   : scripts/08_performance_debugging/08_01_explain_and_shuffles.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.04_performance_debugging.01_explain_and_shuffles
# -----------------------------------------------------------------------
# Description:
#   Reading explain plans + spotting shuffles. Also shows repartition vs coalesce.
#########################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("08_01_explain_and_shuffles")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
).select("transaction_id", "customer_id", "amount")

print("\n--- partitions ---")
print("initial partitions:", txns.rdd.getNumPartitions())

# A groupBy causes a shuffle (expensive at scale)
kpis = txns.groupBy("customer_id").agg(
    F.count("*").alias("txn_cnt"),
    F.round(F.sum("amount"), 2).alias("total_amount"),
)

print("\n--- explain (look for Exchange / shuffle) ---")
kpis.explain(True)

# Repartition increases partitions (shuffle)
repart = txns.repartition(8, "customer_id")
print("\nrepartition partitions:", repart.rdd.getNumPartitions())

# Coalesce reduces partitions (no full shuffle)
coal = repart.coalesce(2)
print("coalesce partitions:", coal.rdd.getNumPartitions())

spark.stop()
