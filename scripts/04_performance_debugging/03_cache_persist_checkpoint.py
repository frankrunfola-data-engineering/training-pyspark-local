#########################################################################
# File   : scripts/08_performance_debugging/08_03_cache_persist_checkpoint.py
# Author : Frank Runfola
# Date   : 1/30/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.04_performance_debugging.03_cache_persist_checkpoint
# -----------------------------------------------------------------------
# Description:
#   Cache vs Persist vs Checkpoint (and why they exist).
#########################################################################

import time
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("08_03_cache_persist_checkpoint")
spark.sparkContext.setLogLevel("ERROR")

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
).select("customer_id", "amount")

# A slightly heavier transform chain
df = (
    txns.filter(F.col("amount").isNotNull())
    .withColumn("amount2", F.col("amount") * 1.07)
    .groupBy("customer_id")
    .agg(F.sum("amount2").alias("sum_amount2"))
)

# 1) cache (MEMORY_ONLY by default)
t0 = time.time()
df_cached = df.cache()
df_cached.count()
t1 = time.time()

# 2) persist with a specific storage level
df_persist = df.persist(StorageLevel.MEMORY_AND_DISK)
df_persist.count()
t2 = time.time()

print(f"\ncache first action seconds: {t1 - t0:0.3f}")
print(f"persist first action seconds: {t2 - t1:0.3f}")

# 3) checkpoint cuts lineage (useful when lineage gets huge)
spark.sparkContext.setCheckpointDir("data/out/_checkpoints")
df_cp = df_cached.checkpoint(eager=True)
print("\n--- checkpointed explain (lineage is cut) ---")
df_cp.explain(True)

spark.stop()
