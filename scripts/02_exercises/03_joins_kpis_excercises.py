#########################################################################
# File   : scripts/02_exercises/03_joins_kpis_excercises.py
# Author : Frank Runfola
# Date   : 2/3/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.02_exercises.03_joins_kpis_excercises
# -----------------------------------------------------------------------
# Description:
#   Joins + KPI exercises (from the original combined file).
#   Exercises 20-24. Includes a small SETUP section so it's runnable standalone.
#########################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("03_joins_kpis_excercises")

customers = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/customers.csv")
)

txns = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv("data/raw/transactions.csv")
)

#########################################################################
# SETUP (Required for joins + KPI exercises in this file)
#------------------------------------------------------------------------
# We build "clean" customer and transaction sets used by the exercises:
#   - cust_clean : customers with a non-empty first_name
#   - txns_clean : txns with amount > 0 and customer_id present
#   - txns_good_fk : txns_clean where customer_id exists in cust_clean
#########################################################################

# Customers: quarantine missing/blank first_name
is_bad_first = F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")
cust_quarantine = customers.filter(is_bad_first)
cust_clean      = customers.filter(~is_bad_first)

# Transactions: quarantine non-positive amount or missing customer_id
txns = txns.withColumn("amount", F.col("amount").cast("double"))
bad_txn = (F.col("amount") <= 0) | (F.col("customer_id").isNull())
txns_quarantine = txns.filter(bad_txn)
txns_clean      = txns.filter(~bad_txn)

# FK check: keep only txns whose customer_id exists in cust_clean
txns_good_fk = txns_clean.join(
    cust_clean.select("customer_id").dropDuplicates(),
    on="customer_id",
    how="left_semi"
)

print("cust_clean:", cust_clean.count())
print("txns_clean:", txns_clean.count())
print("txns_good_fk:", txns_good_fk.count())
print("")
#########################################################################
# EXERCISE 20 (Hard)
#------------------------------------------------------------------------
# Goal: Join txns_good_fk to customers_clean (left join) and return:
# txn_id, customer_id, first_name, state, amount, merchant
#########################################################################
# TODO
'''
enriched_txns = (
)
enriched_txns.show(truncate=False)
'''
#########################################################################
# EXERCISE 21 (Hard)
#------------------------------------------------------------------------
# Goal: Build customer KPIs from txns_good_fk:
# - txn_count
# - total_spend (rounded to 2 decimals)
# - avg_spend (rounded to 2 decimals)
# Return one row per customer_id
#########################################################################
# TODO
'''
customer_kpis = (

)
customer_kpis.show(truncate=False)
'''
#########################################################################
# EXERCISE 22 (Hard -> Very Hard)
#------------------------------------------------------------------------
# Goal: Add last_txn_ts (max txn_ts) to customer_kpis
# Return columns: customer_id, txn_count, total_spend, avg_spend, last_txn_ts
#########################################################################
# TODO
'''
customer_kpis_with_last = (

)
customer_kpis_with_last.show(truncate=False)
'''
#########################################################################
# EXERCISE 23 (Very Hard)
#------------------------------------------------------------------------
# Goal: Create customer_analytics by joining customers_clean to customer_kpis_with_last.
# Fill null KPI values with:
# - txn_count = 0
# - total_spend = 0.0
# - avg_spend = 0.0
#########################################################################
# TODO
'''
customer_analytics = (

)
customer_analytics.orderBy(F.desc("total_spend")).show(truncate=False)
'''
#########################################################################
# EXERCISE 24 (Very Hard)
#------------------------------------------------------------------------
# Goal: Write outputs (overwrite mode) to:
# - data/out/customers_clean
# - data/out/customers_quarantine
# - data/out/txns_good_fk
# - data/out/txns_bad_fk
# - data/out/customer_analytics
#########################################################################
# TODO

print("\nDone (but only if you actually finished the TODOs).")
spark.stop()
