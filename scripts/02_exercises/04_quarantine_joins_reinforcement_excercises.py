#########################################################################
# File   : scripts/02_exercises/04_quarantine_joins_reinforcement_excercises.py
# Author : Frank Runfola
# Date   : 2/3/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.02_exercises.04_quarantine_joins_reinforcement_excercises
# -----------------------------------------------------------------------
# Description:
#   Extra reinforcement for quarantining + joins (new).
#   Exercises 25-32. Starts easy, ramps to windows/analytics.
#########################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("04_quarantine_joins_reinforcement_excercises")

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
# SETUP (Used by the reinforcement exercises below)
#------------------------------------------------------------------------
# Keep this file focused on quarantine + joins practice, ramping difficulty.
#########################################################################

# -----------------------------
# 1) Basic "clean" sets
# -----------------------------
is_bad_first = F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")
cust_clean   = customers.filter(~is_bad_first)

txns = txns.withColumn("amount", F.col("amount").cast("double"))
bad_txn = (F.col("amount") <= 0) | (F.col("customer_id").isNull())
txns_clean = txns.filter(~bad_txn)

txns_good_fk = txns_clean.join(
    cust_clean.select("customer_id").dropDuplicates(),
    on="customer_id",
    how="left_semi"
)

#########################################################################
# EXERCISE 25 (Easy)
#------------------------------------------------------------------------
# Goal: Add simple quarantine flags to txns:
#   - is_missing_customer_id
#   - is_nonpositive_amount
#   - is_missing_merchant (null OR blank)
# Return columns: txn_id, customer_id, amount, merchant, the 3 flags
#########################################################################
# TODO
'''
txns_flags = (
    txns
    # TODO: withColumn(...) for each flag
    # TODO: select the requested columns
)

txns_flags.show(n=10, truncate=False)
'''

#########################################################################
# EXERCISE 26 (Easy -> Medium)
#------------------------------------------------------------------------
# Goal: Create txns_quarantine_easy where ANY flag from Exercise 25 is True,
# and txns_clean_easy as the remaining rows.
# Print both counts.
#########################################################################
# TODO
'''
txns_quarantine_easy = (
    # TODO: filter using your boolean flags
)

txns_clean_easy = (
    # TODO: filter to the opposite set
)

print("txns_clean_easy:", txns_clean_easy.count())
print("txns_quarantine_easy:", txns_quarantine_easy.count())
print("")
'''

#########################################################################
# EXERCISE 27 (Medium)
#------------------------------------------------------------------------
# Goal: Add a quarantine_reason column (single string) to txns_quarantine_easy:
#   - "missing_customer_id"
#   - "nonpositive_amount"
#   - "missing_merchant"
# If multiple are true, pick the first matching reason in the order above.
# Return: txn_id, customer_id, amount, merchant, quarantine_reason
#########################################################################
# TODO
'''
txns_quarantine_with_reason = (
    txns_quarantine_easy
    # TODO: withColumn("quarantine_reason", ...)
    # TODO: select requested columns
)

txns_quarantine_with_reason.show(n=20, truncate=False)
'''

#########################################################################
# EXERCISE 28 (Easy)
#------------------------------------------------------------------------
# Goal: Build an enriched transaction dataset:
# Inner join txns_good_fk to cust_clean, returning:
# txn_id, customer_id, first_name, last_name, state, amount, merchant
#########################################################################
# TODO
'''
enriched_txns = (
    # TODO: join then select
)

enriched_txns.show(n=10, truncate=False)
'''

#########################################################################
# EXERCISE 29 (Medium)
#------------------------------------------------------------------------
# Goal: Find orphan transactions using an anti-join:
# Return txns_clean rows whose customer_id is NOT in cust_clean.
# Show: txn_id, customer_id, amount, merchant
#########################################################################
# TODO
'''
txns_orphan = (
    # TODO: left_anti join to cust_clean
)

txns_orphan.select("txn_id", "customer_id", "amount", "merchant").show(n=20, truncate=False)
'''

#########################################################################
# EXERCISE 30 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Customer-level KPI warmup from txns_good_fk:
# Produce customer_kpis with:
#   customer_id
#   txn_count
#   total_amount
#   avg_amount
#########################################################################
# TODO
'''
customer_kpis = (
    txns_good_fk
    # TODO: groupBy("customer_id") and aggregate
)

customer_kpis.show(truncate=False)
'''

#########################################################################
# EXERCISE 31 (Hard)
#------------------------------------------------------------------------
# Goal: "Top merchants" per state:
# 1) Enrich txns_good_fk with state from cust_clean
# 2) For each (state, merchant), compute total_amount
# 3) For each state, rank merchants by total_amount descending
# Return top 3 merchants per state.
#########################################################################
# TODO
'''
from pyspark.sql.window import Window

txns_state = (
    # TODO: join txns_good_fk to cust_clean, keep state + merchant + amount
)

merchant_by_state = (
    # TODO: groupBy("state","merchant") and sum amount
)

w = Window.partitionBy("state").orderBy(F.col("total_amount").desc())

top3 = (
    merchant_by_state
    # TODO: add rank column and filter rank <= 3
    # TODO: orderBy state then rank
)

top3.show(truncate=False)
'''

#########################################################################
# EXERCISE 32 (Very Hard)
#------------------------------------------------------------------------
# Goal: First purchase per customer:
# For each customer_id in txns_good_fk, return:
#   - first_txn_ts (min txn_ts)
#   - first_merchant (merchant on that first_txn_ts)
# Tip: You will likely need a window ordered by txn_ts.
#########################################################################
# TODO
'''
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("txn_ts").asc())

first_purchase = (
    txns_good_fk
    # TODO: add row_number over the window
    # TODO: filter to row_number == 1
    # TODO: select customer_id, txn_ts as first_txn_ts, merchant as first_merchant
)

first_purchase.show(truncate=False)
'''
