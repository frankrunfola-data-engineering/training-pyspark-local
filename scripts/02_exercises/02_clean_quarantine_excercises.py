#########################################################################
# File   : scripts/02_exercises/02_clean_quarantine_excercises.py
# Author : Frank Runfola
# Date   : 2/3/2026
# -----------------------------------------------------------------------
# Run (from repo root):
#   cd ~/projects/training-pyspark-local
#   python -m scripts.02_exercises.02_clean_quarantine_excercises
# -----------------------------------------------------------------------
# Description:
#   Cleaning + Quarantine exercises (from the original combined file).
#   Exercises 11-19.
#########################################################################

from pyspark.sql import functions as F
from training_pyspark_local.spark_utils import get_spark

spark = get_spark("02_clean_quarantine_excercises")

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
# EXERCISE 11 (Medium)
#------------------------------------------------------------------------
# Goal: Standardize customers:
#   - trim first_name and last_name
#   - uppercase state
# Return full standardized customers DataFrame
#########################################################################
# TODO
'''
customers_std = (
    customers
    .withColumn("first_name",F.trim(F.col("first_name")))
    .withColumn("last_name", F.trim(F.col("last_name")))
    .withColumn("state"     ,F.upper(F.trim(F.col("state"))))
)
customers_std.show(truncate=False)
'''

#########################################################################
# EXERCISE 12 (Medium)
#------------------------------------------------------------------------
# Goal: Create customers_quarantine where first_name is missing OR blank
# And customers_clean as the remaining records
#########################################################################
# TODO
'''
# Column that evaluates to True/False per row.
is_bad_first = F.col("first_name").isNull() | (F.trim(F.col("first_name")) == "")
cust_quarantine = customers.filter(is_bad_first)
cust_clean      = customers.filter(~is_bad_first)
print("cust_clean:", cust_clean.count())
print("cust_quarantine:", cust_quarantine.count())
print("")
'''

#########################################################################
# EXERCISE 13 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Create txns_quarantine where:
#  - amount <= 0 OR customer_id is null
#  - And txns_clean as the remaining records
#########################################################################\
# TODO
'''
txns = txns.withColumn("amount", F.col("amount").cast("double"))  #first ensure amount is double
badData = (F.col("amount")<=0) | (F.col("customer_id").isNull())  #non-positive amt, or bad custId
txns_quarantine = txns.filter(badData)
txns_clean      = txns.filter(~badData)
print("txns_clean:", txns_clean.count())
print("txns_quarantine:", txns_quarantine.count())
#txns_quarantine.show(truncate=False)
print("")
'''

#########################################################################
# EXERCISE 14 (Medium -> Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where merchant does NOT exist in an allowed
#       merchant reference table (allowed_merchants)
#------------------------------------------------------------------------
# Produce:
#   - txns_bad_merchant
#   - txns_good_merchant
#------------------------------------------------------------------------
# Notes:
# - This is the same “reference integrity” idea as FK checks, but easier:
#   it’s a single-column lookup instead of a full customer join.
#########################################################################
# TODO
'''
from pyspark.sql import functions as F

allowed_merchant_list = [
    ("ElectroMart",),
    ("CoffeeCo",),
    ("BookBarn",),
    ("FuelStop",),
    ("OnlineHub",),
    ("GroceryTown",),
    ("PharmaPlus",),
    ("QuickEats",),
    ("HomeGoods",),
    ("PetPlace",),
    ("TravelNow",),
    ("GymZone",),
]

allowed_merchants = spark.createDataFrame(allowed_merchant_list, ["merchant"])

# Normalize merchant for matching (trim + lowercase)
txns_norm = (
    txns_clean.filter(F.trim(f.col()))
    # TODO: create a normalized merchant column
)

allowed_norm = (
    allowed_merchants
    # TODO: normalize merchant the same way + drop duplicates
)

# txns with merchant NOT found in allowed_merchants (quarantine)
txns_bad_merchant = (
    # TODO: join with left_anti using the normalized merchant key
)

# txns with merchant found in allowed_merchants (clean)
txns_good_merchant = (
    # TODO: join with left_semi using the normalized merchant key
)

print(f"txns_good_merchant: ${txns_good_merchant.count()}")
print(f"txns_bad_merchant: ${txns_bad_merchant.count()}")
print("")
'''

#########################################################################
# EXERCISE 15 (Hard)
#------------------------------------------------------------------------
# Goal: Add a quarantine_reason column to txns_quarantine with values:
#   - "non_positive_amount" when amount <= 0
#   - "missing_customer_id" when customer_id is null
# (If both happen, choose one consistent rule)
#########################################################################
# TODO
'''
txns_quarantine_reasoned = (
    txns
    .withColumn(
        "quarantine_reason",
         F.when(F.col("customer_id").isNull(),F.lit("missing_customer_id"))
          .when(F.col("amount")<=0,F.lit("non_positive_amount"))
          .otherwise(F.lit(None)))
)
txns_quarantine_reasoned.show(truncate=False)
'''

#########################################################################
# EXERCISE 16 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where customer_id does NOT exist in customers_clean
# Produce:
#   - txns_bad_fk
#   - txns_good_fk
#########################################################################
# TODO
'''
# txns with customer_id not found in cust_clean (bad FK)
txns_bad_fk = txns.join(
    cust_clean.select("customer_id").dropDuplicates(),
    on="customer_id",
    how="left_anti"  #“rows in left that do not match” (exactly quarantine)
)
# txns with valid customer_id (good FK)
txns_good_fk = txns.join(
    cust_clean.select("customer_id").dropDuplicates(),
    on="customer_id",
    how="left_semi"  #“rows in left that do match” (exactly clean)
)

print(f"txns_good_fk: ${txns_good_fk.count()}")
print(f"txns_bad_fk: ${txns_bad_fk.count()}")
print("")
'''

#########################################################################
# EXERCISE 17 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine orphan customers where customer_id has NO matching
#       transactions in txns_clean
#------------------------------------------------------------------------
## Produce:
# - customers_orphan   (customers with zero transactions)
# - customers_active   (customers with 1+ transactions)
#------------------------------------------------------------------------
# Rules:
# - Join key: customer_id
# - Treat null/blank customer_id in customers_clean as orphan automatically
# - Keep all original customer columns in both outputs
#########################################################################
# TODO
'''
badCustId = (  #Flag bad customer_id values (null or blank after trimming)
    F.col("customer_id").isNull()
    | (F.trim(F.col("customer_id").cast("string")) == "")
)

# Split customers by customer_id presence
customers_bad_id = customers.filter(badCustId)
customers_valid_id = customers.filter(~badCustId)

# Distinct customer_ids that appear in txns_clean (smaller join table)
txns_keys = txns_clean.select("customer_id").dropDuplicates()

# Orphan customers (valid id) = no matching transaction (left_anti = "not found")
customers_no_txns = customers_valid_id.join(txns_keys, on="customer_id", how="left_anti")

# Active customers (at least one matching transaction) (inner = "found")
customers_active = customers_valid_id.join(txns_keys, on="customer_id", how="inner")

# Final orphan set (bad-id customers + valid-id customers with no transactions)
customers_orphan = customers_bad_id.unionByName(customers_no_txns)

# Quick check: print counts for each group
print(f"customers_active: ${customers_active.count()}")
print(f"customers_orphan: ${customers_orphan.count()}")
print("")
'''

#########################################################################
# EXERCISE 18 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine orphan transactions 
#   Where txns_clean.customer_id has NO matching customer in cust_clean
#------------------------------------------------------------------------
## Produce:
#   - txns_orphan_fk   (transactions with missing/invalid customer reference)
#   - txns_valid_fk    (transactions with a valid customer reference)
#------------------------------------------------------------------------
# Rules:
#   - Join key: customer_id
#   - Treat null/blank customer_id in txns_clean as orphan automatically
#   - Keep all original transaction columns in both outputs
#########################################################################
# TODO
'''
bad_cust_id = (  # Condition: txns_clean customer_id is missing/blank
    F.col("customer_id").isNull()
    | (F.trim(F.col("customer_id").cast("string")) == "")
)

# Split txns by customer_id presence
txns_bad_id = txns_clean.filter(bad_cust_id)
txns_with_id = txns_clean.filter(~bad_cust_id)

# Small reference table of valid customer_ids (dedup for faster joins)
cust_keys = cust_clean.select("customer_id").dropDuplicates()

# Orphan FK txns: customer_id NOT found in cust_clean
txns_missing_customer = txns_with_id.join(cust_keys, on="customer_id", how="left_anti")

# Valid FK txns (customer_id found in cust_clean)
txns_valid_fk = txns_with_id.join(cust_keys, on="customer_id", how="inner")

# Final orphan set  (missing/blank id) + (id present but not in customers)
txns_orphan_fk = txns_bad_id.unionByName(txns_missing_customer)

print(f"txns_valid_fk: ${txns_valid_fk.count()}")
print(f"txns_orphan_fk: ${txns_orphan_fk.count()}")
print("")
'''

#########################################################################
# EXERCISE 19 (Hard)
#------------------------------------------------------------------------
# Goal: Quarantine transactions where (state, txn_type) does NOT exist
#       in an allowed reference table (allowed_pairs)
#------------------------------------------------------------------------
# Inputs:
#   - txns_clean: txn_id, customer_id, state, txn_type, amount
#   - allowed_pairs: state, txn_type
#------------------------------------------------------------------------
# Produce:
#   - txns_bad_ref    (invalid (state, txn_type) OR missing keys)
#   - txns_good_ref   (remaining records)
#------------------------------------------------------------------------
# Rules:
#   - Normalize for matching:
#      - state: trim + uppercase
#      - txn_type: trim + lowercase
#   - If state OR txn_type is null/blank -> quarantine
#   - Add quarantine_reason to txns_bad_ref with one of:
#      - "missing_state"
#      - "missing_txn_type"
#      - "invalid_state_txn_type"
#########################################################################
# TODO
# 1) Normalize both txns_clean and allowed_pairs for matching
'''
# Reference table: allowed (state, txn_type) combinations
allowed_pairs = spark.createDataFrame(
    [
        ("NY", "deposit"),
        ("NY", "withdrawal"),
        ("CA", "deposit"),
        ("CA", "withdrawal"),
        ("TX", "deposit"),
        ("FL", "withdrawal"),
    ],
    ["state", "txn_type"]
)

txns_norm = (
    txns_clean
    .withColumn("state_norm", F.upper(F.trim(F.col("state"))))
    .withColumn("txn_type_norm", F.lower(F.trim(F.col("txn_type"))))
)

allowed_norm = (
    allowed_pairs
    .withColumn("state_norm", F.upper(F.trim(F.col("state"))))
    .withColumn("txn_type_norm", F.lower(F.trim(F.col("txn_type"))))
    .select("state_norm", "txn_type_norm")
    .dropDuplicates()
)

# 2) Missing-key checks (Column expressions)
state_missing = txns_norm["state_norm"].isNull() | (txns_norm["state_norm"] == "")
type_missing  = txns_norm["txn_type_norm"].isNull() | (txns_norm["txn_type_norm"] == "")

txns_missing_state = txns_norm.filter(state_missing).withColumn("quarantine_reason", F.lit("missing_state"))
txns_missing_type  = txns_norm.filter(~state_missing & type_missing).withColumn("quarantine_reason", F.lit("missing_txn_type"))

# 3) Pair validity check (only where both keys are present)
txns_keys_present = txns_norm.filter(~state_missing & ~type_missing)

txns_invalid_pair = (
    txns_keys_present
    .join(allowed_norm, on=["state_norm", "txn_type_norm"], how="left_anti")
    .withColumn("quarantine_reason", F.lit("invalid_state_txn_type"))
)

txns_good_ref = txns_keys_present.join(allowed_norm, on=["state_norm", "txn_type_norm"], how="inner")

# 4) Combine all quarantined rows
txns_bad_ref = txns_missing_state.unionByName(txns_missing_type).unionByName(txns_invalid_pair)

print("txns_bad_ref:", txns_bad_ref.count())
print("txns_good_ref:", txns_good_ref.count())
print("")
'''
