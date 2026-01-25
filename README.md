# pyspark-local-intro — PySpark Training (Local VS Code)

Run a small PySpark mini-project locally in VS Code: read CSVs, clean/quarantine, aggregate KPIs, and join outputs.

## Prereqs
- Python 3.10+
- Java (required by Spark)

Verify:
```bash
python --version
java -version
```

## Setup

### 1) Create and activate a virtual environment

**Windows (PowerShell)**
```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
```

**WSL/Linux/macOS**
```bash
rm -rf .venv                            # Delete the existing .venv
python3 -m venv .venv                   # Create the venv inside WSL
source .venv/bin/activate               # Activate the venv (WSL)
which python                            #Check Python:
python --version
python3 -m pip install --upgrade pip    # Install deps inside the venv
pip install -r requirements.txt
```

### 2) VS Code interpreter
- `Ctrl+Shift+P` → Python: Select Interpreter
- Select `.venv`


## Project layout
```text
pyspark-local-intro/
├─ data/
│  ├─ raw/                  # input CSVs
│  └─ out/                  # Spark outputs (folders)
├─ scripts/
│  ├─ 00_smoke_test.py
│  ├─ 01_basics_df.py
│  ├─ 02_cleaning.py
│  ├─ 03_aggregations.py
│  ├─ 04_joins.py
│  └─ 05_exercises.py
├─ src/
│  └─ spark_utils.py
├─ requirements.txt
└─ README.md
```

## Sample data

Create these files:

`data/raw/customers.csv`
```csv
customer_id,first_name,last_name,state,signup_date
1,Frank,Runfola,NY,2025-01-10
2,Ana,Lopez,NC,2025-02-03
3,Sam,Kim,CA,2025-02-15
4,,Patel,TX,2025-03-01
5,Jen,Chen,CA,2025-03-05
```

`data/raw/transactions.csv`
```csv
txn_id,customer_id,txn_ts,amount,merchant
1001,1,2025-03-01 10:12:00,25.10,CoffeeCo
1002,1,2025-03-02 09:01:00,120.00,ElectroMart
1003,2,2025-03-05 12:30:00,0.00,GroceryTown
1004,2,2025-03-06 17:40:00,-5.00,GroceryTown
1005,3,2025-03-07 08:00:00,18.75,CoffeeCo
1006,99,2025-03-07 09:15:00,42.00,UnknownShop
```

## Run

Run from repo root. Use module mode (no `.py` on the module name). Ensure `scripts/__init__.py` and `src/__init__.py` exist.

```bash
python -m scripts.00_smoke_test
python -m scripts.01_basics_df
python -m scripts.02_cleaning
python -m scripts.03_aggregations
python -m scripts.04_joins
python -m scripts.05_exercises
```

Outputs land in `data/out/` (Spark writes folders; that’s normal).

## What you learn
- **00 — Smoke test**            : Spark runs locally + create a DataFrame
- **01 — DataFrame basics**      : read CSVs + select/filter + derived columns
- **02 — Cleaning + quarantine** : normalize + quarantine vs clean outputs
- **03 — Aggregations**          : customer spend KPIs + ranking 
- **04 — Joins**                 : join fact + dimension + KPI table
- **05 — Exercises**             : implement TODOs (quality rules + KPIs + writes)

## Exercises (05_exercises.py)
Fill in the TODOs:
- normalize names and create `full_name`
- quarantine txns where `amount <= 0` or `customer_id` is missing/not found
- build KPIs: `txn_count`, `total_spend`, `last_txn_ts`
- write outputs to `data/out/`

## Common issues
- Java missing: `java -version` must work
- Wrong interpreter in VS Code: select `.venv`
- Running from wrong folder: run commands from repo root
- Spark outputs folders, not single files

## Git (optional)
```text
cd ~/projects/pyspark-local-intro
git init
git add .
git commit -m "Initial PySpark local training project"
git branch -M main
git remote add origin https://github.com/frankrunfola-training/pyspark-local-intro.git
git push -u origin main
```
