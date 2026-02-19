# Data Validation Framework (PySpark)

A robust, config-driven framework to validate data between Source and Target databases using **PySpark**, storing all results in **PostgreSQL**.

---

## Features

- **YAML Configuration** — all connections, tables, and overrides in one clean file
- **Per-Table DB Overrides** — source and target can point to different databases/tables per table entry
- **Sequential Validation Pipeline**:
  1. **DDL Check** — column names and data types must match
  2. **Row Count Check** — total record counts must match
  3. **Aggregates Check** — averages (numeric) and average string lengths
  4. **Deep Row Validation** — row-by-row hash comparison (missing rows + value mismatches at column level)
- **PostgreSQL Result Storage** — summary and error tables auto-created on first run
- **Coloured Terminal Output** — green ✔ for connection success and result storage

---

## Project Structure

```
Validation_Framework/
├── config/
│   └── config.yaml          # All configuration (sources, targets, result DB, tables)
├── data/
│   ├── source/              # Source CSV files
│   └── target/              # Target CSV files
├── jars/
│   └── postgresql-42.2.18.jar   # PostgreSQL JDBC driver for Spark
├── src/
│   ├── main.py              # Entry point — DB check, Spark init, orchestration
│   └── validator.py         # Core validation logic
├── README.md                # Setup & usage (this file)
└── ARCHITECTURE.md          # Code flow diagrams
```

---

## Prerequisites

| Requirement | Details |
|---|---|
| Python | 3.x via `spark_env` conda environment |
| PySpark | Installed in `spark_env` |
| psycopg2 | Installed in `spark_env` (`pip install psycopg2-binary`) |
| PyYAML | Installed in `spark_env` (`pip install pyyaml`) |
| PostgreSQL | Running locally (result tables are auto-created) |
| Java | Required by Spark (JDK 11+) |

---

## Setup

### 1. Activate the Spark Environment

```bash
conda activate spark_env
```

### 2. Configure `config/config.yaml`

Edit the file with your database credentials and tables:

```yaml
# Global source database (CSV example)
source_db:
  type: csv
  path: "/path/to/data/source/"
  format_options:
    header: "true"
    inferSchema: "true"

# Global target database
target_db:
  type: csv
  path: "/path/to/data/target/"
  format_options:
    header: "true"
    inferSchema: "true"

# PostgreSQL result database
result_db:
  type: jdbc
  url: "jdbc:postgresql://localhost:5432/postgres"
  user: your_pg_user
  password: "your_password"
  driver: org.postgresql.Driver
  summary_table: validation_summary
  error_table: validation_errors

# Tables to validate
tables_to_validate:
  - primary_key: emp_id
    source:
      table_name: employees.csv    # file name for CSV, table name for JDBC
      schema: ""
    target:
      table_name: employees.csv    # can be a different name from source
      schema: ""
```

#### Using a Different Database Per Table

Uncomment and fill the `db:` block under `source:` or `target:`:

```yaml
tables_to_validate:
  - primary_key: user_id
    source:
      table_name: users
      schema: public
      db:                                          # overrides global source_db
        type: jdbc
        url: "jdbc:postgresql://prod-host:5432/prod_db"
        user: prod_user
        password: "prod_pass"
        driver: org.postgresql.Driver
    target:
      table_name: users_staging                   # different table name is fine
      schema: public
      db:                                          # overrides global target_db
        type: jdbc
        url: "jdbc:postgresql://staging-host:5432/staging_db"
        user: staging_user
        password: "staging_pass"
        driver: org.postgresql.Driver
```

---

## Running

```bash
# Default config path (config/config.yaml)
python src/main.py

# Custom config path
python src/main.py --config /path/to/your/config.yaml
```

---

## What Happens at Runtime

```
1. ✔  PostgreSQL connectivity check  (prints green on success)
2.    Spark session initialised
3.    For each table in config:
        a. DDL check
        b. Row count check
        c. Aggregate check
        d. Deep row-by-row validation
        e. ✔  Results stored → PostgreSQL  (prints green on success)
```

---

## Result Tables in PostgreSQL

### `validation_summary`

| Column | Description |
|---|---|
| `run_id` | Timestamp-based unique run identifier |
| `table_name` | Source (→ target) table label |
| `status` | `SUCCESS`, `DATA_MISMATCH`, `DDL_FAIL`, `COUNT_FAIL`, `ERROR` |
| `message` | Human-readable result message |
| `source_count` | Row count from source |
| `target_count` | Row count from target |
| `metrics` | Aggregates dictionary |
| `timestamp` | When the run completed |

### `validation_errors`

| Column | Description |
|---|---|
| `table_name` | Table that produced the error |
| `pk_value` | Primary key value of the mismatched row |
| `col_name` | Column with the mismatch (`RELATION` = entire row missing) |
| `source_value` | Value found in source |
| `target_value` | Value found in target |
| `error_type` | `ValueMismatch`, `MissingInTarget`, `MissingInSource` |
| `timestamp` | When the error was captured |

---

## Query Results

```bash
# Connect
psql -U <your_pg_user> -d postgres

# View runs
SELECT * FROM validation_summary ORDER BY timestamp DESC;

# View errors
SELECT * FROM validation_errors ORDER BY timestamp DESC;
```

---

## See Also

- [`ARCHITECTURE.md`](./ARCHITECTURE.md) — code flow diagrams
