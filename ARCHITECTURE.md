# Architecture & Code Flow

> Visual guide to how the Data Validation Framework works.

---

## 1. High-Level Overview

```mermaid
flowchart TD
    A([User runs main.py\n--config config.yaml]) --> B[Load YAML Config]
    B --> C{result_db type\n= jdbc?}
    C -- Yes --> D[Check PostgreSQL\nConnectivity via psycopg2]
    D -- Fail --> E([Exit with error])
    D -- Pass --> F[✔ Print green success\nAuto-create result tables]
    C -- No --> F
    F --> G[Start Spark Session]
    G --> H[Loop: tables_to_validate]
    H --> I[DataValidator\n.run_validation]
    I --> J{More tables?}
    J -- Yes --> H
    J -- No --> K[Stop Spark]
    K --> L([Done])
```

---

## 2. Config Resolution Per Table

Each table entry can independently override the global source/target DB.

```mermaid
flowchart LR
    subgraph config.yaml
        A[global source_db]
        B[global target_db]
        C["tables_to_validate[n]"]
    end

    subgraph Per-Table Resolution
        C -->|has source.db?| D{Override?}
        D -- Yes --> E[Use table-level\nsource db config]
        D -- No  --> F[Use global source_db]

        C -->|has target.db?| G{Override?}
        G -- Yes --> H[Use table-level\ntarget db config]
        G -- No  --> I[Use global target_db]

        C --> J[source.table_name\ntarget.table_name\ncan differ]
    end
```

---

## 3. Validation Pipeline (per table)

```mermaid
flowchart TD
    A([run_validation]) --> B[Read Source DataFrame\nvia read_dataset]
    B --> C[Read Target DataFrame\nvia read_dataset]
    C --> D[DDL Check\nvalidate_ddl]
    D -- Mismatch --> E[Log DDL_FAIL\nto validation_summary]
    D -- Match --> F[Row Count Check\nvalidate_row_counts]
    F -- Mismatch --> G[Log COUNT_FAIL\nto validation_summary]
    F -- Match --> H[Aggregates Check\nget_column_aggregates]
    H --> I[Warn if agg mismatch\nbut continue]
    I --> J[Deep Row Validation\nperform_deep_validation]
    J --> K{Errors found?}
    K -- Yes --> L[Write error rows\nto validation_errors]
    K -- No --> M[Log SUCCESS]
    L --> N[Log DATA_MISMATCH\nto validation_summary]
    M --> O([✔ Done])
    N --> O
```

---

## 4. Deep Row-by-Row Validation

```mermaid
flowchart TD
    A([perform_deep_validation]) --> B[Hash all columns\nper row in source]
    A --> C[Hash all columns\nper row in target]
    B --> D[Full Outer Join\non primary key]
    C --> D
    D --> E1[MissingInTarget\ntarget PK is null]
    D --> E2[MissingInSource\nsource PK is null]
    D --> E3[Hash mismatch\nboth PKs exist]
    E3 --> F[Unpivot source & target\nto long format]
    F --> G[Join on PK + col_name\nfilter differing values]
    G --> H[ValueMismatch rows]
    E1 --> I[Union all error types]
    E2 --> I
    H --> I
    I --> J[write_result → validation_errors]
```

---

## 5. Data Flow: Source to Result

```mermaid
flowchart LR
    S1[(Source DB\nor CSV)] -->|Spark read| V[DataValidator]
    S2[(Target DB\nor CSV)] -->|Spark read| V
    V -->|JDBC write| R1[(PostgreSQL\nvalidation_summary)]
    V -->|JDBC write| R2[(PostgreSQL\nvalidation_errors)]
```

---

## 6. Source Types Supported

```mermaid
flowchart LR
    A[read_dataset] --> B{type}
    B -- jdbc --> C[Spark JDBC Read\nPostgreSQL / any JDBC DB]
    B -- csv  --> D[Spark CSV Read\nwith header & inferSchema]
    B -- parquet --> E[Spark Parquet Read]
    B -- json --> F[Spark JSON Read]
```

---

## 7. File & Module Responsibilities

```mermaid
graph TD
    subgraph src/
        M[main.py\n────────────\nArgparse --config\nYAML loader\npsycopg2 connectivity check\nSpark session init\nOrchestration loop]
        V[validator.py\n────────────\nDataValidator class\nread_dataset\nwrite_result\nvalidate_ddl\nvalidate_row_counts\nget_column_aggregates\nperform_deep_validation\nlog_summary\nrun_validation]
    end
    subgraph config/
        Y[config.yaml\n────────────\napp_name\nspark_config\nsource_db\ntarget_db\nresult_db\ntables_to_validate]
    end
    subgraph jars/
        J[postgresql-42.2.18.jar\n────────────\nJDBC driver for Spark\nto write results to PG]
    end

    M -- loads --> Y
    M -- instantiates --> V
    V -- uses JAR via Spark --> J
```
