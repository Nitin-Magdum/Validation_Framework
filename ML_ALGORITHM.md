# ML Algorithm — Detailed Documentation

> How the Adaptive ML Validation Framework learns, decides, and improves over time.

---

## Overview

The framework runs **two independent ML systems in parallel** on every validation run and combines their outputs to decide _how deeply_ to validate each column:

```mermaid
flowchart LR
    PG[(PostgreSQL\nvalidation_errors)] -->|historical errors| RS[Risk Scorer\nrisk_scorer.py]
    SD[Source DataFrame\nSpark] -->|column values| AD[Anomaly Detector\nanomaly_detector.py]
    RS --> CM[Column Classifier]
    AD --> CM
    CM -->|HIGH| FD[Full Deep Validation]
    CM -->|MEDIUM| SV[10% Sample Validation]
    CM -->|LOW| SK[Skip Deep Check]
    FD --> PG2[(PostgreSQL\nResult Tables)]
    SV --> PG2
    SK --> PG2
```

---

## System 1 — Risk Scorer

### What it does
Reads the complete history of past validation errors for a table from PostgreSQL and computes a **weighted risk score** for each column.

### The Formula

```
risk_score(col) = Σ [ severity(error_type) × decay^age ] / total_runs
```

| Variable | Value | Explanation |
|---|---|---|
| `severity(ValueMismatch)` | 1.0 | Most critical — data value is wrong |
| `severity(MissingInTarget)` | 0.8 | Row exists in source but missing in target |
| `severity(MissingInSource)` | 0.6 | Extra row in target (less critical) |
| `decay` | 0.85 (configurable) | Older errors count less |
| `age` | 0, 1, 2 … | 0 = most recent run, 1 = one run ago |
| `total_runs` | count of distinct runs | Normalises across history length |

### Step-by-step walkthrough

```mermaid
flowchart TD
    A["Load all rows from\nvalidation_errors WHERE table=X"] --> B["Group errors by\nrun timestamp\n(proxy for run boundary)"]
    B --> C["For each error:\nage = how many runs ago"]
    C --> D["score += severity × decay^age"]
    D --> E["Normalise: score / total_runs"]
    E --> F{score value?}
    F -->|">= 0.6"| G["HIGH RISK\nFull deep check"]
    F -->|"0.2 – 0.6"| H["MEDIUM RISK\n10% sample check"]
    F -->|"< 0.2"| I["LOW RISK\nSkip deep check"]
```

### Concrete example

Assume `salary` has failed in the last 3 runs:

| Run ago | Error Type | Severity | Decay^age | Contribution |
|---|---|---|---|---|
| 0 (latest) | ValueMismatch | 1.0 | 0.85⁰ = 1.000 | 1.000 |
| 1 | ValueMismatch | 1.0 | 0.85¹ = 0.850 | 0.850 |
| 2 | MissingInTarget | 0.8 | 0.85² = 0.722 | 0.578 |

```
raw_score = 1.0 + 0.85 + 0.578 = 2.428
normalised = 2.428 / 3 runs = 0.809

→ 0.809 >= 0.6  →  HIGH RISK  →  Full deep validation
```

Whereas `emp_id` which never failed:
```
raw_score = 0  →  normalised = 0.0  →  LOW RISK  →  Skip deep check
```

### Why this algorithm?

> **Why not a neural network or gradient boosting?**

| Option | Problem |
|---|---|
| Neural network | Needs thousands of samples. A new table has 0 history. |
| Gradient boosting | Same — needs labelled training data upfront. |
| Rule-based thresholds | Static, never learns from new patterns. |
| **Weighted decay scoring** ✅ | Works from run 1. Improves incrementally. Explainable. Lightweight. |

The decay factor is key — it means errors from 10 runs ago have almost no influence (`0.85^10 = 0.20`) whereas an error from last run has full weight. This makes the model **self-correcting**: if a previously-failing column has been fixed and passes for several runs, its score drops naturally toward LOW RISK.

---

## System 2 — Anomaly Detector (Isolation Forest)

### What it does
Runs **Isolation Forest** on the actual column values read from the source DataFrame to detect if a column's data distribution is statistically unusual — even if that column has never failed validation before. This catches _new_ problems that haven't yet accumulated in the error history.

### Why Isolation Forest?

```mermaid
flowchart LR
    A[Source column values] --> B[Build a random\ndecision tree forest]
    B --> C[Anomalous points\nisolated in fewer splits]
    C --> D[Normal points\nneed many splits to isolate]
    D --> E[Score = avg depth\nto isolate each point]
    E --> F{mean score < -0.05?}
    F -->|Yes| G[Column FLAGGED as anomalous\nUpgrade to HIGH RISK]
    F -->|No| H[Column distribution\nappears normal]
```

**Isolation Forest** was chosen because:

| Property | Why it matters |
|---|---|
| **Unsupervised** | No labelled "good" vs "bad" data needed |
| **No distribution assumption** | Works on skewed, non-Gaussian, mixed data |
| **Fast on large data** | O(n log n) — usable inside Spark collect cycles |
| **Built into scikit-learn** | No extra dependencies, production-stable |
| **Configurable contamination** | `contamination=0.1` means "expect 10% anomalies" |

**Alternatives considered:**

| Algorithm | Why rejected |
|---|---|
| Z-score / IQR | Assumes normal distribution — fails on salary distributions |
| One-Class SVM | Slow on large datasets, hard to tune |
| Autoencoder | Requires deep learning stack, overkill for column stats |
| Local Outlier Factor | Quadratic complexity — too slow for large DFs |

### Column encoding

| Column type | What is fed to Isolation Forest |
|---|---|
| Numeric (int, long, float, double) | Raw values |
| String | String length distribution |
| Other types | Skipped |

### Regenerative learning (warm start)

```mermaid
flowchart TD
    A{Model file exists?} -->|No| B[Train fresh\nIsolationForest\nn_estimators=100]
    A -->|Yes| C[Load .pkl model]
    C --> D[Set warm_start=True\nn_estimators += 10]
    D --> E[Re-fit on new data]
    B --> F[Save updated .pkl]
    E --> F
    F --> G[Return anomaly scores]
```

Each run **adds 10 more trees** to the existing forest. The model accumulates knowledge of what "normal" looks like for this column across all runs. By run 20, the forest has 300 trees — far more robust than any single training run.

---

## System 3 — Column Classifier (combining both signals)

### What it does
Takes the risk score (from history) and the anomaly score (from Isolation Forest) and makes a final decision per column.

```mermaid
flowchart TD
    RS[Risk Score\nfrom history] --> CL[Column Classifier]
    AS[Anomaly Score\nfrom Isolation Forest] --> CL
    CL --> D1{Has history?}
    D1 -->|No| NEW[NEW → Full check\nconservative first run]
    D1 -->|Yes| D2{Risk score level}
    D2 --> D3{Is column\nanomaly detected?}
    D3 -->|anomaly AND level=LOW or MEDIUM| UPG[Upgrade to HIGH]
    D3 -->|No anomaly| KEEP[Keep original level]
    UPG --> HIGH[HIGH → Full deep\nvalidation]
    KEEP --> D4{Level}
    D4 -->|HIGH| HIGH
    D4 -->|MEDIUM| MED[MEDIUM → 10%\nsample validation]
    D4 -->|LOW| LOW[LOW → Skip\ndeep check]
```

**The anomaly detector acts as a safety net:** even if a column has been LOW RISK for 100 runs, if its values suddenly look anomalous in the current batch, it gets upgraded to HIGH — catching silent drift.

---

## Full Run Timeline

```mermaid
sequenceDiagram
    participant U as User (main.py)
    participant AV as AdaptiveValidator
    participant RS as RiskScorer
    participant AD as AnomalyDetector
    participant SP as Spark
    participant PG as PostgreSQL

    U->>AV: run_validation(table_config)
    AV->>SP: read_dataset(source)
    AV->>SP: read_dataset(target)
    AV->>AV: validate_ddl()
    AV->>AV: validate_row_counts()
    AV->>AV: get_column_aggregates()

    AV->>RS: compute_risk_scores(table)
    RS->>PG: SELECT from validation_errors
    PG-->>RS: historical error rows
    RS-->>AV: {col: {score, level}}

    AV->>AD: detect(source_df, table)
    AD->>SP: collect column values (limit 10k)
    AD->>AD: load or train IsolationForest
    AD->>AD: re-fit + save .pkl
    AD-->>AV: {col: anomaly_score}

    AV->>AV: classify_columns() → HIGH/MEDIUM/LOW/NEW
    AV->>AV: print_risk_table()

    alt HIGH or NEW columns exist
        AV->>SP: select only high-risk cols
        SP->>AV: filtered DataFrames
        AV->>AV: perform_deep_validation(full)
        AV->>PG: write validation_errors
    end

    alt MEDIUM columns exist
        AV->>SP: sample 10% of medium-risk cols
        SP->>AV: sampled DataFrames
        AV->>AV: perform_deep_validation(sample)
        AV->>PG: write validation_errors
    end

    Note over AV: LOW columns → skipped entirely

    AV->>PG: log_summary() → validation_summary
    AV->>RS: save_risk_scores() → ml_risk_scores
    AV->>RS: save_run_metadata() → ml_run_metadata

    Note over AV,PG: Regenerative learning complete
```

---

## How Learning Compounds Over Runs

```mermaid
xychart-beta
    title "Column Risk Score — salary (repeatedly failing)"
    x-axis ["Run 1", "Run 2", "Run 3", "Run 4", "Run 5"]
    y-axis "Risk Score" 0 --> 1
    line [0.33, 0.56, 0.68, 0.75, 0.79]
```

```mermaid
xychart-beta
    title "Column Risk Score — emp_id (consistently passing)"
    x-axis ["Run 1", "Run 2", "Run 3", "Run 4", "Run 5"]
    y-axis "Risk Score" 0 --> 1
    line [0.0, 0.0, 0.0, 0.0, 0.0]
```

After enough runs:
- `salary` score stabilises at ~0.80 → **permanently HIGH** → always deep-checked
- `emp_id` score stays at 0.0 → **permanently LOW** → deep check skipped entirely
- Validation time reduces proportionally as LOW columns accumulate

---

## Database Tables Created by the ML System

### `ml_risk_scores`
Stores the latest risk score per column. Updated (upserted) after every run.

| Column | Type | Description |
|---|---|---|
| `table_name` | TEXT | Source table being validated |
| `col_name` | TEXT | Column name |
| `risk_score` | FLOAT | Normalised 0–1 score |
| `risk_level` | TEXT | `HIGH`, `MEDIUM`, `LOW` |
| `error_count` | INT | Total historical errors for this column |
| `updated_at` | TIMESTAMP | When this score was last updated |

### `ml_run_metadata`
Tracks model evolution across runs.

| Column | Type | Description |
|---|---|---|
| `run_id` | TEXT | Timestamp-based run identifier |
| `table_name` | TEXT | Table validated in this run |
| `total_cols` | INT | Total columns in table |
| `high_risk_cols` | INT | Columns classified HIGH |
| `med_risk_cols` | INT | Columns classified MEDIUM |
| `low_risk_cols` | INT | Columns classified LOW |
| `model_version` | INT | How many learning iterations the model has seen |
| `run_at` | TIMESTAMP | When the run occurred |

---

## Configuration Reference

```yaml
ml_config:
  enabled: true               # Master on/off switch

  model_dir: "models/"        # .pkl files stored here (gitignored)

  risk_threshold_high: 0.6    # score >= 0.6 → HIGH RISK (full check)
  risk_threshold_low:  0.2    # score <= 0.2 → LOW RISK (skip)
                              # 0.2 < score < 0.6 → MEDIUM (10% sample)

  anomaly_contamination: 0.1  # IsolationForest: expected fraction of anomalies
                              # 0.1 = "expect 10% of values to be outliers"

  recency_decay: 0.85         # How fast old errors lose influence
                              # 0.85^5 ≈ 0.44  (5 runs ago = 44% weight)
                              # 0.85^10 ≈ 0.20 (10 runs ago = 20% weight)

  sample_fraction: 0.1        # MEDIUM risk: validate this fraction of rows
```

**Tuning guide:**

| You want to… | Change this |
|---|---|
| Make model more aggressive (more HIGH) | Lower `risk_threshold_high` (e.g. 0.4) |
| Make model forget old errors faster | Lower `recency_decay` (e.g. 0.7) |
| Detect subtler anomalies | Lower `anomaly_contamination` (e.g. 0.05) |
| Validate more rows in MEDIUM | Raise `sample_fraction` (e.g. 0.25) |
| Disable ML entirely | Set `enabled: false` or use `--adaptive false` |

---

## File Responsibilities

```mermaid
graph TD
    subgraph "src/"
        M["main.py\n──────────\n--adaptive flag\nChooses DataValidator\nor AdaptiveValidator"]
        AV["adaptive_validator.py\n──────────\nOrchestrates everything\nrun_validation() override\nSelective deep validation\nRegenrative learning loop"]
        RS["risk_scorer.py\n──────────\nPostgreSQL history reads\nWeighted decay formula\nSave/load ml_risk_scores\nPretty risk table printer"]
        AD["anomaly_detector.py\n──────────\nExtract column values via Spark\nTrain Isolation Forest\nWarm-start re-training\nSave/load .pkl files"]
        V["validator.py\n──────────\n(parent class)\nread_dataset\nperform_deep_validation\nwrite_result\nlog_summary"]
    end

    subgraph "models/"
        P1["employees_csv__salary_iforest.pkl"]
        P2["employees_csv__emp_id_iforest.pkl"]
        P3["...one per column..."]
    end

    subgraph "PostgreSQL"
        T1["validation_errors\n(error history)"]
        T2["validation_summary\n(run results)"]
        T3["ml_risk_scores\n(learned scores)"]
        T4["ml_run_metadata\n(model versions)"]
    end

    M --> AV
    AV --> RS
    AV --> AD
    AV --> V
    RS --> T1
    RS --> T3
    RS --> T4
    AD --> P1
    AD --> P2
    AD --> P3
    V --> T1
    V --> T2
```
