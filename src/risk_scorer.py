"""
risk_scorer.py
--------------
Reads historical validation_errors from PostgreSQL and computes a weighted
risk score per (table_name, column_name).

Risk Score Formula:
    risk_score = Σ (severity_weight × recency_weight^age) / total_runs
    
    severity_weight:  ValueMismatch=1.0, MissingInTarget=0.8, MissingInSource=0.6
    recency_weight:   configurable decay (default 0.85) — older runs matter less
    age:              how many runs ago the error occurred (0 = most recent)
"""

import logging
import psycopg2
import psycopg2.extras
from datetime import datetime

logger = logging.getLogger(__name__)

SEVERITY_WEIGHTS = {
    "ValueMismatch":   1.0,
    "MissingInTarget": 0.8,
    "MissingInSource": 0.6,
}

GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
RESET  = "\033[0m"
BOLD   = "\033[1m"


class RiskScorer:
    def __init__(self, result_cfg: dict, ml_cfg: dict):
        self.result_cfg   = result_cfg
        self.recency_decay = ml_cfg.get("recency_decay", 0.85)
        self.threshold_high = ml_cfg.get("risk_threshold_high", 0.6)
        self.threshold_low  = ml_cfg.get("risk_threshold_low",  0.2)
        self._conn = None

    # ── Connection ────────────────────────────────────────────────────────────
    def _get_conn(self):
        if self._conn is None or self._conn.closed:
            url  = self.result_cfg["url"]   # jdbc:postgresql://host:port/db
            rest = url.replace("jdbc:postgresql://", "")
            host_port, dbname = rest.split("/", 1)
            host, port = (host_port.split(":", 1) if ":" in host_port
                          else (host_port, "5432"))
            self._conn = psycopg2.connect(
                host=host, port=int(port), dbname=dbname,
                user=self.result_cfg["user"],
                password=self.result_cfg.get("password", ""),
            )
            self._conn.autocommit = True
        return self._conn

    def _ensure_tables(self):
        cur = self._get_conn().cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml_risk_scores (
                id          SERIAL PRIMARY KEY,
                table_name  TEXT,
                col_name    TEXT,
                risk_score  FLOAT,
                risk_level  TEXT,
                error_count INT,
                updated_at  TIMESTAMP DEFAULT NOW(),
                UNIQUE (table_name, col_name)
            );
        """)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS ml_run_metadata (
                id              SERIAL PRIMARY KEY,
                run_id          TEXT,
                table_name      TEXT,
                total_cols      INT,
                high_risk_cols  INT,
                med_risk_cols   INT,
                low_risk_cols   INT,
                model_version   INT,
                run_at          TIMESTAMP DEFAULT NOW()
            );
        """)
        cur.close()

    # ── Load history ──────────────────────────────────────────────────────────
    def load_error_history(self, table_name: str) -> list[dict]:
        """
        Returns all historical errors for the given table, ordered newest first.
        """
        cur = self._get_conn().cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        cur.execute("""
            SELECT col_name, error_type, timestamp
            FROM   validation_errors
            WHERE  table_name = %s
            ORDER  BY timestamp DESC
        """, (table_name,))
        rows = cur.fetchall()
        cur.close()
        return [dict(r) for r in rows]

    # ── Core scoring ──────────────────────────────────────────────────────────
    def compute_risk_scores(self, table_name: str) -> dict[str, dict]:
        """
        Returns {col_name: {"score": float, "level": str, "error_count": int}}
        """
        self._ensure_tables()
        history = self.load_error_history(table_name)

        if not history:
            logger.info(f"[RiskScorer] No history for '{table_name}' — first run")
            return {}

        # Group by unique run timestamps (proxy for run boundaries)
        run_timestamps = sorted({r["timestamp"] for r in history}, reverse=True)
        run_index = {ts: i for i, ts in enumerate(run_timestamps)}

        col_scores: dict[str, float] = {}
        col_counts: dict[str, int]   = {}

        for row in history:
            col  = row["col_name"]
            age  = run_index[row["timestamp"]]   # 0 = most recent run
            sev  = SEVERITY_WEIGHTS.get(row["error_type"], 0.5)
            score_contribution = sev * (self.recency_decay ** age)

            col_scores[col] = col_scores.get(col, 0.0) + score_contribution
            col_counts[col] = col_counts.get(col, 0)   + 1

        # Normalise by total runs so score stays in [0, 1+]
        total_runs = len(run_timestamps) or 1
        result = {}
        for col, raw_score in col_scores.items():
            normalised = min(raw_score / total_runs, 1.0)
            level = (
                "HIGH"   if normalised >= self.threshold_high else
                "MEDIUM" if normalised >= self.threshold_low  else
                "LOW"
            )
            result[col] = {
                "score":       round(normalised, 4),
                "level":       level,
                "error_count": col_counts[col],
            }

        return result

    # ── Persist scores ────────────────────────────────────────────────────────
    def save_risk_scores(self, table_name: str, scores: dict):
        self._ensure_tables()
        cur = self._get_conn().cursor()
        for col, info in scores.items():
            cur.execute("""
                INSERT INTO ml_risk_scores (table_name, col_name, risk_score, risk_level, error_count, updated_at)
                VALUES (%s, %s, %s, %s, %s, NOW())
                ON CONFLICT (table_name, col_name)
                DO UPDATE SET risk_score  = EXCLUDED.risk_score,
                              risk_level  = EXCLUDED.risk_level,
                              error_count = EXCLUDED.error_count,
                              updated_at  = NOW()
            """, (table_name, col, info["score"], info["level"], info["error_count"]))
        cur.close()
        logger.info(f"[RiskScorer] Saved {len(scores)} risk scores for '{table_name}'")

    def save_run_metadata(self, run_id, table_name, col_summary: dict, model_version: int):
        self._ensure_tables()
        cur = self._get_conn().cursor()
        cur.execute("""
            INSERT INTO ml_run_metadata
                (run_id, table_name, total_cols, high_risk_cols, med_risk_cols, low_risk_cols, model_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """, (
            run_id, table_name,
            col_summary.get("total", 0),
            col_summary.get("HIGH",   0),
            col_summary.get("MEDIUM", 0),
            col_summary.get("LOW",    0),
            model_version,
        ))
        cur.close()

    # ── Pretty print ──────────────────────────────────────────────────────────
    def print_risk_table(self, table_name: str, scores: dict, all_cols: list[str]):
        """Print a coloured risk summary table to terminal."""
        level_colour = {"HIGH": RED, "MEDIUM": YELLOW, "LOW": GREEN}
        action_map   = {
            "HIGH":   "Full deep validation",
            "MEDIUM": "10% sample validation",
            "LOW":    "Skip deep check",
        }

        print(f"\n{BOLD}  {'─'*58}{RESET}")
        print(f"{BOLD}  Adaptive Risk Analysis → {table_name}{RESET}")
        print(f"  {'─'*58}")
        print(f"  {'Column':<20} {'Risk':<8} {'Score':<8} {'Action'}")
        print(f"  {'─'*58}")

        for col in all_cols:
            if col in scores:
                info   = scores[col]
                level  = info["level"]
                colour = level_colour[level]
                action = action_map[level]
                print(f"  {col:<20} {colour}{level:<8}{RESET} {info['score']:<8.3f} {action}")
            else:
                print(f"  {col:<20} {GREEN}{'NEW':<8}{RESET} {'—':<8} Full check (no history)")

        print(f"  {'─'*58}\n")

    def close(self):
        if self._conn and not self._conn.closed:
            self._conn.close()
