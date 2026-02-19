"""
adaptive_validator.py
---------------------
Extends DataValidator with adaptive ML-driven validation.

Decision logic per column:
    HIGH RISK   → Full deep row-by-row validation
    MEDIUM RISK → 10% sample-based validation
    LOW RISK    → Skip deep validation (DDL + count checks only)
    NEW         → Full validation (conservative first run, no history)

After each run, risk scores and Isolation Forest models are updated
so the next run benefits from the accumulated learning (regenerative).
"""

import logging
import os
import sys
from datetime import datetime

sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from validator import DataValidator
from risk_scorer import RiskScorer
from anomaly_detector import AnomalyDetector

logger = logging.getLogger(__name__)

RESET  = "\033[0m"
BOLD   = "\033[1m"
GREEN  = "\033[92m"
YELLOW = "\033[93m"
RED    = "\033[91m"
CYAN   = "\033[96m"
CHECK  = "✔"


class AdaptiveValidator(DataValidator):
    """
    ML-powered validator that learns from every historical run.
    Falls back to full validation gracefully on first run.
    """

    def __init__(self, spark, config: dict):
        super().__init__(spark, config)
        self.ml_cfg    = config.get("ml_config", {})
        self.enabled   = self.ml_cfg.get("enabled", True)
        result_cfg     = config.get("result_db", {})
        self.scorer    = RiskScorer(result_cfg, self.ml_cfg)
        self.detector  = AnomalyDetector(self.ml_cfg)
        self.sample_f  = self.ml_cfg.get("sample_fraction", 0.1)

    # ── Helpers ───────────────────────────────────────────────────────────────
    def _col_list(self, df) -> list[str]:
        return [f.name for f in df.schema.fields]

    def _classify_columns(
        self,
        risk_scores: dict,
        anomaly_scores: dict,
        all_cols: list[str],
    ) -> dict[str, str]:
        """
        Combines risk scorer + anomaly detector output into a final
        risk classification per column: HIGH / MEDIUM / LOW / NEW
        """
        anomaly_thresh = -0.05

        classification = {}
        for col in all_cols:
            risk_info     = risk_scores.get(col)
            anomaly_score = anomaly_scores.get(col, 0.0)
            is_anomalous  = self.detector.is_anomalous(anomaly_score, anomaly_thresh)

            if risk_info is None:
                # No historical data → always full check (conservative)
                classification[col] = "NEW"
            else:
                level = risk_info["level"]
                # Anomaly detector can upgrade a column's risk level
                if is_anomalous and level in ("LOW", "MEDIUM"):
                    logger.info(
                        f"[AdaptiveValidator] '{col}' upgraded to HIGH "
                        f"(anomaly score={anomaly_score:.4f})"
                    )
                    level = "HIGH"
                classification[col] = level

        return classification

    def _print_summary_banner(self, table_name: str, classification: dict):
        action_map = {
            "HIGH":   "Full deep validation",
            "MEDIUM": f"{int(self.sample_f * 100)}% sample validation",
            "LOW":    "Skip deep check",
            "NEW":    "Full check (first run)",
        }
        colour_map = {"HIGH": RED, "MEDIUM": YELLOW, "LOW": GREEN, "NEW": CYAN}

        print(f"\n{BOLD}  {'─'*64}{RESET}")
        print(f"{BOLD}  🤖  Adaptive ML Analysis → {table_name}{RESET}")
        print(f"  {'─'*64}")
        print(f"  {'Column':<22} {'Risk':<10} {'Action'}")
        print(f"  {'─'*64}")
        for col, level in classification.items():
            c = colour_map.get(level, RESET)
            print(f"  {col:<22} {c}{level:<10}{RESET} {action_map.get(level, '')}")
        print(f"  {'─'*64}\n")

    # ── Core override ─────────────────────────────────────────────────────────
    def run_validation(self, table_config: dict):
        """
        ML-driven entry point — replaces DataValidator.run_validation().
        """
        pk     = table_config["primary_key"]
        run_id = datetime.now().strftime("%Y%m%d%H%M%S")

        # ── Resolve source / target config (same logic as parent) ─────────────
        if "source" in table_config:
            src_block  = table_config["source"]
            src_schema = src_block.get("schema", "")
            src_table  = src_block["table_name"]
            src_db_cfg = src_block.get("db", self.source_config)
        else:
            src_schema = table_config.get("schema", "")
            src_table  = table_config["table_name"]
            src_db_cfg = self.source_config

        if "target" in table_config:
            tgt_block  = table_config["target"]
            tgt_schema = tgt_block.get("schema", "")
            tgt_table  = tgt_block["table_name"]
            tgt_db_cfg = tgt_block.get("db", self.target_config)
        else:
            tgt_schema = table_config.get("schema", "")
            tgt_table  = table_config["table_name"]
            tgt_db_cfg = self.target_config

        label = f"{src_schema}.{src_table}" if src_schema else src_table
        if src_table != tgt_table:
            tgt_label = f"{tgt_schema}.{tgt_table}" if tgt_schema else tgt_table
            label = f"{label} → {tgt_label}"

        logger.info(f"--- [Adaptive] Starting Validation for {label} ---")

        try:
            # ── 1. Read data ──────────────────────────────────────────────────
            source_df = self.read_dataset(src_db_cfg, src_schema, src_table)
            target_df = self.read_dataset(tgt_db_cfg, tgt_schema, tgt_table)

            all_cols = self._col_list(source_df)

            # ── 2. DDL Check (always) ─────────────────────────────────────────
            logger.info("Validating DDL...")
            ddl_ok, ddl_msg = self.validate_ddl(source_df, target_df)
            if not ddl_ok:
                self.log_summary(run_id, label, "DDL_FAIL", ddl_msg)
                return

            # ── 3. Row Count Check (always) ───────────────────────────────────
            logger.info("Validating Row Counts...")
            cnt_ok, s_cnt, t_cnt = self.validate_row_counts(source_df, target_df)
            if not cnt_ok:
                self.log_summary(run_id, label, "COUNT_FAIL",
                                 f"S:{s_cnt}, T:{t_cnt}", s_cnt, t_cnt)
                return

            # ── 4. Aggregate Check ────────────────────────────────────────────
            logger.info("Validating Aggregates...")
            s_aggs = self.get_column_aggregates(source_df)
            t_aggs = self.get_column_aggregates(target_df)
            if str(s_aggs) != str(t_aggs):
                logger.warning(f"Aggregate Mismatch:\nS: {s_aggs}\nT: {t_aggs}")

            if not self.enabled:
                # Fall back gracefully to standard validation
                logger.info("[AdaptiveValidator] ML disabled → running full validation")
                ok, msg = self.perform_deep_validation(
                    source_df, target_df, pk, src_table
                )
                status = "SUCCESS" if ok else "DATA_MISMATCH"
                self.log_summary(run_id, label, status, msg, s_cnt, t_cnt, str(s_aggs))
                return

            # ── 5. Risk Scoring ───────────────────────────────────────────────
            logger.info("[AdaptiveValidator] Computing risk scores from history...")
            risk_scores = self.scorer.compute_risk_scores(src_table)

            # ── 6. Anomaly Detection (Isolation Forest) ───────────────────────
            logger.info("[AdaptiveValidator] Running Isolation Forest on columns...")
            anomaly_scores = self.detector.detect(source_df, src_table)

            # ── 7. Classify columns ───────────────────────────────────────────
            classification = self._classify_columns(
                risk_scores, anomaly_scores, all_cols
            )
            self._print_summary_banner(label, classification)

            # ── 8. Selective Deep Validation ──────────────────────────────────
            high_new_cols = [c for c, l in classification.items()
                             if l in ("HIGH", "NEW")]
            medium_cols   = [c for c, l in classification.items()
                             if l == "MEDIUM"]
            low_cols      = [c for c, l in classification.items()
                             if l == "LOW"]

            if low_cols:
                logger.info(f"[AdaptiveValidator] Skipping deep check for "
                            f"LOW-risk cols: {low_cols}")

            overall_ok  = True
            overall_msg = []

            # Full deep check on HIGH / NEW columns
            if high_new_cols:
                check_cols = list(dict.fromkeys([pk] + high_new_cols))
                ok, msg = self.perform_deep_validation(
                    source_df.select(*check_cols),
                    target_df.select(*check_cols),
                    pk, src_table
                )
                if not ok:
                    overall_ok = False
                    overall_msg.append(f"HIGH/NEW cols: {msg}")

            # Sample-based check on MEDIUM columns
            if medium_cols:
                check_cols = list(dict.fromkeys([pk] + medium_cols))
                src_sample = source_df.select(*check_cols).sample(
                    withReplacement=False,
                    fraction=self.sample_f,
                    seed=42
                )
                tgt_sample = target_df.select(*check_cols).sample(
                    withReplacement=False,
                    fraction=self.sample_f,
                    seed=42
                )
                ok, msg = self.perform_deep_validation(
                    src_sample, tgt_sample,
                    pk, f"{src_table}[SAMPLE]"
                )
                if not ok:
                    overall_ok = False
                    overall_msg.append(f"MEDIUM cols (sampled): {msg}")

            # ── 9. Log summary ────────────────────────────────────────────────
            status  = "SUCCESS" if overall_ok else "DATA_MISMATCH"
            message = (" | ".join(overall_msg) if overall_msg
                       else "All validated columns match")

            low_note = (f" ({len(low_cols)} LOW-risk cols skipped)"
                        if low_cols else "")
            self.log_summary(run_id, label, status,
                             message + low_note, s_cnt, t_cnt, str(s_aggs))

            # ── 10. Regenerative Learning ─────────────────────────────────────
            logger.info("[AdaptiveValidator] Updating risk scores (regenerative)...")
            updated_scores = self.scorer.compute_risk_scores(src_table)
            self.scorer.save_risk_scores(src_table, updated_scores)

            col_summary = {
                "total":  len(all_cols),
                "HIGH":   sum(1 for l in classification.values() if l == "HIGH"),
                "MEDIUM": sum(1 for l in classification.values() if l == "MEDIUM"),
                "LOW":    sum(1 for l in classification.values() if l == "LOW"),
            }
            # model_version = number of runs stored in ml_risk_scores for this table
            model_version = len(updated_scores) + 1
            self.scorer.save_run_metadata(
                run_id, src_table, col_summary, model_version
            )

            print(f"\n{GREEN}{BOLD}  {CHECK}  Adaptive validation complete "
                  f"→ {status}{RESET}\n")
            logger.info(f"[Adaptive] Finished for {label}: {status}")

        except Exception as e:
            logger.error(f"[AdaptiveValidator] Exception for {label}: {e}")
            self.log_summary(run_id, label, "ERROR", str(e))
