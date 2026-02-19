"""
anomaly_detector.py
-------------------
Uses scikit-learn's Isolation Forest to detect distributional anomalies in
source DataFrame columns.

Per-column model lifecycle:
    1. First run  → train Isolation Forest on column values → save .pkl
    2. Later runs → load .pkl → retrain with new data (warm regeneration)
    3. Returns anomaly_scores dict: {col_name: float}  (-1.0 = most anomalous)

Supports pre-trained models: if a .pkl file already exists in model_dir,
it is loaded and updated rather than trained from scratch.
"""

import os
import logging
import joblib
import numpy as np
from sklearn.ensemble import IsolationForest

logger = logging.getLogger(__name__)


class AnomalyDetector:
    def __init__(self, ml_cfg: dict):
        self.model_dir     = ml_cfg.get("model_dir", "models/")
        self.contamination = ml_cfg.get("anomaly_contamination", 0.1)
        os.makedirs(self.model_dir, exist_ok=True)

    # ── Utilities ─────────────────────────────────────────────────────────────
    def _model_path(self, table_name: str, col_name: str) -> str:
        safe_table = table_name.replace(".", "_").replace("/", "_")
        safe_col   = col_name.replace(" ", "_")
        return os.path.join(self.model_dir, f"{safe_table}__{safe_col}_iforest.pkl")

    def _load_model(self, path: str) -> IsolationForest | None:
        if os.path.exists(path):
            try:
                model = joblib.load(path)
                logger.info(f"[AnomalyDetector] Loaded pre-trained model: {path}")
                return model
            except Exception as e:
                logger.warning(f"[AnomalyDetector] Could not load model {path}: {e}")
        return None

    def _save_model(self, model: IsolationForest, path: str):
        joblib.dump(model, path)
        logger.info(f"[AnomalyDetector] Saved model: {path}")

    # ── Extract column values from Spark DF ───────────────────────────────────
    def _extract_values(self, df, col_name: str) -> np.ndarray | None:
        """
        Collects column values from Spark DataFrame.
        Numeric → raw values; String → string lengths.
        Returns None if column is empty or all-null.
        """
        from pyspark.sql import functions as F
        from pyspark.sql.types import (IntegerType, LongType,
                                       DoubleType, FloatType, StringType)

        field = next((f for f in df.schema.fields if f.name == col_name), None)
        if field is None:
            return None

        dtype = field.dataType
        if isinstance(dtype, (IntegerType, LongType, DoubleType, FloatType)):
            vals = (df.select(F.col(col_name).cast("double"))
                      .na.drop()
                      .limit(10_000)         # cap for performance
                      .toPandas()[col_name]
                      .values)
        elif isinstance(dtype, StringType):
            vals = (df.select(F.length(F.col(col_name)).alias("len"))
                      .na.drop()
                      .limit(10_000)
                      .toPandas()["len"]
                      .values.astype(float))
        else:
            return None

        return vals.reshape(-1, 1) if len(vals) > 0 else None

    # ── Core detection ────────────────────────────────────────────────────────
    def detect(self, source_df, table_name: str) -> dict[str, float]:
        """
        Runs Isolation Forest on each column of source_df.
        Returns {col_name: mean_anomaly_score} — lower is more anomalous.
        Scores < -0.05 are considered suspicious.
        """
        results: dict[str, float] = {}

        for field in source_df.schema.fields:
            col = field.name
            path = self._model_path(table_name, col)

            values = self._extract_values(source_df, col)
            if values is None or len(values) < 5:
                logger.debug(f"[AnomalyDetector] Skipping '{col}' — insufficient data")
                continue

            # Load existing or train fresh
            model = self._load_model(path)
            if model is not None:
                # Regenerative: retrain with warm start using combined data
                model.set_params(warm_start=True, n_estimators=model.n_estimators + 10)
                try:
                    model.fit(values)
                except Exception:
                    # Fallback: retrain fresh if warm start fails
                    model = IsolationForest(
                        contamination=self.contamination,
                        random_state=42,
                        n_estimators=100,
                    )
                    model.fit(values)
            else:
                model = IsolationForest(
                    contamination=self.contamination,
                    random_state=42,
                    n_estimators=100,
                )
                model.fit(values)

            # Score: decision_function returns higher = more normal
            scores = model.decision_function(values)
            mean_score = float(np.mean(scores))
            results[col] = round(mean_score, 4)

            self._save_model(model, path)
            logger.info(f"[AnomalyDetector] '{col}' anomaly score: {mean_score:.4f}")

        return results

    def is_anomalous(self, score: float, threshold: float = -0.05) -> bool:
        """Returns True if the anomaly score is below the suspicious threshold."""
        return score < threshold
