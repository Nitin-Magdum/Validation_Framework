import argparse
import logging
import sys
import os
import psycopg2
import yaml
from pyspark.sql import SparkSession
from validator import DataValidator
from adaptive_validator import AdaptiveValidator

# ── ANSI colour helpers ──────────────────────────────────────────────────────
GREEN = "\033[92m"
RESET = "\033[0m"
CHECK = "✔"


def green(msg):
    return f"{GREEN}{msg}{RESET}"


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def load_config(config_path):
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)


def check_postgres_connectivity(result_cfg):
    """
    Verify PostgreSQL connectivity and auto-create result tables if missing.
    Prints green ✔ on success, raises on failure.
    """
    url  = result_cfg.get("url", "")
    user = result_cfg.get("user", "postgres")
    pwd  = result_cfg.get("password", "")
    summary_table = result_cfg.get("summary_table", "validation_summary")
    error_table   = result_cfg.get("error_table",   "validation_errors")

    # Parse host / port / dbname from JDBC URL
    jdbc_prefix = "jdbc:postgresql://"
    if url.startswith(jdbc_prefix):
        rest = url[len(jdbc_prefix):]
        host_port, dbname = rest.split("/", 1)
        host, port = (host_port.split(":", 1) if ":" in host_port
                      else (host_port, "5432"))
        port = int(port)
    else:
        host, port, dbname = "localhost", 5432, "postgres"

    print(f"\n{'─'*55}")
    print(f"  Checking PostgreSQL connection …")
    print(f"  Host : {host}:{port}  DB : {dbname}  User : {user}")
    print(f"{'─'*55}")

    conn = psycopg2.connect(host=host, port=port, dbname=dbname,
                            user=user, password=pwd, connect_timeout=5)
    conn.autocommit = True
    cur = conn.cursor()

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {summary_table} (
            id            SERIAL PRIMARY KEY,
            run_id        TEXT,
            table_name    TEXT,
            status        TEXT,
            message       TEXT,
            source_count  BIGINT,
            target_count  BIGINT,
            metrics       TEXT,
            timestamp     TEXT
        );
    """)

    cur.execute(f"""
        CREATE TABLE IF NOT EXISTS {error_table} (
            id           SERIAL PRIMARY KEY,
            table_name   TEXT,
            pk_value     TEXT,
            col_name     TEXT,
            source_value TEXT,
            target_value TEXT,
            error_type   TEXT,
            timestamp    TEXT
        );
    """)

    cur.close()
    conn.close()

    print(green(f"\n  {CHECK}  PostgreSQL connection successful!"))
    print(green(f"  {CHECK}  Tables '{summary_table}' and '{error_table}' are ready.\n"))


def main():
    parser = argparse.ArgumentParser(description="Data Validation Framework")
    parser.add_argument("--config", default="config/config.yaml",
                        help="Path to YAML config file (default: config/config.yaml)")
    parser.add_argument("--adaptive", default="true",
                        help="Enable adaptive ML validation (default: true). Use 'false' to disable.")
    args = parser.parse_args()
    use_adaptive = args.adaptive.lower() not in ("false", "0", "no")
    config_path = args.config

    if not os.path.exists(config_path):
        logger.error(f"Config file not found: {config_path}")
        sys.exit(1)

    sys.path.append(os.path.dirname(os.path.abspath(__file__)))

    if 'PYSPARK_PYTHON' not in os.environ:
        os.environ['PYSPARK_PYTHON'] = sys.executable
    if 'PYSPARK_DRIVER_PYTHON' not in os.environ:
        os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

    config = load_config(config_path)

    # ── Step 1: Verify PostgreSQL result DB connectivity ──────────────────────
    result_cfg = config.get("result_db", {})
    if result_cfg.get("type", "").lower() == "jdbc":
        try:
            check_postgres_connectivity(result_cfg)
        except Exception as e:
            print(f"\n  ✘  PostgreSQL connection FAILED: {e}\n")
            logger.error(f"Cannot connect to result PostgreSQL database: {e}")
            sys.exit(1)

    # ── Step 2: Initialize Spark ──────────────────────────────────────────────
    spark_conf = config.get("spark_config", {})
    builder = SparkSession.builder.appName(config.get("app_name", "DataValidator"))
    for key, value in spark_conf.items():
        builder = builder.config(key, value)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Spark Session Created: {spark.version}")

    # ── Step 3: Run Validation ────────────────────────────────────────────────
    try:
        if use_adaptive and config.get("ml_config", {}).get("enabled", False):
            logger.info("Using AdaptiveValidator (ML mode ON)")
            validator = AdaptiveValidator(spark, config)
        else:
            logger.info("Using standard DataValidator (ML mode OFF)")
            validator = DataValidator(spark, config)
        tables = config.get("tables_to_validate", [])
        if not tables:
            logger.warning("No tables found in config to validate.")
            return

        for table_config in tables:
            logger.info(f"Processing: {table_config}")
            validator.run_validation(table_config)

    except Exception as e:
        logger.error(f"Critical Application Error: {e}")
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark Session Stopped")


if __name__ == "__main__":
    main()
