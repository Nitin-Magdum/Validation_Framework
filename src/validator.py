import logging
import os
import shutil
import glob
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, DoubleType, StringType, LongType, FloatType
from datetime import datetime

# ── ANSI colour helpers ──────────────────────────────────────────────────────
GREEN = "\033[92m"
RESET = "\033[0m"
CHECK = "✔"


def green(msg: str) -> str:
    return f"{GREEN}{msg}{RESET}"


# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DataValidator:
    def __init__(self, spark, config):
        """
        Initialize the Data Validation Framework.
        :param spark: SparkSession object
        :param config: Dictionary containing configuration details
        """
        self.spark = spark
        self.config = config
        self.source_config = config['source_db']
        self.target_config = config['target_db']
        self.result_config = config['result_db']
        self.summary_table = self.result_config.get('summary_table', 'validation_summary')
        self.error_table = self.result_config.get('error_table', 'validation_errors')


    def read_dataset(self, config, schema, table_name):
        """Generic reader for JDBC or File sources (CSV, Parquet, etc)."""
        source_type = config.get("type", "jdbc").lower()
        
        try:
            if source_type == "jdbc":
                full_table_name = f"{schema}.{table_name}"
                return self.spark.read \
                    .format("jdbc") \
                    .option("url", config['url']) \
                    .option("dbtable", full_table_name) \
                    .option("user", config['user']) \
                    .option("password", config['password']) \
                    .option("driver", config['driver']) \
                    .load()
                    
            elif source_type in ["csv", "parquet", "json"]:
                # File based reading
                base_path = config.get("path")
                # If schema is provided in config but irrelevant for files, ignore it or use as folder
                # For file paths, usually schema concept doesn't map directly unless folder structure
                # We assume 'table_name' is the file name or folder
                path_segments = [p for p in [base_path, schema, table_name] if p]
                file_path = os.path.join(*path_segments)
                
                # Simple fix for absolute paths concatenation if needed, or use straightforward string
                # let's rely on config providing base_path and we append table_name
                # If schema is present (e.g. 'public') it might be ignored for CSV unless strictly folder
                
                if config.get("path"):
                     file_path = f"{config.get('path').rstrip('/')}/{table_name}"
                else: 
                     file_path = table_name

                options = config.get("format_options", {})
                
                reader = self.spark.read.format(source_type)
                for k, v in options.items():
                    reader = reader.option(k, v)
                    
                logger.info(f"Reading {source_type} from {file_path}")
                return reader.load(file_path)
                
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
                
        except Exception as e:
            logger.error(f"Error reading dataset {table_name}: {e}")
            raise

    def write_result(self, df, table_name, mode="append"):
        """Generic writer for JDBC or File based results."""
        target_type = self.result_config.get("type", "jdbc").lower()

        try:
            if target_type == "jdbc":
                df.write \
                    .format("jdbc") \
                    .option("url", self.result_config['url']) \
                    .option("dbtable", table_name) \
                    .option("user", self.result_config['user']) \
                    .option("password", self.result_config['password']) \
                    .option("driver", self.result_config['driver']) \
                    .mode(mode) \
                    .save()

                print(green(f"\n  {CHECK}  Results stored successfully → PostgreSQL table '{table_name}'\n"))
                logger.info(f"Results written to PostgreSQL table '{table_name}' (mode={mode})")

            elif target_type in ["csv", "parquet", "json"]:
                base_path = self.result_config.get("path")
                output_path = f"{base_path.rstrip('/')}/{table_name}"

                options = self.result_config.get("format_options", {})

                temp_path = f"{output_path}_temp"
                if os.path.exists(temp_path):
                    shutil.rmtree(temp_path)

                df.coalesce(1).write.format(target_type).mode("overwrite").options(**options).save(temp_path)

                part_files = glob.glob(f"{temp_path}/part-*.{target_type}")
                if not part_files:
                    part_files = glob.glob(f"{temp_path}/part-*")

                if part_files:
                    source_file = part_files[0]
                    final_path = f"{output_path}.{target_type}"
                    os.makedirs(os.path.dirname(final_path), exist_ok=True)
                    if mode == "overwrite" or not os.path.exists(final_path):
                        if os.path.exists(final_path):
                            os.remove(final_path)
                        shutil.move(source_file, final_path)
                    else:
                        with open(final_path, 'a') as f_out, open(source_file, 'r') as f_in:
                            f_in.readline()  # skip header on append
                            shutil.copyfileobj(f_in, f_out)
                    logger.info(f"Results written to {final_path}")
                    shutil.rmtree(temp_path)
                else:
                    logger.warning(f"No part file found in {temp_path}")

            else:
                raise ValueError(f"Unsupported result type: {target_type}")

        except Exception as e:
            logger.error(f"Failed to write results for {table_name}: {e}")
            raise

    def get_column_aggregates(self, df):
        """Calculate dynamic metrics: Avg for numeric, Count Distinct for string."""
        agg_exprs = []
        for field in df.schema.fields:
            col_name = field.name
            dtype = field.dataType
            
            # Count Nulls for every column
            agg_exprs.append(F.sum(F.when(F.col(col_name).isNull(), 1).otherwise(0)).alias(f"null_count_{col_name}"))

            # Type specific metrics
            if isinstance(dtype, (IntegerType, LongType, DoubleType, FloatType)):
                agg_exprs.append(F.avg(F.col(col_name)).alias(f"avg_{col_name}"))
            elif isinstance(dtype, StringType):
                # For string, counting nulls or distinct values might be good. Using length for simplicity as requested "suitable".
                agg_exprs.append(F.avg(F.length(F.col(col_name))).alias(f"avg_len_{col_name}"))
        
        if not agg_exprs:
            return None
            
        return df.agg(*agg_exprs).collect()[0].asDict()

    def validate_ddl(self, source_df, target_df):
        """Compare schema (column names and types)."""
        source_schema = {f.name.lower(): str(f.dataType) for f in source_df.schema.fields}
        target_schema = {f.name.lower(): str(f.dataType) for f in target_df.schema.fields}
        
        if source_schema == target_schema:
            return True, "Schema Match"
        else:
            diff = set(source_schema.items()) ^ set(target_schema.items())
            return False, f"Schema Mismatch: {diff}"

    def validate_row_counts(self, source_df, target_df):
        """Compare row counts."""
        s_count = source_df.count()
        t_count = target_df.count()
        
        if s_count == t_count:
            return True, s_count, t_count
        else:
            return False, s_count, t_count

    def perform_deep_validation(self, source_df, target_df, pk_col, table_name):
        """
        Perform row-by-row comparison using hashing.
        Identifies mismatching rows and missing/extraneous rows.
        """
        # Ensure column order matches for hashing
        common_cols = [c for c in source_df.columns if c in target_df.columns] # Should be all if DDL passed
        
        # Add hash column
        s_hashed = source_df.withColumn("row_hash", F.hash(*[F.col(c) for c in common_cols]))
        t_hashed = target_df.withColumn("row_hash", F.hash(*[F.col(c) for c in common_cols]))
        
        # Compare
        # Join on PK
        joined = s_hashed.alias("s").join(
            t_hashed.alias("t"),
            F.col(f"s.{pk_col}") == F.col(f"t.{pk_col}"),
            "full_outer"
        )
        
        # Check for Mismatches
        # Condition 1: Hash mismatch (but both PKs exist)
        mismatches = joined.filter(
            (F.col(f"s.{pk_col}").isNotNull()) & 
            (F.col(f"t.{pk_col}").isNotNull()) & 
            (F.col("s.row_hash") != F.col("t.row_hash"))
        )
        
        # Condition 2: Missing in Target
        missing_target = joined.filter(F.col(f"t.{pk_col}").isNull())
        
        # Condition 3: Missing in Source (Extra in Target)
        missing_source = joined.filter(F.col(f"s.{pk_col}").isNull())
        
        error_rows = []
        timestamp = datetime.now()

        # Helper to construct error rows
        def generate_error_rows(df_errors, error_type):
            if df_errors.count() > 0:
                # We need to collect or process these. For large data, writing directly is better.
                # However, mapping to specific columns requires looping or stack.
                # For "mismatches", we need to find WHICH column.
                # This could be expensive. Limit checking or do it efficiently?
                # User asked: "column name... store in one pg sql table"
                
                # To find specific column mismatch efficiently in Spark:
                # We can use a map or complex expression.
                pass
            return df_errors.count()

        # Let's count them first
        mismatch_count = mismatches.count()
        missing_target_count = missing_target.count()
        missing_source_count = missing_source.count()
        
        total_errors = mismatch_count + missing_target_count + missing_source_count
        
        if total_errors == 0:
            return True, "Data Matched Perfectly"

        # If errors exist, log detailed usage to postgres
        # Strategy: Normalize the data to write to Postgres.
        # We'll use a specific schema for logging errors:
        # Table Name, PK Value, Column Name, Source Value, Target Value, Error Type, Timestamp
        

        
        all_errors = []

        # 1. Missing in Target (easy)
        if missing_target_count > 0:
            log_df = missing_target.select(
                F.lit(table_name).alias("table_name"),
                F.col(f"s.{pk_col}").cast(StringType()).alias("pk_value"),
                F.lit("RELATION").alias("col_name"), # Entire row missing
                F.lit("Exists").alias("source_value"),
                F.lit("Missing").alias("target_value"),
                F.lit("MissingInTarget").alias("error_type"),
                F.lit(timestamp).alias("timestamp")
            )
            all_errors.append(log_df)

        # 2. Missing in Source (easy)
        if missing_source_count > 0:
            log_df = missing_source.select(
                F.lit(table_name).alias("table_name"),
                F.col(f"t.{pk_col}").cast(StringType()).alias("pk_value"),
                F.lit("RELATION").alias("col_name"), 
                F.lit("Missing").alias("source_value"),
                F.lit("Exists").alias("target_value"),
                F.lit("MissingInSource").alias("error_type"),
                F.lit(timestamp).alias("timestamp")
            )
            all_errors.append(log_df)

        # 3. Mismatches (harder - need to find column)
        if mismatch_count > 0:
            # Compare/Pivot Logic
            mm_rows_s = mismatches.select(F.col(f"s.{pk_col}").alias("pk"), *[F.col(f"s.{c}").alias(c) for c in common_cols if c != pk_col])
            mm_rows_t = mismatches.select(F.col(f"t.{pk_col}").alias("pk"), *[F.col(f"t.{c}").alias(c) for c in common_cols if c != pk_col])
            
            # Melting/Unpivoting
            def unpivot(df, id_col):
                cols = [c for c in df.columns if c != id_col]
                # Cast to string to avoid type mismatch in stack
                stack_expr = ", ".join([f"'{c}', CAST(`{c}` AS STRING)" for c in cols])
                return df.select(
                    id_col,
                    F.expr(f"stack({len(cols)}, {stack_expr}) as (col_name, val)")
                )
            
            s_long = unpivot(mm_rows_s, "pk")
            t_long = unpivot(mm_rows_t, "pk")
            
            diffs = s_long.alias("sl").join(
                t_long.alias("tl"),
                (F.col("sl.pk") == F.col("tl.pk")) & (F.col("sl.col_name") == F.col("tl.col_name")),
                "inner"
            ).filter(
                (F.col("sl.val") != F.col("tl.val")) |
                (F.col("sl.val").isNull() & F.col("tl.val").isNotNull()) |
                (F.col("sl.val").isNotNull() & F.col("tl.val").isNull())
            ).select(
                F.lit(table_name).alias("table_name"),
                F.col("sl.pk").cast(StringType()).alias("pk_value"),
                F.col("sl.col_name"),
                F.col("sl.val").alias("source_value"),
                F.col("tl.val").alias("target_value"),
                F.lit("ValueMismatch").alias("error_type"),
                F.lit(timestamp).alias("timestamp")
            )
            all_errors.append(diffs)

        # Combine all errors and write once
        if all_errors:
            final_error_df = all_errors[0]
            for i in range(1, len(all_errors)):
                final_error_df = final_error_df.union(all_errors[i])
            
            self.write_result(final_error_df, self.error_table)
            
        return False, f"Found Errors: MissingTarget={missing_target_count}, MissingSource={missing_source_count}, Modified={mismatch_count}"

    def run_validation(self, table_config):
        """
        Execute validation pipeline for a single table.

        table_config supports two formats:
          Legacy  : { schema, table_name, primary_key }
          New     : { primary_key,
                      source: { table_name, schema, db: {...} },
                      target: { table_name, schema, db: {...} } }

        The optional 'db' block inside source/target overrides the global
        source_db / target_db for that specific table.
        """
        pk = table_config['primary_key']
        run_id = datetime.now().strftime("%Y%m%d%H%M%S")

        # ── Resolve source config ──────────────────────────────────────────
        if 'source' in table_config:
            src_block   = table_config['source']
            src_schema  = src_block.get('schema', '')
            src_table   = src_block['table_name']
            src_db_cfg  = src_block.get('db', self.source_config)   # override or default
        else:
            # Legacy format
            src_schema  = table_config.get('schema', '')
            src_table   = table_config['table_name']
            src_db_cfg  = self.source_config

        # ── Resolve target config ──────────────────────────────────────────
        if 'target' in table_config:
            tgt_block   = table_config['target']
            tgt_schema  = tgt_block.get('schema', '')
            tgt_table   = tgt_block['table_name']
            tgt_db_cfg  = tgt_block.get('db', self.target_config)   # override or default
        else:
            tgt_schema  = table_config.get('schema', '')
            tgt_table   = table_config['table_name']
            tgt_db_cfg  = self.target_config

        # Display label for logs (source → target)
        label = f"{src_schema}.{src_table}" if src_schema else src_table
        if src_table != tgt_table:
            tgt_label = f"{tgt_schema}.{tgt_table}" if tgt_schema else tgt_table
            label = f"{label} → {tgt_label}"

        logger.info(f"--- Starting Validation for {label} ---")

        try:
            # 1. Read Data
            logger.info("Reading Source and Target data...")
            source_df = self.read_dataset(src_db_cfg, src_schema, src_table)
            target_df = self.read_dataset(tgt_db_cfg, tgt_schema, tgt_table)

            # 2. DDL Check
            logger.info("Validating DDL...")
            ddl_match, ddl_msg = self.validate_ddl(source_df, target_df)
            if not ddl_match:
                logger.error(f"DDL Mismatch: {ddl_msg}")
                self.log_summary(run_id, label, "DDL_FAIL", ddl_msg)
                return

            # 3. Row Count Check
            logger.info("Validating Row Counts...")
            cnt_match, s_cnt, t_cnt = self.validate_row_counts(source_df, target_df)
            if not cnt_match:
                logger.error(f"Count Mismatch: Source={s_cnt}, Target={t_cnt}")
                self.log_summary(run_id, label, "COUNT_FAIL", f"S:{s_cnt}, T:{t_cnt}", s_cnt, t_cnt)
                return

            # 4. Aggregates Check
            logger.info("Validating Aggregates...")
            s_aggs = self.get_column_aggregates(source_df)
            t_aggs = self.get_column_aggregates(target_df)

            if str(s_aggs) != str(t_aggs):
                logger.warning(f"Aggregate Mismatch: \nS: {s_aggs}\nT: {t_aggs}")

            # 5. Deep Row-by-Row Validation
            logger.info("Performing Deep Row-by-Row Validation...")
            data_match, data_msg = self.perform_deep_validation(source_df, target_df, pk, src_table)

            status = "SUCCESS" if data_match else "DATA_MISMATCH"
            self.log_summary(run_id, label, status, data_msg, s_cnt, t_cnt, str(s_aggs))

            logger.info(f"Validation Finished for {label}: {status}")

        except Exception as e:
            logger.error(f"Validation Exception for {label}: {e}")
            self.log_summary(run_id, label, "ERROR", str(e))


    def log_summary(self, run_id, table, status, message, s_cnt=0, t_cnt=0, metrics=""):
        """Log summary to target."""
        try:
            data = [{
                "run_id": run_id,
                "table_name": table,
                "status": status,
                "message": message,
                "source_count": s_cnt,
                "target_count": t_cnt,
                "metrics": str(metrics), # Ensure string
                "timestamp": str(datetime.now()) # Ensure string
            }]
            # Define schema explicitly to avoid inference issues or use string conversion
            df = self.spark.createDataFrame(data)
            logger.info(f"Logging summary for {table}: {status}")
            self.write_result(df, self.summary_table)
        except Exception as e:
            logger.error(f"Failed to log summary: {e}")

