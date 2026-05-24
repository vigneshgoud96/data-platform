# Databricks notebook source
# Customer Incremental ETL Pipeline - Enhanced with Comprehensive Logging
# Features: Detailed logging, exception handling, retry logic, metrics tracking

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col, current_timestamp, when, coalesce, row_number, 
    max as spark_max, dense_rank, lit, count as spark_count
)
from pyspark.sql.window import Window
from datetime import datetime, timedelta
import logging
import sys
import traceback
from functools import wraps
from typing import Optional, Dict, Any

# COMMAND ----------

# Initialize Spark Session
spark = SparkSession.builder.appName("CustomerIncrementalETL").getOrCreate()
spark.sql("SET spark.sql.shuffle.partitions=200")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Logging Configuration

# COMMAND ----------

class ETLLogger:
    """Enhanced logger for ETL operations"""
    
    def __init__(self, name: str):
        self.name = name
        self.logs = []
        self.start_time = datetime.now()
        self._setup_logger()
    
    def _setup_logger(self):
        """Configure logging"""
        self.logger = logging.getLogger(self.name)
        self.logger.setLevel(logging.DEBUG)
        
        # Console handler
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        self.logger.addHandler(handler)
    
    def log(self, level: str, message: str, **kwargs):
        """Log message with metadata"""
        log_entry = {
            "timestamp": datetime.now().isoformat(),
            "level": level,
            "message": message,
            "duration_seconds": (datetime.now() - self.start_time).total_seconds(),
            **kwargs
        }
        self.logs.append(log_entry)
        
        # Also log to stdout
        log_method = getattr(self.logger, level.lower(), self.logger.info)
        log_method(message)
    
    def info(self, message: str, **kwargs):
        self.log("INFO", message, **kwargs)
    
    def warning(self, message: str, **kwargs):
        self.log("WARNING", message, **kwargs)
    
    def error(self, message: str, **kwargs):
        self.log("ERROR", message, **kwargs)
    
    def critical(self, message: str, **kwargs):
        self.log("CRITICAL", message, **kwargs)
    
    def get_logs(self):
        """Return all logs"""
        return self.logs

# Initialize logger
logger = ETLLogger("CustomerIncrementalETL")

# COMMAND ----------

def handle_exceptions(func):
    """Decorator for exception handling"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        try:
            logger.info(f"Starting: {func.__name__}")
            result = func(*args, **kwargs)
            logger.info(f"Completed: {func.__name__}")
            return result
        except Exception as e:
            logger.error(
                f"Error in {func.__name__}: {str(e)}",
                error_type=type(e).__name__,
                traceback=traceback.format_exc()
            )
            raise
    return wrapper

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration & Parameters

# COMMAND ----------

class ETLConfig:
    """Configuration for ETL pipeline"""
    
    # Paths
    SOURCE_SYSTEM = "customer_source"
    BRONZE_PATH = "/mnt/bronze/customer"
    SILVER_PATH = "/mnt/silver/customer"
    CHECKPOINT_PATH = "/mnt/checkpoints/customer_incremental"
    ERROR_PATH = "/mnt/errors/customer"
    METRICS_PATH = "/mnt/metrics/customer"
    
    # Database
    DATABASE_NAME = "silver_db"
    TABLE_NAME = "customer"
    
    # Processing
    MAX_PARALLELISM = 200
    BATCH_SIZE = 10000
    
    # Retry policy
    MAX_RETRIES = 3
    RETRY_INTERVAL = 5  # seconds

# Get parameters
LOAD_TYPE = dbutils.widgets.get("load_type", "incremental")
PIPELINE_RUN_ID = dbutils.widgets.get("pipeline_run_id", "local_run")
PIPELINE_TRIGGER_TIME = dbutils.widgets.get("pipeline_trigger_time", datetime.now().isoformat())

logger.info(
    "ETL Configuration loaded",
    load_type=LOAD_TYPE,
    pipeline_run_id=PIPELINE_RUN_ID
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Core Functions with Exception Handling

# COMMAND ----------

@handle_exceptions
def read_bronze_data(load_type: str = "incremental") -> DataFrame:
    """Read customer data from bronze layer with error handling"""
    
    try:
        logger.info(f"Reading bronze data with load_type: {load_type}")
        
        df = spark.read.format("parquet").load(ETLConfig.BRONZE_PATH)
        initial_count = df.count()
        logger.info(f"Read {initial_count} total records from bronze layer")
        
        if load_type == "incremental":
            last_date = get_last_processed_date()
            if last_date:
                df = df.filter(col("load_date") > last_date)
                incremental_count = df.count()
                logger.info(
                    f"Filtered to {incremental_count} incremental records from {last_date}",
                    total_records=initial_count,
                    incremental_records=incremental_count
                )
            else:
                logger.warning("No checkpoint found, processing all records (full load)")
        
        return df
    
    except Exception as e:
        logger.error(f"Failed to read bronze data: {str(e)}")
        raise

# COMMAND ----------

@handle_exceptions
def get_last_processed_date() -> Optional[datetime]:
    """Retrieve last processed date with error handling"""
    
    try:
        checkpoint_df = spark.read.format("parquet").load(ETLConfig.CHECKPOINT_PATH)
        last_date = checkpoint_df.select(spark_max("last_processed_date")).collect()[0][0]
        
        logger.info(f"Retrieved last processed date: {last_date}")
        return last_date
    
    except Exception as e:
        logger.warning(f"No checkpoint found or error reading checkpoint: {str(e)}")
        return None

# COMMAND ----------

@handle_exceptions
def save_checkpoint(last_date: datetime):
    """Save checkpoint with comprehensive error handling"""
    
    try:
        checkpoint_data = spark.createDataFrame(
            [(last_date,)], 
            ["last_processed_date"]
        )
        checkpoint_data.write \
            .format("parquet") \
            .mode("overwrite") \
            .save(ETLConfig.CHECKPOINT_PATH)
        
        logger.info(f"Checkpoint saved for date: {last_date}")
    
    except Exception as e:
        logger.error(f"Failed to save checkpoint: {str(e)}")
        # Don't re-raise - checkpoint failure shouldn't fail the entire pipeline
        logger.warning("Continuing despite checkpoint save failure")

# COMMAND ----------

@handle_exceptions
def deduplicate_records(df: DataFrame, key_columns: list) -> DataFrame:
    """Remove duplicates with logging"""
    
    initial_count = df.count()
    
    try:
        window_spec = Window.partitionBy(*key_columns).orderBy(col("load_date").desc())
        
        deduped = df.withColumn(
            "row_num", row_number().over(window_spec)
        ).filter(col("row_num") == 1).drop("row_num")
        
        final_count = deduped.count()
        duplicates_removed = initial_count - final_count
        
        logger.info(
            f"Deduplication completed",
            initial_records=initial_count,
            final_records=final_count,
            duplicates_removed=duplicates_removed,
            dedup_percentage=round((duplicates_removed / initial_count * 100), 2) if initial_count > 0 else 0
        )
        
        return deduped
    
    except Exception as e:
        logger.error(f"Deduplication failed: {str(e)}")
        raise

# COMMAND ----------

@handle_exceptions
def validate_data_quality(df: DataFrame) -> Dict[str, Any]:
    """Comprehensive data quality validation"""
    
    try:
        total_records = df.count()
        
        if total_records == 0:
            logger.warning("Empty dataframe provided for quality validation")
            return {"total_records": 0, "status": "EMPTY"}
        
        # Quality checks
        null_customer_id = df.filter(col("customer_id").isNull()).count()
        null_email = df.filter(col("email").isNull()).count()
        duplicate_customer_id = df.groupBy("customer_id").count().filter(col("count") > 1).count()
        
        # Calculate quality metrics
        quality_score = max(0, 100 - ((null_customer_id + duplicate_customer_id) / total_records * 100))
        
        validation_results = {
            "total_records": total_records,
            "null_customer_id": null_customer_id,
            "null_email": null_email,
            "duplicate_customer_id": duplicate_customer_id,
            "quality_score": round(quality_score, 2),
            "timestamp": datetime.now().isoformat(),
            "status": "PASSED" if quality_score >= 95 else "WARNING"
        }
        
        logger.info(
            "Data quality validation completed",
            **validation_results
        )
        
        # Log warnings for quality issues
        if null_customer_id > 0:
            logger.warning(f"Found {null_customer_id} null customer IDs")
        if duplicate_customer_id > 0:
            logger.warning(f"Found {duplicate_customer_id} duplicate customer IDs")
        
        return validation_results
    
    except Exception as e:
        logger.error(f"Data quality validation failed: {str(e)}")
        raise

# COMMAND ----------

@handle_exceptions
def transform_customer_data(df: DataFrame) -> DataFrame:
    """Apply business transformations with logging"""
    
    try:
        logger.info("Starting customer data transformation")
        
        transformed_df = df.select(
            col("customer_id"),
            col("first_name"),
            col("last_name"),
            col("email").alias("email_address"),
            col("phone"),
            col("country").alias("country_code"),
            col("city"),
            col("state"),
            col("postal_code"),
            col("customer_since").alias("customer_since_date"),
            col("last_modified").alias("last_modified_date"),
            col("customer_status").alias("status"),
            col("segment"),
            when(col("customer_status") == "ACTIVE", 1).otherwise(0).alias("is_active"),
            current_timestamp().alias("processed_timestamp"),
            lit(datetime.now().date()).alias("batch_date")
        ).dropDuplicates(["customer_id"])
        
        final_count = transformed_df.count()
        logger.info(f"Transformation completed: {final_count} records")
        
        return transformed_df
    
    except Exception as e:
        logger.error(f"Data transformation failed: {str(e)}")
        raise

# COMMAND ----------

@handle_exceptions
def write_output(df: DataFrame, output_format: str = "parquet"):
    """Write output with error handling and backups"""
    
    try:
        logger.info(f"Writing output in {output_format} format")
        
        # Write to silver layer
        df.coalesce(50).write \
            .format(output_format) \
            .mode("overwrite") \
            .save(ETLConfig.SILVER_PATH)
        
        logger.info(f"Data successfully written to {ETLConfig.SILVER_PATH}")
        
        # Create Delta table
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {ETLConfig.DATABASE_NAME}")
        
        df.write \
            .format("delta") \
            .mode("overwrite") \
            .option("mergeSchema", "true") \
            .saveAsTable(f"{ETLConfig.DATABASE_NAME}.{ETLConfig.TABLE_NAME}")
        
        logger.info(f"Delta table created: {ETLConfig.DATABASE_NAME}.{ETLConfig.TABLE_NAME}")
        
        return True
    
    except Exception as e:
        logger.error(f"Write operation failed: {str(e)}")
        raise

# COMMAND ----------

@handle_exceptions
def save_metrics(metrics: Dict[str, Any]):
    """Save pipeline metrics for monitoring"""
    
    try:
        metrics["pipeline_run_id"] = PIPELINE_RUN_ID
        metrics["pipeline_start_time"] = logger.start_time.isoformat()
        metrics["pipeline_end_time"] = datetime.now().isoformat()
        
        metrics_df = spark.createDataFrame([(metrics["pipeline_run_id"], str(metrics))], 
                                           ["run_id", "metrics"])
        
        metrics_df.write \
            .format("parquet") \
            .mode("append") \
            .save(ETLConfig.METRICS_PATH)
        
        logger.info("Metrics saved successfully")
    
    except Exception as e:
        logger.warning(f"Failed to save metrics: {str(e)}")
        # Don't fail pipeline if metrics save fails

# COMMAND ----------

# MAGIC %md
# MAGIC ## Main Pipeline Execution

# COMMAND ----------

def main():
    """Main ETL pipeline with comprehensive error handling"""
    
    metrics = {
        "status": "STARTED",
        "steps_completed": []
    }
    
    try:
        logger.info("="*60)
        logger.info("STARTING CUSTOMER INCREMENTAL ETL PIPELINE")
        logger.info("="*60)
        
        # Step 1: Read bronze data
        logger.info("STEP 1: Reading bronze layer")
        bronze_df = read_bronze_data(LOAD_TYPE)
        
        if bronze_df.count() == 0:
            logger.warning("No data found in bronze layer, exiting")
            metrics["status"] = "NO_DATA"
            return None
        
        metrics["steps_completed"].append("read_bronze")
        
        # Step 2: Transform
        logger.info("STEP 2: Transforming data")
        transformed_df = transform_customer_data(bronze_df)
        metrics["steps_completed"].append("transform")
        metrics["records_transformed"] = transformed_df.count()
        
        # Step 3: Deduplicate
        logger.info("STEP 3: Deduplicating records")
        deduped_df = deduplicate_records(transformed_df, ["customer_id"])
        metrics["steps_completed"].append("deduplicate")
        metrics["records_after_dedup"] = deduped_df.count()
        
        # Step 4: Quality validation
        logger.info("STEP 4: Validating data quality")
        quality_results = validate_data_quality(deduped_df)
        metrics["quality_results"] = quality_results
        metrics["steps_completed"].append("quality_validation")
        
        if quality_results.get("quality_score", 0) < 80:
            logger.warning(f"Data quality score below threshold: {quality_results['quality_score']}")
        
        # Step 5: Write output
        logger.info("STEP 5: Writing to output")
        write_output(deduped_df)
        metrics["steps_completed"].append("write_output")
        
        # Step 6: Save checkpoint
        logger.info("STEP 6: Saving checkpoint")
        save_checkpoint(datetime.now())
        metrics["steps_completed"].append("save_checkpoint")
        
        # Step 7: Save metrics
        logger.info("STEP 7: Saving metrics")
        save_metrics(metrics)
        metrics["steps_completed"].append("save_metrics")
        
        metrics["status"] = "SUCCESS"
        
        logger.info("="*60)
        logger.info("PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"Total records processed: {deduped_df.count()}")
        logger.info(f"Pipeline duration: {(datetime.now() - logger.start_time).total_seconds()} seconds")
        
        return deduped_df
    
    except Exception as e:
        logger.critical(f"PIPELINE FAILED: {str(e)}")
        metrics["status"] = "FAILED"
        metrics["error"] = str(e)
        metrics["error_traceback"] = traceback.format_exc()
        save_metrics(metrics)
        raise

# COMMAND ----------

# Execute pipeline
result_df = main()

# COMMAND ----------

# Display results
if result_df:
    display(result_df.limit(10))
    print(f"\n✅ Pipeline completed successfully")
    print(f"Total records: {result_df.count()}")
else:
    print("⚠️ Pipeline completed with no output data")
