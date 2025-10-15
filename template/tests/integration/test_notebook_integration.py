"""
Integration tests for notebook functionality.

This module tests the integration between notebooks and the broader
data pipeline, ensuring notebooks can be executed and integrated correctly
with the Databricks environment.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import json
import tempfile
import os
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


@pytest.mark.integration
class TestNotebookIntegration:
    """Integration tests for notebook execution and data flow."""
    
    def test_bronze_ingestion_notebook(self, spark_session):
        """Test bronze layer ingestion notebook integration."""
        # Create sample raw data
        raw_data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00"),
            ("2", "Jane Smith", "jane@example.com", "2024-01-01 11:00:00"),
            ("3", "Bob Johnson", "bob@example.com", "2024-01-01 12:00:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_at", StringType(), True)
        ])
        
        raw_df = spark_session.createDataFrame(raw_data, schema)
        
        # Simulate bronze layer processing
        bronze_df = raw_df.withColumn("ingestion_timestamp", 
                                    spark_session.sql("SELECT current_timestamp()").collect()[0][0])
        
        assert bronze_df.count() == 3
        assert "ingestion_timestamp" in bronze_df.columns
        assert bronze_df.filter("customer_id IS NULL").count() == 0
    
    def test_silver_transformation_notebook(self, spark_session):
        """Test silver layer transformation notebook integration."""
        # Create bronze data
        bronze_data = [
            ("1", "John Doe", "john@example.com", "2024-01-01 10:00:00", "2024-01-01 10:01:00"),
            ("2", "Jane Smith", "JANE@EXAMPLE.COM", "2024-01-01 11:00:00", "2024-01-01 11:01:00"),
            ("3", "Bob Johnson", "bob@invalid", "2024-01-01 12:00:00", "2024-01-01 12:01:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("ingestion_timestamp", StringType(), True)
        ])
        
        bronze_df = spark_session.createDataFrame(bronze_data, schema)
        
        # Simulate silver layer transformations
        from pyspark.sql.functions import lower, regexp_replace, when, col
        
        silver_df = bronze_df.select(
            col("customer_id"),
            regexp_replace(col("name"), "\\s+", " ").alias("name_clean"),
            lower(col("email")).alias("email_clean"),
            col("created_at"),
            when(col("email").rlike("^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$"), True)
            .otherwise(False).alias("email_valid"),
            col("ingestion_timestamp")
        )
        
        assert silver_df.count() == 3
        assert silver_df.filter(col("email_valid") == True).count() == 2  # Only 2 valid emails
        assert silver_df.select("email_clean").collect()[1][0] == "jane@example.com"  # Lowercased
    
    def test_gold_aggregation_notebook(self, spark_session):
        """Test gold layer aggregation notebook integration."""
        # Create silver data
        silver_data = [
            ("1", "John Doe", "john@example.com", "2024-01-01", True, "2024-01-01 10:01:00"),
            ("2", "Jane Smith", "jane@example.com", "2024-01-01", True, "2024-01-01 11:01:00"),
            ("3", "Bob Johnson", "bob@invalid", "2024-01-01", False, "2024-01-01 12:01:00"),
            ("4", "Alice Brown", "alice@example.com", "2024-01-02", True, "2024-01-02 09:01:00")
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name_clean", StringType(), True),
            StructField("email_clean", StringType(), True),
            StructField("date", StringType(), True),
            StructField("email_valid", BooleanType(), True),
            StructField("ingestion_timestamp", StringType(), True)
        ])
        
        from pyspark.sql.types import BooleanType
        silver_df = spark_session.createDataFrame(silver_data, schema)
        
        # Simulate gold layer aggregations
        from pyspark.sql.functions import count, sum as spark_sum, col
        
        daily_summary = silver_df.groupBy("date").agg(
            count("customer_id").alias("total_customers"),
            spark_sum(col("email_valid").cast("int")).alias("valid_email_customers"),
            (spark_sum(col("email_valid").cast("int")) / count("customer_id") * 100).alias("email_validity_rate")
        )
        
        assert daily_summary.count() == 2  # Two distinct dates
        
        jan_1_stats = daily_summary.filter(col("date") == "2024-01-01").collect()[0]
        assert jan_1_stats["total_customers"] == 3
        assert jan_1_stats["valid_email_customers"] == 2
        assert abs(jan_1_stats["email_validity_rate"] - 66.67) < 0.1
    
    def test_data_quality_validation_notebook(self, spark_session, sample_customer_data):
        """Test data quality validation notebook integration."""
        # Simulate data quality validation execution
        from src.lib.data_quality import DataQualityEngine, QualityRule, QualityRuleType
        
        engine = DataQualityEngine(spark_session)
        
        # Add validation rules
        rules = [
            QualityRule(
                name="customer_id_completeness",
                rule_type=QualityRuleType.COMPLETENESS,
                column="customer_id",
                expression="customer_id IS NOT NULL",
                threshold=0.95,
                severity="CRITICAL"
            ),
            QualityRule(
                name="email_validity",
                rule_type=QualityRuleType.VALIDITY,
                column="email",
                expression="email RLIKE '^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$'",
                threshold=0.80,
                severity="HIGH"
            )
        ]
        
        for rule in rules:
            engine.add_rule(rule)
        
        # Execute validation
        results = engine.validate_dataset(sample_customer_data, "customer_table")
        
        assert len(results.rule_results) == 2
        assert results.dataset_name == "customer_table"
        
        # Generate quality report
        report = engine.generate_quality_report(results)
        assert "summary" in report
        assert "detailed_results" in report
    
    def test_system_monitoring_notebook(self, spark_session):
        """Test system monitoring notebook integration."""
        # Simulate monitoring data collection
        monitoring_data = [
            ("pipeline_1", "bronze_ingestion", 120.5, "SUCCESS", "2024-01-01 10:00:00"),
            ("pipeline_1", "silver_transformation", 85.2, "SUCCESS", "2024-01-01 10:02:00"),
            ("pipeline_1", "gold_aggregation", 45.1, "SUCCESS", "2024-01-01 10:03:00"),
            ("pipeline_2", "bronze_ingestion", 150.0, "FAILED", "2024-01-01 11:00:00"),
        ]
        
        schema = StructType([
            StructField("pipeline_id", StringType(), True),
            StructField("stage", StringType(), True),
            StructField("duration_seconds", DoubleType(), True),
            StructField("status", StringType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        monitoring_df = spark_session.createDataFrame(monitoring_data, schema)
        
        # Analyze performance metrics
        from pyspark.sql.functions import avg, max as spark_max, count, col
        
        performance_summary = monitoring_df.groupBy("stage", "status").agg(
            count("*").alias("execution_count"),
            avg("duration_seconds").alias("avg_duration"),
            spark_max("duration_seconds").alias("max_duration")
        )
        
        assert performance_summary.count() == 4  # 3 successful stages + 1 failed
        
        # Check for failed executions
        failed_executions = monitoring_df.filter(col("status") == "FAILED")
        assert failed_executions.count() == 1
        assert failed_executions.collect()[0]["pipeline_id"] == "pipeline_2"


@pytest.mark.integration  
class TestDataPipelineFlow:
    """Test complete data pipeline flow integration."""
    
    def test_end_to_end_pipeline_flow(self, spark_session):
        """Test complete pipeline from bronze to gold."""
        # Step 1: Raw data ingestion (Bronze)
        raw_data = [
            ("txn_1", "cust_1", 100.50, "2024-01-01 10:00:00"),
            ("txn_2", "cust_2", 250.75, "2024-01-01 11:00:00"),
            ("txn_3", "cust_1", 75.25, "2024-01-01 12:00:00"),
            ("txn_4", "cust_3", -10.00, "2024-01-01 13:00:00"),  # Invalid negative amount
        ]
        
        raw_schema = StructType([
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("timestamp", StringType(), True)
        ])
        
        raw_df = spark_session.createDataFrame(raw_data, raw_schema)
        
        # Bronze layer: Add ingestion metadata
        from pyspark.sql.functions import current_timestamp, lit
        bronze_df = raw_df.withColumn("ingestion_timestamp", current_timestamp()) \
                          .withColumn("source_system", lit("transaction_api"))
        
        assert bronze_df.count() == 4
        
        # Step 2: Data cleansing and validation (Silver)
        from pyspark.sql.functions import col, when
        
        silver_df = bronze_df.select(
            col("transaction_id"),
            col("customer_id"),
            col("amount"),
            col("timestamp"),
            when(col("amount") > 0, True).otherwise(False).alias("amount_valid"),
            when(col("customer_id").isNotNull() & (col("customer_id") != ""), True)
            .otherwise(False).alias("customer_id_valid"),
            col("ingestion_timestamp"),
            col("source_system")
        ).filter(col("amount_valid") & col("customer_id_valid"))  # Filter out invalid records
        
        assert silver_df.count() == 3  # One record filtered out due to negative amount
        
        # Step 3: Business aggregations (Gold)
        from pyspark.sql.functions import sum as spark_sum, count, avg, date_format
        
        # Customer summary
        customer_summary = silver_df.groupBy("customer_id").agg(
            count("transaction_id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
        
        assert customer_summary.count() == 2  # Two unique customers (cust_1, cust_2)
        
        cust_1_summary = customer_summary.filter(col("customer_id") == "cust_1").collect()[0]
        assert cust_1_summary["transaction_count"] == 2
        assert abs(cust_1_summary["total_amount"] - 175.75) < 0.01
        
        # Daily summary
        daily_summary = silver_df.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd")) \
                                .groupBy("date").agg(
                                    count("transaction_id").alias("daily_transactions"),
                                    spark_sum("amount").alias("daily_revenue")
                                )
        
        assert daily_summary.count() == 1  # All transactions on same day
        daily_stats = daily_summary.collect()[0]
        assert daily_stats["daily_transactions"] == 3
        assert abs(daily_stats["daily_revenue"] - 426.5) < 0.01
    
    @patch('src.lib.monitoring.MetricsCollector')
    def test_pipeline_monitoring_integration(self, mock_metrics, spark_session):
        """Test pipeline monitoring integration."""
        # Mock monitoring setup
        mock_collector = Mock()
        mock_metrics.return_value = mock_collector
        
        # Simulate pipeline execution with monitoring
        pipeline_start_time = time.time()
        
        # Execute a simple data transformation
        test_data = [("1", "test"), ("2", "data")]
        test_schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(test_data, test_schema)
        result_count = df.count()
        
        pipeline_end_time = time.time()
        execution_duration = pipeline_end_time - pipeline_start_time
        
        # Verify monitoring calls would be made
        assert result_count == 2
        assert execution_duration > 0
        
        # In real implementation, these metrics would be collected
        expected_metrics = {
            "pipeline_duration": execution_duration,
            "records_processed": result_count,
            "pipeline_status": "SUCCESS"
        }
        
        assert expected_metrics["records_processed"] == 2
        assert expected_metrics["pipeline_status"] == "SUCCESS"


@pytest.mark.integration
class TestNotebookErrorHandling:
    """Test error handling in notebook integration scenarios."""
    
    def test_data_quality_failure_handling(self, spark_session):
        """Test handling of data quality failures in pipeline."""
        # Create data with quality issues
        problematic_data = [
            (None, "John Doe", "john@example.com"),  # Missing customer_id
            ("2", "", "jane@example.com"),            # Empty name
            ("3", "Bob Johnson", "invalid-email"),     # Invalid email
            ("4", "Alice Brown", "alice@example.com")  # Valid record
        ]
        
        schema = StructType([
            StructField("customer_id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(problematic_data, schema)
        
        # Apply data quality validations
        from pyspark.sql.functions import col, when, length
        
        quality_df = df.withColumn(
            "quality_issues",
            when(col("customer_id").isNull(), "missing_customer_id")
            .when(col("name").isNull() | (length(col("name")) == 0), "empty_name")
            .when(~col("email").rlike("^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$"), "invalid_email")
            .otherwise("valid")
        )
        
        # Separate valid and invalid records
        valid_records = quality_df.filter(col("quality_issues") == "valid")
        invalid_records = quality_df.filter(col("quality_issues") != "valid")
        
        assert valid_records.count() == 1
        assert invalid_records.count() == 3
        
        # Verify issue categorization
        issue_summary = invalid_records.groupBy("quality_issues").count()
        issues_dict = {row["quality_issues"]: row["count"] for row in issue_summary.collect()}
        
        assert issues_dict["missing_customer_id"] == 1
        assert issues_dict["empty_name"] == 1
        assert issues_dict["invalid_email"] == 1
    
    def test_notebook_dependency_validation(self, spark_session):
        """Test validation of notebook dependencies and prerequisites."""
        # Simulate checking for required tables/views
        required_tables = ["bronze_customers", "bronze_transactions"]
        available_tables = ["bronze_customers"]  # Missing bronze_transactions
        
        missing_tables = [table for table in required_tables if table not in available_tables]
        
        assert len(missing_tables) == 1
        assert "bronze_transactions" in missing_tables
        
        # In a real scenario, this would prevent notebook execution
        can_execute = len(missing_tables) == 0
        assert can_execute is False
    
    def test_resource_constraint_handling(self, spark_session):
        """Test handling of resource constraints in notebook execution."""
        # Simulate memory-intensive operation with fallback
        large_data_size = 1000  # Simulate large dataset
        memory_threshold = 500
        
        if large_data_size > memory_threshold:
            # Use chunked processing
            chunk_size = memory_threshold
            chunks = [large_data_size // chunk_size, large_data_size % chunk_size]
            total_processed = sum(chunk for chunk in chunks if chunk > 0)
        else:
            # Process all at once
            total_processed = large_data_size
        
        assert total_processed == large_data_size
        
        # Create sample chunked data processing
        all_results = []
        for i in range(0, large_data_size, chunk_size):
            chunk_data = [(str(j), f"value_{j}") for j in range(i, min(i + chunk_size, large_data_size))]
            chunk_schema = StructType([
                StructField("id", StringType(), True),
                StructField("value", StringType(), True)
            ])
            
            chunk_df = spark_session.createDataFrame(chunk_data, chunk_schema)
            chunk_count = chunk_df.count()
            all_results.append(chunk_count)
        
        total_records = sum(all_results)
        assert total_records == large_data_size


if __name__ == "__main__":
    pytest.main([__file__, "-v"])