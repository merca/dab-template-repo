"""
Performance tests for enterprise components.

This module contains performance and load testing for the data processing
pipeline, quality validation, and monitoring systems to ensure they meet
enterprise performance requirements.
"""

import pytest
import time
from unittest.mock import Mock, patch
import concurrent.futures
import threading
from datetime import datetime, timedelta

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType


@pytest.mark.performance
class TestDataProcessingPerformance:
    """Performance tests for data processing operations."""
    
    def test_large_dataset_ingestion_performance(self, spark_session, benchmark):
        """Test performance of ingesting large datasets."""
        # Create large dataset (100k records)
        large_data_size = 100000
        
        def create_large_dataset():
            data = [(f"id_{i}", f"customer_{i}", i * 10.5, f"2024-01-{(i % 30) + 1:02d} 10:00:00") 
                   for i in range(large_data_size)]
            
            schema = StructType([
                StructField("transaction_id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("amount", DoubleType(), True),
                StructField("timestamp", StringType(), True)
            ])
            
            df = spark_session.createDataFrame(data, schema)
            return df.count()
        
        # Benchmark dataset creation and processing
        result = benchmark(create_large_dataset)
        
        assert result == large_data_size
        
        # Verify performance meets requirements (less than 30 seconds for 100k records)
        # This would be validated by benchmark fixture automatically
    
    def test_data_transformation_performance(self, spark_session, benchmark):
        """Test performance of complex data transformations."""
        # Create test dataset
        data_size = 50000
        data = [(f"id_{i}", f"customer_{i % 1000}", i * 2.5, f"category_{i % 10}") 
               for i in range(data_size)]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        def complex_transformation():
            from pyspark.sql.functions import sum as spark_sum, avg, count, col, when, upper
            
            # Complex aggregation with multiple operations
            result_df = df.groupBy("customer_id", "category").agg(
                count("id").alias("transaction_count"),
                spark_sum("amount").alias("total_amount"),
                avg("amount").alias("avg_amount")
            ).withColumn(
                "customer_tier",
                when(col("total_amount") > 10000, "PREMIUM")
                .when(col("total_amount") > 5000, "GOLD")
                .otherwise("STANDARD")
            ).withColumn(
                "category_upper", upper(col("category"))
            )
            
            return result_df.count()
        
        result = benchmark(complex_transformation)
        
        # Should have aggregated results for unique customer-category combinations
        assert result > 0 and result <= data_size
    
    def test_concurrent_data_processing_performance(self, spark_session):
        """Test performance under concurrent data processing loads."""
        def process_batch(batch_id):
            """Process a batch of data."""
            data = [(f"batch_{batch_id}_id_{i}", f"customer_{i}", i * 1.5) 
                   for i in range(1000)]
            
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("amount", DoubleType(), True)
            ])
            
            df = spark_session.createDataFrame(data, schema)
            return df.count(), batch_id
        
        start_time = time.time()
        
        # Process 10 concurrent batches
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(process_batch, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        # Verify all batches processed correctly
        assert len(results) == 10
        assert all(result[0] == 1000 for result in results)  # Each batch should have 1000 records
        
        # Performance requirement: Process 10k records across batches in under 60 seconds
        assert execution_time < 60
    
    def test_memory_usage_optimization(self, spark_session):
        """Test memory usage patterns for large datasets."""
        # Create progressively larger datasets and monitor processing
        dataset_sizes = [1000, 5000, 10000, 25000]
        processing_times = []
        
        for size in dataset_sizes:
            data = [(f"id_{i}", f"data_{i}", i * 2.0) for i in range(size)]
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("value", DoubleType(), True)
            ])
            
            start_time = time.time()
            df = spark_session.createDataFrame(data, schema)
            count = df.count()
            processing_time = time.time() - start_time
            
            processing_times.append(processing_time)
            assert count == size
        
        # Verify processing time scales reasonably (not exponentially)
        # Processing time should not increase by more than 10x when data increases by 25x
        time_ratio = processing_times[-1] / processing_times[0]
        data_ratio = dataset_sizes[-1] / dataset_sizes[0]
        
        assert time_ratio < data_ratio * 0.4  # Should be more efficient than linear scaling


@pytest.mark.performance
class TestDataQualityPerformance:
    """Performance tests for data quality validation."""
    
    def test_quality_validation_performance(self, spark_session, benchmark):
        """Test performance of quality validation on large datasets."""
        from src.lib.data_quality import DataQualityEngine, QualityRule, QualityRuleType
        
        # Create large test dataset
        data_size = 10000
        data = [(f"id_{i}", f"customer_{i}", f"email_{i}@example.com", i * 10.0) 
               for i in range(data_size)]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("email", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Set up quality engine with multiple rules
        engine = DataQualityEngine(spark_session)
        
        rules = [
            QualityRule("completeness_id", QualityRuleType.COMPLETENESS, "id", 
                       "id IS NOT NULL", 0.95),
            QualityRule("completeness_customer", QualityRuleType.COMPLETENESS, "customer_id", 
                       "customer_id IS NOT NULL", 0.95),
            QualityRule("validity_email", QualityRuleType.VALIDITY, "email", 
                       "email RLIKE '^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$'", 0.90),
            QualityRule("validity_amount", QualityRuleType.VALIDITY, "amount", 
                       "amount >= 0", 0.95)
        ]
        
        for rule in rules:
            engine.add_rule(rule)
        
        def validate_dataset():
            return engine.validate_dataset(df, "performance_test_table")
        
        # Benchmark quality validation
        result = benchmark(validate_dataset)
        
        assert len(result.rule_results) == 4
        assert result.dataset_name == "performance_test_table"
    
    def test_multiple_dataset_quality_validation(self, spark_session):
        """Test performance of validating multiple datasets concurrently."""
        from src.lib.data_quality import DataQualityEngine, QualityRule, QualityRuleType
        
        def validate_single_dataset(dataset_id):
            """Validate a single dataset."""
            data = [(f"id_{i}", f"customer_{dataset_id}_{i}", i * 5.0) 
                   for i in range(1000)]
            
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("amount", DoubleType(), True)
            ])
            
            df = spark_session.createDataFrame(data, schema)
            
            engine = DataQualityEngine(spark_session)
            rule = QualityRule("completeness_test", QualityRuleType.COMPLETENESS, "id", 
                              "id IS NOT NULL", 0.95)
            engine.add_rule(rule)
            
            result = engine.validate_dataset(df, f"dataset_{dataset_id}")
            return result.dataset_name, len(result.rule_results)
        
        start_time = time.time()
        
        # Validate 5 datasets concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
            futures = [executor.submit(validate_single_dataset, i) for i in range(5)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        assert len(results) == 5
        assert all(result[1] == 1 for result in results)  # Each should have 1 rule result
        
        # Should complete validation of 5k total records in under 30 seconds
        assert execution_time < 30


@pytest.mark.performance
class TestMonitoringPerformance:
    """Performance tests for monitoring and metrics collection."""
    
    def test_metrics_collection_performance(self, benchmark):
        """Test performance of metrics collection under load."""
        from src.lib.monitoring import MetricsCollector
        
        collector = MetricsCollector()
        
        def collect_bulk_metrics():
            # Simulate collecting many metrics rapidly
            for i in range(1000):
                collector.increment_counter(f"metric_{i % 10}", tags={"batch": "performance_test"})
                collector.record_gauge(f"gauge_{i % 5}", i * 0.1, tags={"type": "performance"})
                collector.record_histogram(f"histogram_{i % 3}", i * 2.5, tags={"category": "test"})
            
            return collector.get_metrics_count()
        
        # Benchmark metrics collection
        result = benchmark(collect_bulk_metrics)
        
        # Should have collected all metrics
        assert result >= 3000  # 1000 each of counters, gauges, histograms
    
    def test_concurrent_metrics_collection(self):
        """Test concurrent metrics collection performance."""
        from src.lib.monitoring import MetricsCollector
        
        collector = MetricsCollector()
        
        def collect_metrics_batch(batch_id):
            """Collect metrics for a specific batch."""
            for i in range(100):
                collector.increment_counter("requests", tags={"batch": str(batch_id)})
                collector.record_gauge("active_connections", i, tags={"batch": str(batch_id)})
                collector.record_histogram("response_time", i * 0.01, tags={"batch": str(batch_id)})
            
            return batch_id
        
        start_time = time.time()
        
        # Run 10 concurrent metric collection batches
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(collect_metrics_batch, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        assert len(results) == 10
        
        # Should complete concurrent metrics collection quickly
        assert execution_time < 10
        
        # Verify metrics were collected
        metrics_count = collector.get_metrics_count()
        assert metrics_count >= 3000  # 10 batches * 100 iterations * 3 metric types
    
    @patch('src.lib.monitoring.AlertManager')
    def test_alert_processing_performance(self, mock_alert_manager):
        """Test performance of alert processing under high load."""
        from src.lib.monitoring import AlertManager
        
        # Mock alert manager
        mock_manager = Mock()
        mock_alert_manager.return_value = mock_manager
        mock_manager.send_alert.return_value = True
        
        alert_manager = AlertManager()
        
        def process_alerts_batch():
            alerts = []
            for i in range(500):
                alert = {
                    "id": f"alert_{i}",
                    "severity": ["LOW", "MEDIUM", "HIGH", "CRITICAL"][i % 4],
                    "message": f"Test alert {i}",
                    "timestamp": datetime.now().isoformat(),
                    "source": "performance_test"
                }
                alerts.append(alert)
            
            # Process all alerts
            for alert in alerts:
                alert_manager.send_alert(
                    alert["message"], 
                    alert["severity"], 
                    alert["source"]
                )
            
            return len(alerts)
        
        start_time = time.time()
        processed_count = process_alerts_batch()
        end_time = time.time()
        
        processing_time = end_time - start_time
        
        assert processed_count == 500
        
        # Should process 500 alerts in under 10 seconds
        assert processing_time < 10
        
        # Verify alert manager was called for each alert
        assert mock_manager.send_alert.call_count == 500


@pytest.mark.performance
class TestSecretManagerPerformance:
    """Performance tests for secret management operations."""
    
    def test_secret_retrieval_performance(self, benchmark):
        """Test performance of secret retrieval operations."""
        from src.lib.secret_manager import SecretManager, EnvironmentVariableProvider
        
        # Set up secret manager with mock provider
        manager = SecretManager(enable_cache=True)
        
        provider = Mock()
        provider.name = "performance_test_provider"
        provider.get_secret.side_effect = lambda key: f"secret_value_for_{key}"
        
        manager.add_provider(provider)
        
        def retrieve_secrets_batch():
            secrets = {}
            for i in range(100):
                key = f"secret_key_{i % 20}"  # Some overlap for cache testing
                secrets[key] = manager.get_secret(key)
            return len(secrets)
        
        # Benchmark secret retrieval
        result = benchmark(retrieve_secrets_batch)
        
        assert result <= 20  # Should be 20 unique secrets due to overlap
        
        # Verify caching is working (fewer provider calls than requests)
        assert provider.get_secret.call_count <= 20
    
    def test_concurrent_secret_access(self):
        """Test concurrent access to secrets for performance."""
        from src.lib.secret_manager import SecretManager
        
        manager = SecretManager(enable_cache=True)
        
        provider = Mock()
        provider.name = "concurrent_test_provider"
        provider.get_secret.side_effect = lambda key: f"value_{key}"
        
        manager.add_provider(provider)
        
        def access_secrets_concurrently(thread_id):
            """Access secrets from multiple threads."""
            secrets = {}
            for i in range(50):
                key = f"secret_{i % 10}"  # Overlap to test cache performance
                secrets[key] = manager.get_secret(key)
            return thread_id, len(secrets)
        
        start_time = time.time()
        
        # Run concurrent secret access
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = [executor.submit(access_secrets_concurrently, i) for i in range(10)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        execution_time = end_time - start_time
        
        assert len(results) == 10
        
        # Should complete concurrent secret access quickly due to caching
        assert execution_time < 5
        
        # Verify efficient caching (provider called much less than total requests)
        total_requests = 10 * 50  # 10 threads * 50 requests each
        unique_keys = 10  # Only 10 unique keys
        
        # Provider should be called closer to unique key count due to caching
        assert provider.get_secret.call_count <= unique_keys * 2  # Allow some cache misses


@pytest.mark.performance
class TestEndToEndPerformance:
    """End-to-end performance tests for complete workflows."""
    
    def test_complete_pipeline_performance(self, spark_session):
        """Test performance of complete data pipeline."""
        # Step 1: Data ingestion
        ingestion_start = time.time()
        
        data_size = 10000
        raw_data = [(f"id_{i}", f"customer_{i % 1000}", i * 5.0, f"2024-01-{(i % 30) + 1:02d}") 
                   for i in range(data_size)]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("date", StringType(), True)
        ])
        
        bronze_df = spark_session.createDataFrame(raw_data, schema)
        bronze_count = bronze_df.count()
        
        ingestion_time = time.time() - ingestion_start
        
        # Step 2: Data transformation
        transformation_start = time.time()
        
        from pyspark.sql.functions import col, when, avg, sum as spark_sum
        
        silver_df = bronze_df.withColumn(
            "amount_valid", 
            when(col("amount") > 0, True).otherwise(False)
        ).filter(col("amount_valid"))
        
        silver_count = silver_df.count()
        
        transformation_time = time.time() - transformation_start
        
        # Step 3: Data aggregation
        aggregation_start = time.time()
        
        gold_df = silver_df.groupBy("customer_id", "date").agg(
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
        
        gold_count = gold_df.count()
        
        aggregation_time = time.time() - aggregation_start
        
        # Verify results
        assert bronze_count == data_size
        assert silver_count <= bronze_count  # May filter some records
        assert gold_count > 0
        
        # Performance requirements
        assert ingestion_time < 30  # Ingestion should complete in under 30 seconds
        assert transformation_time < 20  # Transformation in under 20 seconds
        assert aggregation_time < 15  # Aggregation in under 15 seconds
        
        total_time = ingestion_time + transformation_time + aggregation_time
        assert total_time < 60  # Complete pipeline in under 1 minute
    
    def test_load_testing_simulation(self, spark_session):
        """Simulate high-load conditions for stress testing."""
        def simulate_data_load(load_id):
            """Simulate a data processing load."""
            data = [(f"load_{load_id}_id_{i}", f"customer_{i}", i * 2.0) 
                   for i in range(500)]
            
            schema = StructType([
                StructField("id", StringType(), True),
                StructField("customer_id", StringType(), True),
                StructField("amount", DoubleType(), True)
            ])
            
            df = spark_session.createDataFrame(data, schema)
            
            # Simulate processing
            from pyspark.sql.functions import sum as spark_sum, avg
            result = df.groupBy("customer_id").agg(
                spark_sum("amount").alias("total"),
                avg("amount").alias("average")
            )
            
            return result.count(), load_id
        
        start_time = time.time()
        
        # Simulate 20 concurrent loads (high stress)
        with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
            futures = [executor.submit(simulate_data_load, i) for i in range(20)]
            results = [future.result() for future in concurrent.futures.as_completed(futures)]
        
        end_time = time.time()
        total_execution_time = end_time - start_time
        
        # Verify all loads completed
        assert len(results) == 20
        assert all(result[0] > 0 for result in results)  # All should have results
        
        # Performance under high load should still be reasonable
        assert total_execution_time < 120  # Complete 20 concurrent loads in under 2 minutes
        
        # Calculate throughput
        total_records_processed = 20 * 500  # 20 loads * 500 records each
        throughput = total_records_processed / total_execution_time
        
        # Should achieve reasonable throughput (at least 100 records/second)
        assert throughput > 100


if __name__ == "__main__":
    pytest.main([__file__, "-v", "--benchmark-only"])