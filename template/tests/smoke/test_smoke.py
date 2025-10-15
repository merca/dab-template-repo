"""
Smoke tests for basic functionality validation.

This module contains quick validation tests to ensure core functionality
works correctly after deployment or major changes. These tests are designed
to run fast and catch basic integration issues.
"""

import pytest
import os
import tempfile
from unittest.mock import Mock, patch
import json
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


@pytest.mark.smoke
class TestBasicFunctionality:
    """Smoke tests for basic system functionality."""
    
    def test_spark_session_creation(self, spark_session):
        """Test that Spark session can be created successfully."""
        assert spark_session is not None
        assert spark_session.version is not None
        
        # Test basic DataFrame creation
        data = [("test1", 1), ("test2", 2)]
        schema = StructType([
            StructField("name", StringType(), True),
            StructField("value", IntegerType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        assert df.count() == 2
        assert len(df.columns) == 2
    
    def test_data_quality_engine_basic(self, spark_session):
        """Test basic data quality engine functionality."""
        from src.lib.data_quality import DataQualityEngine, QualityRule, QualityRuleType
        
        # Create basic test data
        data = [("1", "test@example.com"), ("2", "invalid-email"), ("3", "test2@example.com")]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Initialize quality engine
        engine = DataQualityEngine(spark_session)
        assert engine is not None
        
        # Add simple rule
        rule = QualityRule(
            name="email_validity",
            rule_type=QualityRuleType.VALIDITY,
            column="email",
            expression="email RLIKE '^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$'",
            threshold=0.60
        )
        
        engine.add_rule(rule)
        assert len(engine.rules) == 1
        
        # Validate dataset
        results = engine.validate_dataset(df, "smoke_test_table")
        assert results is not None
        assert len(results.rule_results) == 1
        assert results.dataset_name == "smoke_test_table"
    
    def test_secret_manager_basic(self):
        """Test basic secret manager functionality."""
        from src.lib.secret_manager import SecretManager, EnvironmentVariableProvider
        
        # Test with environment variable provider
        with patch.dict(os.environ, {'TEST_SECRET': 'test_value'}):
            manager = SecretManager()
            provider = EnvironmentVariableProvider()
            manager.add_provider(provider)
            
            secret = manager.get_secret('TEST_SECRET')
            assert secret == 'test_value'
    
    def test_monitoring_basic(self):
        """Test basic monitoring functionality."""
        from src.lib.monitoring import MetricsCollector
        
        collector = MetricsCollector()
        assert collector is not None
        
        # Test basic metric collection
        collector.increment_counter("test_counter", tags={"smoke": "test"})
        collector.record_gauge("test_gauge", 42.0, tags={"smoke": "test"})
        collector.record_histogram("test_histogram", 1.5, tags={"smoke": "test"})
        
        # Verify metrics were collected
        metrics_count = collector.get_metrics_count()
        assert metrics_count >= 3  # At least 3 metrics collected


@pytest.mark.smoke
class TestNotebookDependencies:
    """Smoke tests to verify notebook dependencies and imports."""
    
    def test_notebook_imports(self):
        """Test that all required modules can be imported."""
        # Test core library imports
        try:
            from src.lib.data_quality import DataQualityEngine
            from src.lib.secret_manager import SecretManager
            from src.lib.monitoring import MetricsCollector
            assert True  # If we get here, imports worked
        except ImportError as e:
            pytest.fail(f"Failed to import required modules: {e}")
    
    def test_pyspark_imports(self):
        """Test that PySpark components can be imported."""
        try:
            from pyspark.sql import SparkSession
            from pyspark.sql.functions import col, when, sum as spark_sum
            from pyspark.sql.types import StructType, StructField, StringType
            assert True
        except ImportError as e:
            pytest.fail(f"Failed to import PySpark modules: {e}")
    
    def test_external_dependencies(self):
        """Test that external dependencies are available."""
        try:
            import pytest
            import pandas as pd  # Often used in notebooks
            import json
            import datetime
            assert True
        except ImportError as e:
            pytest.fail(f"Failed to import external dependencies: {e}")


@pytest.mark.smoke
class TestDataPipelineBasics:
    """Smoke tests for basic data pipeline functionality."""
    
    def test_bronze_to_silver_basic(self, spark_session):
        """Test basic bronze to silver transformation."""
        # Create bronze data
        bronze_data = [
            ("1", "John Doe", "JOHN@EXAMPLE.COM"),
            ("2", "Jane Smith", "jane@example.com"),
            ("3", "Bob", "invalid-email")
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("email", StringType(), True)
        ])
        
        bronze_df = spark_session.createDataFrame(bronze_data, schema)
        
        # Apply silver transformations
        from pyspark.sql.functions import lower, col, when
        
        silver_df = bronze_df.select(
            col("id"),
            col("name"),
            lower(col("email")).alias("email_clean"),
            when(col("email").rlike("^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$"), True)
            .otherwise(False).alias("email_valid")
        )
        
        assert silver_df.count() == 3
        assert "email_clean" in silver_df.columns
        assert "email_valid" in silver_df.columns
        
        # Verify transformations
        results = silver_df.collect()
        assert results[0]["email_clean"] == "john@example.com"  # Lowercased
        assert results[2]["email_valid"] is False  # Invalid email detected
    
    def test_silver_to_gold_basic(self, spark_session):
        """Test basic silver to gold aggregation."""
        # Create silver data
        silver_data = [
            ("1", "cust_1", 100.0, "2024-01-01", True),
            ("2", "cust_1", 150.0, "2024-01-01", True),
            ("3", "cust_2", 200.0, "2024-01-01", True),
            ("4", "cust_1", 50.0, "2024-01-02", True)
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("date", StringType(), True),
            StructField("valid", BooleanType(), True)
        ])
        
        from pyspark.sql.types import BooleanType
        silver_df = spark_session.createDataFrame(silver_data, schema)
        
        # Apply gold aggregations
        from pyspark.sql.functions import sum as spark_sum, count, avg
        
        gold_df = silver_df.filter("valid = true").groupBy("customer_id", "date").agg(
            count("id").alias("transaction_count"),
            spark_sum("amount").alias("total_amount"),
            avg("amount").alias("avg_amount")
        )
        
        assert gold_df.count() == 3  # 2 for cust_1 (2 dates) + 1 for cust_2
        
        # Verify aggregation
        cust_1_jan_1 = gold_df.filter("customer_id = 'cust_1' AND date = '2024-01-01'").collect()[0]
        assert cust_1_jan_1["transaction_count"] == 2
        assert cust_1_jan_1["total_amount"] == 250.0


@pytest.mark.smoke
class TestConfigurationAndEnvironment:
    """Smoke tests for configuration and environment setup."""
    
    def test_environment_variables_access(self):
        """Test access to environment variables."""
        # Test setting and reading environment variables
        test_var = "SMOKE_TEST_VAR"
        test_value = "smoke_test_value"
        
        with patch.dict(os.environ, {test_var: test_value}):
            assert os.getenv(test_var) == test_value
    
    def test_file_system_access(self):
        """Test basic file system operations."""
        # Test creating and reading temporary files
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.json') as f:
            test_data = {"smoke_test": True, "timestamp": datetime.now().isoformat()}
            json.dump(test_data, f)
            temp_file = f.name
        
        try:
            # Read the file back
            with open(temp_file, 'r') as f:
                loaded_data = json.load(f)
            
            assert loaded_data["smoke_test"] is True
            assert "timestamp" in loaded_data
        finally:
            # Clean up
            os.unlink(temp_file)
    
    def test_json_schema_validation(self, databricks_bundle_schema):
        """Test that JSON schema can be loaded and validated."""
        # Test schema structure
        assert "properties" in databricks_bundle_schema
        assert "project_name" in databricks_bundle_schema["properties"]
        assert "catalog_name" in databricks_bundle_schema["properties"]
    
    def test_template_variables_basic(self):
        """Test basic template variable functionality."""
        # Simulate template variable substitution
        template_vars = {
            "project_name": "smoke_test_project",
            "catalog_name": "smoke_test_catalog",
            "schema_name": "smoke_test_schema"
        }
        
        # Test variable access
        assert template_vars["project_name"] == "smoke_test_project"
        assert all(key in template_vars for key in ["project_name", "catalog_name", "schema_name"])


@pytest.mark.smoke
class TestDataValidationBasics:
    """Smoke tests for data validation functionality."""
    
    def test_completeness_validation(self, spark_session):
        """Test basic completeness validation."""
        # Create test data with null values
        data = [
            ("1", "value1"),
            ("2", None),
            ("3", "value3"),
            (None, "value4")
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("value", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Check completeness manually
        total_records = df.count()
        non_null_ids = df.filter("id IS NOT NULL").count()
        non_null_values = df.filter("value IS NOT NULL").count()
        
        assert total_records == 4
        assert non_null_ids == 3  # 75% completeness for id
        assert non_null_values == 3  # 75% completeness for value
        
        # Calculate completeness rates
        id_completeness = non_null_ids / total_records
        value_completeness = non_null_values / total_records
        
        assert id_completeness == 0.75
        assert value_completeness == 0.75
    
    def test_validity_validation(self, spark_session):
        """Test basic validity validation."""
        # Create test data with valid/invalid emails
        data = [
            ("user1@example.com",),
            ("invalid-email",),
            ("user2@test.org",),
            ("another.invalid",),
            ("user3@company.co.uk",)
        ]
        
        schema = StructType([StructField("email", StringType(), True)])
        df = spark_session.createDataFrame(data, schema)
        
        # Validate email format
        email_pattern = "^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$"
        valid_emails = df.filter(f"email RLIKE '{email_pattern}'").count()
        total_emails = df.count()
        
        assert total_emails == 5
        assert valid_emails == 3  # 60% validity rate
        
        validity_rate = valid_emails / total_emails
        assert validity_rate == 0.6


@pytest.mark.smoke
class TestIntegrationPoints:
    """Smoke tests for critical integration points."""
    
    def test_databricks_bundle_structure(self):
        """Test that expected bundle structure exists."""
        # Check for critical files and directories
        critical_paths = [
            "src/lib/data_quality.py",
            "src/lib/secret_manager.py",
            "src/lib/monitoring.py",
            "src/notebooks/data_processing.py",
            "databricks.yml.tmpl",
            "template-schema.json"
        ]
        
        for path in critical_paths:
            assert os.path.exists(path), f"Critical file missing: {path}"
    
    def test_template_schema_validity(self):
        """Test that template schema is valid JSON."""
        schema_path = "template-schema.json"
        
        try:
            with open(schema_path, 'r') as f:
                schema = json.load(f)
            
            # Basic schema validation
            assert isinstance(schema, dict)
            assert "properties" in schema
            assert "required" in schema
            
        except (json.JSONDecodeError, FileNotFoundError) as e:
            pytest.fail(f"Template schema is invalid: {e}")
    
    def test_notebook_accessibility(self):
        """Test that notebooks exist and are accessible."""
        notebook_paths = [
            "src/notebooks/data_processing.py",
            "src/notebooks/bronze_ingestion.py",
            "src/notebooks/silver_transformation.py",
            "src/notebooks/gold_aggregation.py",
            "src/notebooks/data_quality_validation.py",
            "src/notebooks/system_monitoring.py"
        ]
        
        for notebook_path in notebook_paths:
            assert os.path.exists(notebook_path), f"Notebook missing: {notebook_path}"
            
            # Check that file is not empty
            with open(notebook_path, 'r') as f:
                content = f.read().strip()
            assert len(content) > 0, f"Notebook is empty: {notebook_path}"


if __name__ == "__main__":
    pytest.main([__file__, "-v", "-m", "smoke"])