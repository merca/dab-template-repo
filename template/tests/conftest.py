"""
Enterprise Testing Configuration and Fixtures

This module provides comprehensive testing fixtures and utilities for the entire test suite.
Includes Spark session management, Databricks connectivity, test data generation,
and enterprise-grade testing infrastructure.
"""

import os
import sys
import pytest
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, Optional, Generator
import tempfile
import shutil
import logging
from unittest.mock import Mock, MagicMock

# Add src to Python path for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DoubleType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

try:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.core import Config
    DATABRICKS_SDK_AVAILABLE = True
except ImportError:
    DATABRICKS_SDK_AVAILABLE = False

# Import our modules for testing
try:
    from src.data_quality import QualityRuleEngine, MetricsCollector, QualityRule, MetricType
    from src.secret_manager import SecretManager, SecretProvider
    from src.monitoring import MetricsCollector as MonitoringCollector, DistributedTracer, AlertManager
    MODULES_AVAILABLE = True
except ImportError as e:
    print(f"Warning: Could not import modules - {e}")
    MODULES_AVAILABLE = False


# ===== Test Configuration =====

@pytest.fixture(scope="session", autouse=True)
def configure_test_environment():
    """Configure the testing environment with proper settings."""
    # Set testing environment variables
    os.environ["TESTING"] = "true"
    os.environ["PYTHONPATH"] = str(Path(__file__).parent.parent / "src")
    
    # Configure logging for tests
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[logging.StreamHandler()]
    )
    
    # Suppress verbose logging from third-party libraries
    logging.getLogger("py4j").setLevel(logging.WARNING)
    logging.getLogger("pyspark").setLevel(logging.WARNING)
    logging.getLogger("databricks").setLevel(logging.WARNING)
    
    yield
    
    # Cleanup after all tests
    if "TESTING" in os.environ:
        del os.environ["TESTING"]


# ===== Spark Testing Fixtures =====

@pytest.fixture(scope="session")
def spark_session() -> Optional[SparkSession]:
    """Create a Spark session for testing."""
    if not SPARK_AVAILABLE:
        pytest.skip("PySpark not available")
        return None
    
    spark = (SparkSession.builder
             .appName("{{project_name}}-tests")
             .config("spark.sql.execution.arrow.pyspark.enabled", "true")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
             .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
             .config("spark.sql.warehouse.dir", tempfile.mkdtemp())
             .master("local[*]")
             .getOrCreate())
    
    # Set log level to reduce noise
    spark.sparkContext.setLogLevel("WARN")
    
    yield spark
    
    spark.stop()


@pytest.fixture
def sample_customer_data(spark_session):
    """Generate sample customer data for testing."""
    if not spark_session:
        return None
        
    from faker import Faker
    fake = Faker()
    
    # Generate sample data
    data = []
    for i in range(100):
        data.append({
            "customer_id": f"CUST_{i:06d}",
            "email": fake.email(),
            "full_name": fake.name(),
            "phone": fake.phone_number(),
            "registration_date": fake.date_between(start_date='-2y', end_date='today'),
            "customer_tier": fake.random_element(["Basic", "Standard", "Premium"]),
            "customer_status": fake.random_element(["Active", "Inactive"]),
            "total_spent": round(fake.random.uniform(0, 10000), 2),
            "created_at": fake.date_time_between(start_date='-2y', end_date='now'),
            "updated_at": fake.date_time_between(start_date='-1y', end_date='now')
        })
    
    schema = StructType([
        StructField("customer_id", StringType(), False),
        StructField("email", StringType(), False),
        StructField("full_name", StringType(), False),
        StructField("phone", StringType(), True),
        StructField("registration_date", StringType(), False),
        StructField("customer_tier", StringType(), False),
        StructField("customer_status", StringType(), False),
        StructField("total_spent", DoubleType(), False),
        StructField("created_at", TimestampType(), False),
        StructField("updated_at", TimestampType(), False)
    ])
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def sample_transaction_data(spark_session):
    """Generate sample transaction data for testing."""
    if not spark_session:
        return None
        
    from faker import Faker
    fake = Faker()
    
    # Generate sample data
    data = []
    for i in range(500):
        data.append({
            "transaction_id": f"TXN{i:010d}",
            "customer_id": f"CUST_{fake.random_int(0, 99):06d}",
            "amount": round(fake.random.uniform(1, 1000), 2),
            "transaction_date": fake.date_between(start_date='-1y', end_date='today'),
            "transaction_type": fake.random_element(["purchase", "refund", "adjustment"]),
            "payment_method": fake.random_element(["credit card", "debit card", "digital wallet"]),
            "status": fake.random_element(["completed", "pending", "failed"]),
            "created_at": fake.date_time_between(start_date='-1y', end_date='now')
        })
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("transaction_date", StringType(), False),
        StructField("transaction_type", StringType(), False),
        StructField("payment_method", StringType(), False),
        StructField("status", StringType(), False),
        StructField("created_at", TimestampType(), False)
    ])
    
    return spark_session.createDataFrame(data, schema)


# ===== Databricks Testing Fixtures =====

@pytest.fixture
def mock_databricks_client():
    """Create a mock Databricks client for testing."""
    mock_client = Mock(spec=WorkspaceClient)
    
    # Mock common methods
    mock_client.clusters = Mock()
    mock_client.jobs = Mock()
    mock_client.workspace = Mock()
    mock_client.catalogs = Mock()
    mock_client.schemas = Mock()
    mock_client.tables = Mock()
    
    # Mock cluster responses
    mock_client.clusters.list.return_value = [
        Mock(cluster_id="test-cluster-1", cluster_name="{{project_name}}-test-cluster", state="RUNNING"),
        Mock(cluster_id="test-cluster-2", cluster_name="other-cluster", state="TERMINATED")
    ]
    
    # Mock job responses
    mock_client.jobs.list.return_value = [
        Mock(job_id=123, settings=Mock(name="{{project_name}}-test-job")),
        Mock(job_id=456, settings=Mock(name="other-job"))
    ]
    
    return mock_client


@pytest.fixture
def databricks_config():
    """Databricks configuration for integration tests."""
    return {
        "host": os.environ.get("DATABRICKS_HOST", "https://test.databricks.com"),
        "client_id": os.environ.get("DATABRICKS_CLIENT_ID", "test-client-id"),
        "client_secret": os.environ.get("DATABRICKS_CLIENT_SECRET", "test-client-secret"),
        "catalog": "main",
        "schema": "test_schema"
    }


# ===== Data Quality Testing Fixtures =====

@pytest.fixture
def quality_rule_engine(spark_session):
    """Create a QualityRuleEngine instance for testing."""
    if not MODULES_AVAILABLE or not spark_session:
        return Mock()
    return QualityRuleEngine(spark_session)


@pytest.fixture
def sample_quality_rules():
    """Generate sample quality rules for testing."""
    if not MODULES_AVAILABLE:
        return []
        
    return [
        QualityRule(
            rule_id="test_not_null",
            rule_name="Test Not Null Rule",
            rule_type="not_null",
            column="customer_id",
            severity="CRITICAL",
            description="Customer ID must not be null"
        ),
        QualityRule(
            rule_id="test_email_format",
            rule_name="Test Email Format Rule", 
            rule_type="format",
            column="email",
            parameters={"pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"},
            severity="HIGH",
            description="Email must be in valid format"
        ),
        QualityRule(
            rule_id="test_amount_range",
            rule_name="Test Amount Range Rule",
            rule_type="range", 
            column="amount",
            parameters={"min": 0.01, "max": 10000.0},
            severity="MEDIUM",
            description="Amount must be within valid range"
        )
    ]


# ===== Secret Management Testing Fixtures =====

@pytest.fixture
def mock_secret_manager():
    """Create a mock secret manager for testing."""
    if not MODULES_AVAILABLE:
        mock = Mock()
        mock.get_secret.return_value = "mock-secret-value"
        mock.put_secret.return_value = None
        mock.list_secrets.return_value = ["secret1", "secret2"]
        return mock
    
    # Create real SecretManager but with mocked providers
    manager = SecretManager(default_provider=SecretProvider.ENVIRONMENT)
    
    # Mock the environment provider
    mock_provider = Mock()
    mock_provider.get_secret.return_value = "test-secret-value"
    mock_provider.put_secret.return_value = None
    mock_provider.list_secrets.return_value = ["test_secret_1", "test_secret_2"]
    
    manager.providers[SecretProvider.ENVIRONMENT] = mock_provider
    
    return manager


# ===== Monitoring Testing Fixtures =====

@pytest.fixture
def metrics_collector():
    """Create a metrics collector for testing."""
    if not MODULES_AVAILABLE:
        return Mock()
    return MonitoringCollector()


@pytest.fixture
def distributed_tracer():
    """Create a distributed tracer for testing."""
    if not MODULES_AVAILABLE:
        return Mock()
    return DistributedTracer("test-service")


@pytest.fixture
def alert_manager(metrics_collector):
    """Create an alert manager for testing."""
    if not MODULES_AVAILABLE:
        return Mock()
    return AlertManager(metrics_collector)


# ===== File System Testing Fixtures =====

@pytest.fixture
def temp_directory():
    """Create a temporary directory for testing."""
    temp_dir = tempfile.mkdtemp()
    yield Path(temp_dir)
    shutil.rmtree(temp_dir, ignore_errors=True)


@pytest.fixture
def sample_config_file(temp_directory):
    """Create a sample configuration file for testing."""
    config_data = {
        "project_name": "{{project_name}}",
        "version": "1.0.0",
        "environments": {
            "dev": {
                "catalog": "main",
                "schema": "dev_schema"
            },
            "prod": {
                "catalog": "main", 
                "schema": "prod_schema"
            }
        }
    }
    
    config_file = temp_directory / "test_config.json"
    with open(config_file, "w") as f:
        import json
        json.dump(config_data, f, indent=2)
    
    return config_file


# ===== Performance Testing Fixtures =====

@pytest.fixture
def performance_benchmark():
    """Benchmark decorator for performance tests."""
    def benchmark_decorator(func):
        def wrapper(*args, **kwargs):
            import time
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            duration = end_time - start_time
            print(f"Benchmark {func.__name__}: {duration:.4f} seconds")
            return result
        return wrapper
    return benchmark_decorator


# ===== Test Data Generation Utilities =====

class TestDataGenerator:
    """Utility class for generating test data."""
    
    @staticmethod
    def generate_customer_data(count: int = 100) -> list:
        """Generate customer test data."""
        from faker import Faker
        fake = Faker()
        
        customers = []
        for i in range(count):
            customers.append({
                "customer_id": f"CUST_{i:06d}",
                "email": fake.email(),
                "full_name": fake.name(),
                "phone": fake.phone_number(),
                "registration_date": fake.date_between(start_date='-2y', end_date='today').isoformat(),
                "customer_tier": fake.random_element(["Basic", "Standard", "Premium"]),
                "total_spent": round(fake.random.uniform(0, 10000), 2)
            })
        return customers
    
    @staticmethod
    def generate_transaction_data(customer_ids: list, count: int = 1000) -> list:
        """Generate transaction test data."""
        from faker import Faker
        fake = Faker()
        
        transactions = []
        for i in range(count):
            transactions.append({
                "transaction_id": f"TXN{i:010d}",
                "customer_id": fake.random_element(customer_ids),
                "amount": round(fake.random.uniform(1, 1000), 2),
                "transaction_date": fake.date_between(start_date='-1y', end_date='today').isoformat(),
                "transaction_type": fake.random_element(["purchase", "refund", "adjustment"]),
                "payment_method": fake.random_element(["credit card", "debit card", "digital wallet"]),
                "status": fake.random_element(["completed", "pending", "failed"])
            })
        return transactions


@pytest.fixture
def test_data_generator():
    """Provide test data generator utility."""
    return TestDataGenerator()


# ===== Integration Testing Utilities =====

@pytest.fixture
def integration_test_config():
    """Configuration for integration tests."""
    return {
        "databricks": {
            "host": os.environ.get("DATABRICKS_HOST_DEV", "mock://databricks"),
            "token": os.environ.get("DATABRICKS_TOKEN_DEV", "mock-token"),
            "catalog": "main",
            "schema": "test_schema"
        },
        "storage": {
            "type": "local",
            "path": str(Path(__file__).parent / "data")
        },
        "timeouts": {
            "connection": 30,
            "query": 300
        }
    }


# ===== Cleanup and Teardown =====

@pytest.fixture(autouse=True)
def cleanup_test_tables(spark_session):
    """Cleanup test tables after each test."""
    yield
    
    if spark_session:
        # Clean up any test tables created during the test
        try:
            test_tables = [table.name for table in spark_session.catalog.listTables() 
                          if table.name.startswith("test_")]
            
            for table in test_tables:
                spark_session.sql(f"DROP TABLE IF EXISTS {table}")
        except Exception as e:
            print(f"Warning: Could not clean up test tables: {e}")


# ===== Pytest Markers and Hooks =====

def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "unit: Unit tests (fast, isolated)"
    )
    config.addinivalue_line(
        "markers", "integration: Integration tests (slower, requires external resources)"
    )
    config.addinivalue_line(
        "markers", "performance: Performance and benchmark tests"
    )
    config.addinivalue_line(
        "markers", "smoke: Smoke tests for critical functionality"
    )
    config.addinivalue_line(
        "markers", "databricks: Tests that require Databricks connectivity"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test location."""
    for item in items:
        # Add markers based on test file location
        if "unit" in item.fspath.strpath:
            item.add_marker(pytest.mark.unit)
        elif "integration" in item.fspath.strpath:
            item.add_marker(pytest.mark.integration)
        elif "performance" in item.fspath.strpath:
            item.add_marker(pytest.mark.performance)
        elif "smoke" in item.fspath.strpath:
            item.add_marker(pytest.mark.smoke)
        
        # Add databricks marker for tests that use databricks fixtures
        if any(fixture in item.fixturenames for fixture in ["databricks_client", "databricks_config"]):
            item.add_marker(pytest.mark.databricks)


# ===== Test Session Information =====

@pytest.fixture(scope="session", autouse=True)
def test_session_info(request):
    """Print test session information."""
    print(f"\n{'='*60}")
    print(f"{{{{project_name}}}} Enterprise Test Suite")
    print(f"{'='*60}")
    print(f"Test Session Started: {datetime.now().isoformat()}")
    print(f"Python Path: {sys.executable}")
    print(f"Working Directory: {os.getcwd()}")
    print(f"PySpark Available: {SPARK_AVAILABLE}")
    print(f"Databricks SDK Available: {DATABRICKS_SDK_AVAILABLE}")
    print(f"Project Modules Available: {MODULES_AVAILABLE}")
    print(f"{'='*60}\n")
    
    yield
    
    print(f"\n{'='*60}")
    print(f"Test Session Completed: {datetime.now().isoformat()}")
    print(f"{'='*60}")


# Export commonly used fixtures
__all__ = [
    "spark_session",
    "sample_customer_data", 
    "sample_transaction_data",
    "mock_databricks_client",
    "quality_rule_engine",
    "mock_secret_manager",
    "metrics_collector",
    "distributed_tracer",
    "alert_manager",
    "temp_directory",
    "test_data_generator"
]