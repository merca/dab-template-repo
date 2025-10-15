"""
Unit tests for the data quality module.

This module tests the core functionality of the data quality framework
including rule validation, quality metrics, and profiling capabilities.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import pandas as pd

from src.lib.data_quality import (
    DataQualityEngine,
    QualityRule,
    QualityMetrics,
    DataProfiler,
    QualityRuleType,
    QualityStatus
)


class TestQualityRule:
    """Test cases for QualityRule class."""
    
    def test_rule_creation(self):
        """Test creating a quality rule."""
        rule = QualityRule(
            name="completeness_check",
            rule_type=QualityRuleType.COMPLETENESS,
            column="customer_id",
            expression="customer_id IS NOT NULL",
            threshold=0.95,
            severity="HIGH"
        )
        
        assert rule.name == "completeness_check"
        assert rule.rule_type == QualityRuleType.COMPLETENESS
        assert rule.column == "customer_id"
        assert rule.threshold == 0.95
        assert rule.severity == "HIGH"
    
    def test_rule_validation(self):
        """Test rule validation logic."""
        rule = QualityRule(
            name="range_check",
            rule_type=QualityRuleType.VALIDITY,
            column="amount",
            expression="amount >= 0 AND amount <= 10000",
            threshold=0.98
        )
        
        # Mock DataFrame
        df_mock = Mock(spec=DataFrame)
        df_mock.count.return_value = 100
        df_mock.filter.return_value.count.return_value = 95
        
        result = rule.validate(df_mock)
        
        assert result.rule_name == "range_check"
        assert result.passed_count == 95
        assert result.total_count == 100
        assert result.success_rate == 0.95
        assert result.status == QualityStatus.FAILED  # Below 0.98 threshold


class TestDataProfiler:
    """Test cases for DataProfiler class."""
    
    def test_profiler_initialization(self):
        """Test DataProfiler initialization."""
        profiler = DataProfiler()
        assert profiler is not None
        assert hasattr(profiler, 'profile_dataset')
    
    @patch('src.lib.data_quality.DataProfiler._calculate_column_stats')
    def test_profile_dataset(self, mock_calc_stats, sample_customer_data):
        """Test dataset profiling functionality."""
        profiler = DataProfiler()
        
        # Mock column stats calculation
        mock_calc_stats.return_value = {
            'null_count': 0,
            'distinct_count': 100,
            'min_value': None,
            'max_value': None,
            'mean': None,
            'stddev': None
        }
        
        profile = profiler.profile_dataset(sample_customer_data, ['customer_id', 'name'])
        
        assert 'table_stats' in profile
        assert 'column_stats' in profile
        assert len(profile['column_stats']) == 2
        assert mock_calc_stats.call_count == 2


class TestDataQualityEngine:
    """Test cases for DataQualityEngine class."""
    
    def test_engine_initialization(self, spark_session):
        """Test DataQualityEngine initialization."""
        engine = DataQualityEngine(spark_session)
        assert engine.spark == spark_session
        assert engine.rules == []
        assert engine.metrics is not None
    
    def test_add_rule(self, spark_session):
        """Test adding quality rules to engine."""
        engine = DataQualityEngine(spark_session)
        
        rule = QualityRule(
            name="test_rule",
            rule_type=QualityRuleType.COMPLETENESS,
            column="test_col",
            expression="test_col IS NOT NULL",
            threshold=0.9
        )
        
        engine.add_rule(rule)
        assert len(engine.rules) == 1
        assert engine.rules[0].name == "test_rule"
    
    @patch('src.lib.data_quality.DataQualityEngine._execute_rule')
    def test_validate_dataset(self, mock_execute, spark_session, sample_customer_data):
        """Test dataset validation with rules."""
        engine = DataQualityEngine(spark_session)
        
        # Add test rules
        rule1 = QualityRule(
            name="completeness_check",
            rule_type=QualityRuleType.COMPLETENESS,
            column="customer_id",
            expression="customer_id IS NOT NULL",
            threshold=0.95
        )
        
        rule2 = QualityRule(
            name="validity_check",
            rule_type=QualityRuleType.VALIDITY,
            column="email",
            expression="email RLIKE '^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$'",
            threshold=0.90
        )
        
        engine.add_rule(rule1)
        engine.add_rule(rule2)
        
        # Mock rule execution results
        from src.lib.data_quality import ValidationResult
        mock_execute.side_effect = [
            ValidationResult("completeness_check", 95, 100, 0.95, QualityStatus.PASSED, ""),
            ValidationResult("validity_check", 88, 100, 0.88, QualityStatus.FAILED, "Below threshold")
        ]
        
        results = engine.validate_dataset(sample_customer_data, "test_table")
        
        assert len(results.rule_results) == 2
        assert results.overall_status == QualityStatus.FAILED  # One rule failed
        assert mock_execute.call_count == 2
    
    def test_generate_quality_report(self, spark_session):
        """Test quality report generation."""
        engine = DataQualityEngine(spark_session)
        
        # Mock validation results
        from src.lib.data_quality import ValidationResult, DatasetValidationResult
        rule_results = [
            ValidationResult("rule1", 95, 100, 0.95, QualityStatus.PASSED, ""),
            ValidationResult("rule2", 88, 100, 0.88, QualityStatus.FAILED, "Below threshold")
        ]
        
        dataset_result = DatasetValidationResult(
            dataset_name="test_table",
            rule_results=rule_results,
            overall_status=QualityStatus.FAILED,
            execution_time=1.5,
            timestamp="2024-01-01T00:00:00"
        )
        
        report = engine.generate_quality_report(dataset_result)
        
        assert 'summary' in report
        assert 'detailed_results' in report
        assert report['summary']['total_rules'] == 2
        assert report['summary']['passed_rules'] == 1
        assert report['summary']['failed_rules'] == 1


class TestQualityMetrics:
    """Test cases for QualityMetrics class."""
    
    def test_metrics_initialization(self):
        """Test QualityMetrics initialization."""
        metrics = QualityMetrics()
        assert metrics is not None
        assert hasattr(metrics, 'calculate_completeness')
        assert hasattr(metrics, 'calculate_validity')
    
    def test_calculate_completeness(self, spark_session):
        """Test completeness calculation."""
        metrics = QualityMetrics()
        
        # Create test DataFrame
        data = [("1", "John"), ("2", None), ("3", "Jane"), (None, "Bob")]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True)
        ])
        
        df = spark_session.createDataFrame(data, schema)
        
        # Test ID completeness (75% - 3/4 non-null)
        id_completeness = metrics.calculate_completeness(df, "id")
        assert id_completeness == 0.75
        
        # Test name completeness (75% - 3/4 non-null)
        name_completeness = metrics.calculate_completeness(df, "name")
        assert name_completeness == 0.75
    
    def test_calculate_validity(self, spark_session):
        """Test validity calculation."""
        metrics = QualityMetrics()
        
        # Create test DataFrame with email data
        data = [
            ("user1@example.com",),
            ("invalid-email",),
            ("user2@test.org",),
            ("another.invalid",)
        ]
        schema = StructType([StructField("email", StringType(), True)])
        df = spark_session.createDataFrame(data, schema)
        
        # Test email validity (50% - 2/4 valid emails)
        validity = metrics.calculate_validity(
            df, 
            "email", 
            "email RLIKE '^[A-Za-z0-9+_.-]+@([A-Za-z0-9.-]+\\.[A-Za-z]{2,})$'"
        )
        assert validity == 0.5
    
    def test_calculate_uniqueness(self, spark_session):
        """Test uniqueness calculation."""
        metrics = QualityMetrics()
        
        # Create test DataFrame with duplicate data
        data = [("1",), ("2",), ("1",), ("3",)]
        schema = StructType([StructField("id", StringType(), True)])
        df = spark_session.createDataFrame(data, schema)
        
        uniqueness = metrics.calculate_uniqueness(df, "id")
        assert uniqueness == 0.75  # 3 unique values out of 4 total


@pytest.mark.integration
class TestDataQualityIntegration:
    """Integration tests for data quality components."""
    
    def test_end_to_end_quality_validation(self, spark_session, sample_transaction_data):
        """Test complete data quality validation flow."""
        engine = DataQualityEngine(spark_session)
        
        # Add comprehensive rules
        rules = [
            QualityRule(
                name="transaction_id_completeness",
                rule_type=QualityRuleType.COMPLETENESS,
                column="transaction_id",
                expression="transaction_id IS NOT NULL",
                threshold=0.99,
                severity="CRITICAL"
            ),
            QualityRule(
                name="amount_validity",
                rule_type=QualityRuleType.VALIDITY,
                column="amount",
                expression="amount > 0",
                threshold=0.95,
                severity="HIGH"
            ),
            QualityRule(
                name="customer_id_uniqueness",
                rule_type=QualityRuleType.UNIQUENESS,
                column="customer_id",
                expression="",
                threshold=0.90,
                severity="MEDIUM"
            )
        ]
        
        for rule in rules:
            engine.add_rule(rule)
        
        # Execute validation
        results = engine.validate_dataset(sample_transaction_data, "transactions")
        
        assert results is not None
        assert len(results.rule_results) == 3
        assert results.dataset_name == "transactions"
        
        # Generate and validate report
        report = engine.generate_quality_report(results)
        assert 'summary' in report
        assert 'detailed_results' in report
        assert report['summary']['total_rules'] == 3


@pytest.mark.performance
class TestDataQualityPerformance:
    """Performance tests for data quality operations."""
    
    def test_large_dataset_validation_performance(self, spark_session, benchmark):
        """Test performance of quality validation on large datasets."""
        # Create large test dataset
        large_data = [(f"id_{i}", f"user_{i}", i * 10.0) for i in range(10000)]
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("name", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
        
        large_df = spark_session.createDataFrame(large_data, schema)
        
        engine = DataQualityEngine(spark_session)
        rule = QualityRule(
            name="performance_test",
            rule_type=QualityRuleType.COMPLETENESS,
            column="id",
            expression="id IS NOT NULL",
            threshold=0.95
        )
        engine.add_rule(rule)
        
        # Benchmark the validation
        result = benchmark(engine.validate_dataset, large_df, "large_test_table")
        
        assert result is not None
        assert len(result.rule_results) == 1
        assert result.rule_results[0].total_count == 10000


if __name__ == "__main__":
    pytest.main([__file__, "-v"])