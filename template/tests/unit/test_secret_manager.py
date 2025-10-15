"""
Unit tests for the secret manager module.

This module tests the core functionality of the enterprise secret management
system including multi-provider support, caching, and audit logging.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, call
import json
import time
from datetime import datetime, timedelta

from src.lib.secret_manager import (
    SecretManager,
    DatabricksSecretsProvider,
    AzureKeyVaultProvider,
    AWSSecretsManagerProvider,
    EnvironmentVariableProvider,
    SecretNotFoundError,
    SecretProviderError,
    SecretAuditLogger
)


class TestSecretManager:
    """Test cases for SecretManager class."""
    
    def test_manager_initialization(self):
        """Test SecretManager initialization."""
        manager = SecretManager()
        assert manager is not None
        assert len(manager.providers) == 0
        assert manager.cache_ttl == 300  # Default 5 minutes
        assert manager.audit_logger is not None
    
    def test_add_provider(self):
        """Test adding secret providers."""
        manager = SecretManager()
        provider = Mock()
        provider.name = "test_provider"
        
        manager.add_provider(provider)
        assert len(manager.providers) == 1
        assert manager.providers[0] == provider
    
    @patch('src.lib.secret_manager.SecretManager._get_from_cache')
    @patch('src.lib.secret_manager.SecretManager._store_in_cache')
    def test_get_secret_with_cache_hit(self, mock_store, mock_get):
        """Test retrieving secret with cache hit."""
        manager = SecretManager(enable_cache=True)
        
        # Mock cache hit
        mock_get.return_value = "cached_secret_value"
        
        result = manager.get_secret("test_key")
        
        assert result == "cached_secret_value"
        mock_get.assert_called_once_with("test_key")
        mock_store.assert_not_called()
    
    @patch('src.lib.secret_manager.SecretManager._get_from_cache')
    @patch('src.lib.secret_manager.SecretManager._store_in_cache')
    def test_get_secret_with_cache_miss(self, mock_store, mock_get):
        """Test retrieving secret with cache miss."""
        manager = SecretManager(enable_cache=True)
        
        # Mock cache miss
        mock_get.return_value = None
        
        # Mock provider
        provider = Mock()
        provider.name = "test_provider"
        provider.get_secret.return_value = "provider_secret_value"
        manager.add_provider(provider)
        
        result = manager.get_secret("test_key")
        
        assert result == "provider_secret_value"
        mock_get.assert_called_once_with("test_key")
        provider.get_secret.assert_called_once_with("test_key")
        mock_store.assert_called_once_with("test_key", "provider_secret_value")
    
    def test_get_secret_not_found(self):
        """Test retrieving non-existent secret."""
        manager = SecretManager()
        
        # Mock provider that raises SecretNotFoundError
        provider = Mock()
        provider.name = "test_provider"
        provider.get_secret.side_effect = SecretNotFoundError("Secret not found")
        manager.add_provider(provider)
        
        with pytest.raises(SecretNotFoundError):
            manager.get_secret("non_existent_key")
    
    def test_get_secret_provider_fallback(self):
        """Test fallback between providers."""
        manager = SecretManager()
        
        # First provider fails
        provider1 = Mock()
        provider1.name = "provider1"
        provider1.get_secret.side_effect = SecretNotFoundError("Not found in provider1")
        
        # Second provider succeeds
        provider2 = Mock()
        provider2.name = "provider2"
        provider2.get_secret.return_value = "secret_from_provider2"
        
        manager.add_provider(provider1)
        manager.add_provider(provider2)
        
        result = manager.get_secret("test_key")
        
        assert result == "secret_from_provider2"
        provider1.get_secret.assert_called_once_with("test_key")
        provider2.get_secret.assert_called_once_with("test_key")
    
    def test_get_secrets_bulk(self):
        """Test bulk secret retrieval."""
        manager = SecretManager()
        
        provider = Mock()
        provider.name = "test_provider"
        provider.get_secret.side_effect = lambda key: f"value_for_{key}"
        manager.add_provider(provider)
        
        keys = ["key1", "key2", "key3"]
        results = manager.get_secrets(keys)
        
        expected = {"key1": "value_for_key1", "key2": "value_for_key2", "key3": "value_for_key3"}
        assert results == expected
        assert provider.get_secret.call_count == 3
    
    def test_refresh_secret(self):
        """Test secret refresh functionality."""
        manager = SecretManager(enable_cache=True)
        
        with patch.object(manager, '_remove_from_cache') as mock_remove:
            with patch.object(manager, 'get_secret') as mock_get:
                mock_get.return_value = "refreshed_value"
                
                result = manager.refresh_secret("test_key")
                
                assert result == "refreshed_value"
                mock_remove.assert_called_once_with("test_key")
                mock_get.assert_called_once_with("test_key")


class TestDatabricksSecretsProvider:
    """Test cases for DatabricksSecretsProvider."""
    
    @patch('src.lib.secret_manager.WorkspaceClient')
    def test_provider_initialization(self, mock_client):
        """Test DatabricksSecretsProvider initialization."""
        provider = DatabricksSecretsProvider()
        assert provider.name == "databricks"
        assert provider.client is not None
    
    @patch('src.lib.secret_manager.WorkspaceClient')
    def test_get_secret_success(self, mock_client):
        """Test successful secret retrieval."""
        # Mock the client and its methods
        mock_client_instance = Mock()
        mock_client.return_value = mock_client_instance
        
        mock_secret_value = Mock()
        mock_secret_value.value = "secret_value"
        mock_client_instance.secrets.get_secret.return_value = mock_secret_value
        
        provider = DatabricksSecretsProvider()
        result = provider.get_secret("test_scope/test_key")
        
        assert result == "secret_value"
        mock_client_instance.secrets.get_secret.assert_called_once_with(
            scope="test_scope", 
            key="test_key"
        )
    
    @patch('src.lib.secret_manager.WorkspaceClient')
    def test_get_secret_not_found(self, mock_client):
        """Test secret not found scenario."""
        mock_client_instance = Mock()
        mock_client.return_value = mock_client_instance
        
        # Mock exception from Databricks SDK
        from databricks.sdk.errors import NotFound
        mock_client_instance.secrets.get_secret.side_effect = NotFound("Secret not found")
        
        provider = DatabricksSecretsProvider()
        
        with pytest.raises(SecretNotFoundError):
            provider.get_secret("test_scope/test_key")
    
    def test_parse_secret_path(self):
        """Test secret path parsing."""
        provider = DatabricksSecretsProvider()
        
        # Test valid path
        scope, key = provider._parse_secret_path("my_scope/my_key")
        assert scope == "my_scope"
        assert key == "my_key"
        
        # Test invalid path
        with pytest.raises(ValueError):
            provider._parse_secret_path("invalid_path")


class TestAzureKeyVaultProvider:
    """Test cases for AzureKeyVaultProvider."""
    
    @patch('src.lib.secret_manager.SecretClient')
    def test_provider_initialization(self, mock_client_class):
        """Test AzureKeyVaultProvider initialization."""
        provider = AzureKeyVaultProvider("https://test-vault.vault.azure.net/")
        assert provider.name == "azure_keyvault"
        assert provider.vault_url == "https://test-vault.vault.azure.net/"
        mock_client_class.assert_called_once()
    
    @patch('src.lib.secret_manager.SecretClient')
    def test_get_secret_success(self, mock_client_class):
        """Test successful secret retrieval from Azure Key Vault."""
        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance
        
        mock_secret = Mock()
        mock_secret.value = "azure_secret_value"
        mock_client_instance.get_secret.return_value = mock_secret
        
        provider = AzureKeyVaultProvider("https://test-vault.vault.azure.net/")
        result = provider.get_secret("test-secret")
        
        assert result == "azure_secret_value"
        mock_client_instance.get_secret.assert_called_once_with("test-secret")
    
    @patch('src.lib.secret_manager.SecretClient')
    def test_get_secret_not_found(self, mock_client_class):
        """Test secret not found in Azure Key Vault."""
        mock_client_instance = Mock()
        mock_client_class.return_value = mock_client_instance
        
        # Mock exception from Azure SDK
        from azure.core.exceptions import ResourceNotFoundError
        mock_client_instance.get_secret.side_effect = ResourceNotFoundError("Secret not found")
        
        provider = AzureKeyVaultProvider("https://test-vault.vault.azure.net/")
        
        with pytest.raises(SecretNotFoundError):
            provider.get_secret("non-existent-secret")


class TestAWSSecretsManagerProvider:
    """Test cases for AWSSecretsManagerProvider."""
    
    @patch('src.lib.secret_manager.boto3.client')
    def test_provider_initialization(self, mock_boto_client):
        """Test AWSSecretsManagerProvider initialization."""
        provider = AWSSecretsManagerProvider(region_name="us-east-1")
        assert provider.name == "aws_secrets_manager"
        assert provider.region_name == "us-east-1"
        mock_boto_client.assert_called_once_with('secretsmanager', region_name="us-east-1")
    
    @patch('src.lib.secret_manager.boto3.client')
    def test_get_secret_success(self, mock_boto_client):
        """Test successful secret retrieval from AWS Secrets Manager."""
        mock_client_instance = Mock()
        mock_boto_client.return_value = mock_client_instance
        
        mock_response = {
            'SecretString': json.dumps({'key': 'aws_secret_value'})
        }
        mock_client_instance.get_secret_value.return_value = mock_response
        
        provider = AWSSecretsManagerProvider()
        result = provider.get_secret("test-secret")
        
        assert result == {'key': 'aws_secret_value'}
        mock_client_instance.get_secret_value.assert_called_once_with(SecretId="test-secret")
    
    @patch('src.lib.secret_manager.boto3.client')
    def test_get_secret_string_value(self, mock_boto_client):
        """Test secret retrieval with plain string value."""
        mock_client_instance = Mock()
        mock_boto_client.return_value = mock_client_instance
        
        mock_response = {
            'SecretString': 'plain_string_secret'
        }
        mock_client_instance.get_secret_value.return_value = mock_response
        
        provider = AWSSecretsManagerProvider()
        result = provider.get_secret("test-secret")
        
        assert result == "plain_string_secret"
    
    @patch('src.lib.secret_manager.boto3.client')
    def test_get_secret_not_found(self, mock_boto_client):
        """Test secret not found in AWS Secrets Manager."""
        mock_client_instance = Mock()
        mock_boto_client.return_value = mock_client_instance
        
        # Mock exception from AWS SDK
        from botocore.exceptions import ClientError
        error_response = {'Error': {'Code': 'ResourceNotFoundException'}}
        mock_client_instance.get_secret_value.side_effect = ClientError(
            error_response, 'GetSecretValue'
        )
        
        provider = AWSSecretsManagerProvider()
        
        with pytest.raises(SecretNotFoundError):
            provider.get_secret("non-existent-secret")


class TestEnvironmentVariableProvider:
    """Test cases for EnvironmentVariableProvider."""
    
    def test_provider_initialization(self):
        """Test EnvironmentVariableProvider initialization."""
        provider = EnvironmentVariableProvider()
        assert provider.name == "environment"
    
    @patch('os.getenv')
    def test_get_secret_success(self, mock_getenv):
        """Test successful secret retrieval from environment variables."""
        mock_getenv.return_value = "env_secret_value"
        
        provider = EnvironmentVariableProvider()
        result = provider.get_secret("TEST_SECRET")
        
        assert result == "env_secret_value"
        mock_getenv.assert_called_once_with("TEST_SECRET")
    
    @patch('os.getenv')
    def test_get_secret_not_found(self, mock_getenv):
        """Test secret not found in environment variables."""
        mock_getenv.return_value = None
        
        provider = EnvironmentVariableProvider()
        
        with pytest.raises(SecretNotFoundError):
            provider.get_secret("NON_EXISTENT_SECRET")
    
    @patch('os.getenv')
    def test_get_secret_with_prefix(self, mock_getenv):
        """Test secret retrieval with prefix."""
        mock_getenv.return_value = "prefixed_secret_value"
        
        provider = EnvironmentVariableProvider(prefix="MYAPP_")
        result = provider.get_secret("SECRET_KEY")
        
        assert result == "prefixed_secret_value"
        mock_getenv.assert_called_once_with("MYAPP_SECRET_KEY")


class TestSecretAuditLogger:
    """Test cases for SecretAuditLogger."""
    
    def test_logger_initialization(self):
        """Test SecretAuditLogger initialization."""
        logger = SecretAuditLogger()
        assert logger is not None
        assert hasattr(logger, 'log_access')
        assert hasattr(logger, 'log_error')
    
    def test_log_access(self):
        """Test logging secret access events."""
        logger = SecretAuditLogger()
        
        with patch.object(logger.logger, 'info') as mock_info:
            logger.log_access("test_key", "test_provider", "user123")
            
            mock_info.assert_called_once()
            call_args = mock_info.call_args[0][0]
            assert "test_key" in call_args
            assert "test_provider" in call_args
            assert "user123" in call_args
    
    def test_log_error(self):
        """Test logging secret access errors."""
        logger = SecretAuditLogger()
        
        with patch.object(logger.logger, 'error') as mock_error:
            logger.log_error("test_key", "test_provider", "Secret not found", "user123")
            
            mock_error.assert_called_once()
            call_args = mock_error.call_args[0][0]
            assert "test_key" in call_args
            assert "test_provider" in call_args
            assert "Secret not found" in call_args
            assert "user123" in call_args
    
    def test_get_access_statistics(self):
        """Test retrieving access statistics."""
        logger = SecretAuditLogger()
        
        # Simulate some access events
        logger.log_access("key1", "provider1", "user1")
        logger.log_access("key2", "provider1", "user1")
        logger.log_access("key1", "provider2", "user2")
        
        stats = logger.get_access_statistics()
        
        assert 'total_accesses' in stats
        assert 'unique_keys' in stats
        assert 'providers' in stats
        assert 'users' in stats


@pytest.mark.integration
class TestSecretManagerIntegration:
    """Integration tests for secret manager components."""
    
    @patch.dict('os.environ', {'TEST_SECRET': 'env_value'})
    def test_multi_provider_fallback(self):
        """Test multi-provider fallback scenario."""
        manager = SecretManager()
        
        # Add providers in priority order
        databricks_provider = Mock()
        databricks_provider.name = "databricks"
        databricks_provider.get_secret.side_effect = SecretNotFoundError("Not in Databricks")
        
        env_provider = EnvironmentVariableProvider()
        
        manager.add_provider(databricks_provider)
        manager.add_provider(env_provider)
        
        # Should fallback to environment variable
        result = manager.get_secret("TEST_SECRET")
        assert result == "env_value"
    
    def test_caching_behavior(self):
        """Test caching behavior across multiple requests."""
        manager = SecretManager(enable_cache=True, cache_ttl=1)
        
        provider = Mock()
        provider.name = "test_provider"
        provider.get_secret.return_value = "cached_value"
        manager.add_provider(provider)
        
        # First call should hit provider
        result1 = manager.get_secret("test_key")
        assert result1 == "cached_value"
        assert provider.get_secret.call_count == 1
        
        # Second call should use cache
        result2 = manager.get_secret("test_key")
        assert result2 == "cached_value"
        assert provider.get_secret.call_count == 1  # No additional call
        
        # Wait for cache to expire
        time.sleep(1.1)
        
        # Third call should hit provider again
        result3 = manager.get_secret("test_key")
        assert result3 == "cached_value"
        assert provider.get_secret.call_count == 2


@pytest.mark.performance
class TestSecretManagerPerformance:
    """Performance tests for secret manager operations."""
    
    def test_bulk_secret_retrieval_performance(self, benchmark):
        """Test performance of bulk secret retrieval."""
        manager = SecretManager()
        
        provider = Mock()
        provider.name = "test_provider"
        provider.get_secret.side_effect = lambda key: f"value_{key}"
        manager.add_provider(provider)
        
        keys = [f"key_{i}" for i in range(100)]
        
        result = benchmark(manager.get_secrets, keys)
        
        assert len(result) == 100
        assert all(f"value_key_{i}" == result[f"key_{i}"] for i in range(100))
    
    def test_cache_performance(self, benchmark):
        """Test cache performance benefits."""
        manager = SecretManager(enable_cache=True)
        
        provider = Mock()
        provider.name = "test_provider"
        provider.get_secret.return_value = "cached_value"
        manager.add_provider(provider)
        
        # Warm up the cache
        manager.get_secret("test_key")
        
        # Benchmark cached retrieval
        result = benchmark(manager.get_secret, "test_key")
        
        assert result == "cached_value"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])