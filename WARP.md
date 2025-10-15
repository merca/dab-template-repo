# WARP.md

This file provides guidance to WARP (warp.dev) when working with code in this repository.

## Project Overview

This is a **Databricks Asset Bundle (DAB) template repository** for creating enterprise-ready Databricks projects. It provides a complete template that generates new Databricks projects with configurable enterprise features including medallion architecture, CI/CD pipelines, security, governance, and monitoring.

## Common Commands

### Template Initialization and Development
```bash
# Initialize a new project from this template
databricks bundle init https://github.com/your-org/dab-template-repo

# Template development/validation (when working on the template itself)
databricks bundle validate --var-file template/configs/dev.yml
```

### Generated Project Commands (after initialization)
```bash
# Validate bundle configuration
databricks bundle validate --target dev
databricks bundle validate --target staging  
databricks bundle validate --target prod

# Deploy to environments
databricks bundle deploy --target dev
databricks bundle deploy --target staging --force-lock
databricks bundle deploy --target prod --force-lock

# Run specific jobs/pipelines
databricks bundle run --target dev job_name
databricks bundle run --target dev bronze_pipeline
databricks bundle run --target dev silver_pipeline
databricks bundle run --target dev gold_pipeline
databricks bundle run --target dev data_quality_job
databricks bundle run --target dev monitoring_job
```

### Python Development (for generated projects)
```bash
# Install dependencies with UV (preferred)
uv venv --python 3.11
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows
uv pip install -r requirements.txt
uv pip install -r requirements-dev.txt

# Code quality checks
ruff check src/
ruff format src/
mypy src/ --config-file pyproject.toml
pre-commit run --all-files

# Testing
pytest tests/unit/ --cov=src/ --cov-report=term-missing
pytest tests/integration/
pytest tests/ -m "not slow"  # Skip slow tests
```

## Architecture

### Template Structure
- **`template/`** - Contains all template files with Go template syntax (`.tmpl` files)
- **`databricks_template_schema.json`** - JSON schema defining configuration parameters for template initialization
- **`template/configs/`** - Legacy config files (deprecated, kept for reference)

### Generated Project Architecture
The template generates projects with:

1. **Medallion Architecture (Bronze/Silver/Gold)** - Configurable data lake architecture
2. **Environment-based Configuration** - Separate targets for dev/staging/prod with different resource allocation
3. **Serverless Compute** - Uses pure serverless compute instead of traditional clusters
4. **Unity Catalog Integration** - Full Unity Catalog schema and permissions management
5. **Enterprise Features** - Security, governance, monitoring, and data quality frameworks

### Key Template Variables
- `project_name` - Project identifier (becomes package name)
- `cicd_provider` - github/azure/gitlab for CI/CD workflows
- `cloud_provider` - azure/aws/gcp for cloud-specific configurations
- `include_*` flags - Toggle enterprise features (unity_catalog, data_quality, monitoring, security, governance)
- `environments` - List of deployment environments
- `medallion_architecture` - Enable bronze/silver/gold pattern

### Configuration Management
- **Main Config**: `databricks.yml.tmpl` uses YAML anchors and variables for organization
- **Environment Targets**: All environment-specific settings are consolidated in the main file under `targets:`
- **Resource Overrides**: Production environments get enhanced configurations (schedules, retries, notifications)

## Development Guidelines

### Template Development
- Use Go template syntax for conditional logic: `{{- if .condition }}...{{- end }}`
- Test template changes by initializing new projects with different configurations
- Validate generated `databricks.yml` files across all environment combinations
- Update schema file when adding new configuration options

### Code Quality Standards
- Use Ruff for linting and formatting (replaces Black + isort + flake8)
- MyPy for type checking with strict configuration
- Pre-commit hooks enforce code quality
- Maintain 80%+ test coverage
- Follow Python 3.11+ features and syntax

### Bundle Configuration Best Practices
- Use variables for environment-specific values instead of hardcoding
- Organize complex configurations with YAML anchors
- Prefer serverless compute over traditional clusters
- Include comprehensive tagging for resource management
- Use service principals for non-dev environments

### Security and Governance
- Never hardcode secrets or credentials in templates
- Use Databricks secret scopes for sensitive configuration
- Include proper IAM/RBAC patterns in generated projects
- Enable audit logging and monitoring by default
- Follow least-privilege access principles

## Testing Strategy

### Template Testing
- Validate schema against various configuration combinations
- Test template rendering with different parameter sets
- Ensure generated bundles validate successfully
- Verify CI/CD workflows work across cloud providers

### Generated Project Testing
- **Unit Tests**: Test individual functions and modules
- **Integration Tests**: Test Databricks job execution and data processing
- **End-to-end Tests**: Full pipeline validation across environments
- Use pytest markers: `@pytest.mark.unit`, `@pytest.mark.integration`, `@pytest.mark.slow`

## CI/CD Integration

### Supported Providers
- **GitHub Actions**: `.github/workflows/ci-cd.yml.tmpl`
- **Azure DevOps**: Template generates Azure Pipelines configuration
- **GitLab**: Template generates GitLab CI configuration

### Pipeline Stages
1. **Quality Checks**: Linting, type checking, unit tests
2. **Bundle Validation**: Validate Databricks bundle across environments
3. **Development Deployment**: Auto-deploy to dev on develop branch
4. **Staging Deployment**: Deploy to staging on main branch (with approval)
5. **Production Deployment**: Deploy to prod on main branch (with approval and integration tests)

### Environment Variables Required
- `DATABRICKS_HOST_*` - Workspace URLs per environment
- `DATABRICKS_CLIENT_ID_*` / `DATABRICKS_CLIENT_SECRET_*` (Azure) or `DATABRICKS_TOKEN_*` (AWS/GCP)
- Optional: `CODECOV_TOKEN`, `SLACK_WEBHOOK_URL`