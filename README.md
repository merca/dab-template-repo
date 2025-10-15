# Enterprise Databricks Asset Bundle Template

This repository contains an enterprise-ready Databricks Asset Bundle (DAB) template for creating robust, scalable Databricks projects with enterprise features.

## Features

This template provides a complete development environment for new Databricks projects, including:

- **Enterprise Architecture**: Pre-configured medallion architecture (Bronze/Silver/Gold)
- **CI/CD Pipelines**: Complete pipelines for GitHub Actions, Azure DevOps, and GitLab
- **Security & Governance**: Built-in security features and compliance configurations
- **Data Quality**: Integrated data quality framework with monitoring
- **Unity Catalog**: Full Unity Catalog integration and configuration
- **Development Environment**: DevContainer and WSL support for consistent development
- **Monitoring & Alerting**: Enterprise monitoring and alerting capabilities

## Getting Started

### Prerequisites

Install the Databricks CLI:

```bash
# macOS
brew tap databricks/tap
brew install databricks

# Windows
winget install Databricks.CLI

# Linux
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sh
```

Make sure to configure your Databricks profile in `.databrickscfg`.

### Initialize a New Project

Initialize a new project using this template:

```bash
databricks bundle init https://github.com/your-org/dab-template-repo
```

When initializing your project, you'll be prompted to answer several configuration questions:

| Parameter | Description | Example |
|-----------|-------------|---------|
| `project_name` | Name of the project (usually the same as the repository name) | `my-databricks-project` |
| `author` | Name of the author or team lead | `John Doe` |
| `email` | Email address of the author or team contact | `john.doe@company.com` |
| `organization` | Organization or company name | `ACME Corp` |
| `project_description` | Brief description of the project | `Data processing pipeline for customer analytics` |
| `cicd_provider` | CI/CD provider | `github`/`azure`/`gitlab` |
| `cloud_provider` | Cloud provider | `azure`/`aws`/`gcp` |
| `include_example_jobs` | Whether to include example pipelines and jobs | `true`/`false` |
| `include_unity_catalog` | Whether to include Unity Catalog configuration | `true`/`false` |
| `include_data_quality` | Whether to include data quality framework | `true`/`false` |
| `include_monitoring` | Whether to include enterprise monitoring and alerting | `true`/`false` |
| `include_security` | Whether to include enhanced security features | `true`/`false` |
| `include_governance` | Whether to include governance and compliance features | `true`/`false` |
| `environments` | List of deployment environments | `["dev", "staging", "prod"]` |
| `medallion_architecture` | Whether to implement medallion architecture | `true`/`false` |
| `include_devcontainer` | Whether to include DevContainer configuration | `true`/`false` |
| `python_version` | Python version for development and runtime | `3.11` |

### Post-Initialization Setup

After initializing your project:

1. **Configure Authentication**: Set up your Databricks authentication
2. **Environment Setup**: Configure your development environments
3. **Deploy Resources**: Run `databricks bundle deploy --target dev`
4. **Run Workflows**: Test your deployment with `databricks bundle run --target dev`

## Template Structure

```
template/
├── .devcontainer/          # Development container configuration
├── .github/                # GitHub Actions workflows
├── configs/                # Environment-specific configurations
├── src/                    # Source code and notebooks
├── tests/                  # Test files
├── databricks.yml.tmpl     # Main bundle configuration template
└── pyproject.toml.tmpl     # Python project configuration template
```

## Customization

This template is designed to be flexible and customizable. You can:

- Modify the template files to fit your organization's standards
- Add or remove optional components based on your needs
- Extend the schema to include additional configuration options
- Customize the CI/CD workflows for your specific requirements

## Contributing

Contributions are welcome! Please read our [Contributing Guidelines](CONTRIBUTING.md) for details on how to submit pull requests, report issues, and contribute to the project.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.