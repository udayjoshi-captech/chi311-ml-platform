# ============================================================================
# Chicago 311 Intelligence Platform - Azure Terraform Variables
# ============================================================================

# ============================================================================
# Azure Configuration
# ============================================================================

variable "azure_subscription_id" {
    description = "Azure subscription ID"
    type        = string
}

variable "azure_region" {
    description = "Azure region for all resources"
    type        = string
    default     = "eastus"
}

variable "environment" {
    description = "Environment name (dev, qa, prod)"
    type        = string
    default     = "dev"

    validation {
        condition       = contains(["dev", "qa", "prod"], var.environment)
        error_message   = "Environment must be dev, qa, or prod."
    }
}

# ============================================================================
# Databricks Configuration
# ============================================================================

variable "databricks_sku" {
    description = "Databricks workspace SKU (standard or premium)"
    type        = string
    default     = "premium"

    validation {
        condition       = contains(["standard", "premium"], car.databricks_sku)
        error_message   = "Databricks SKU must be standard or premium"
    }
}

# ============================================================================
# Databricks Configuration
# ============================================================================

variable "databricks_sku" {
    description = "Databricks workspace SKU (standard or premium)"
    type        = string
    default     = "premium"

    validation {
        condition       = contains(["standard", "premium"], car.databricks_sku)
        error_message   = "Databricks SKU must be standard or premium"
    }
}

# ============================================================================
# API Credentials
# ============================================================================

variable "socrata_app_token" {
    description = "Socrata API app token for Chicago Open Data Portal"
    type        = string
    sensitive   = true
    default     = ""
}

# ============================================================================
# Contacts & Alerts
# ============================================================================

variable "owner_email" {
    description = "Email of the project owner"
    type        = string
    sensitive   = true
    default     = "ujoshi@captechconsulting.com"
}

variable "alert_email" {
    description = "Email for budget and cost alerts"
    type        = string
    sensitive   = true
    default     = "ujoshi@captechconsulting.com"
}

# ============================================================================
# Budget
# ============================================================================

variable "monthly_budget" {
    description = "Monthly budget in USD for the resource group"
    type        = number
    default     = 25
}

# ============================================================================
# Tags
# ============================================================================

variable "additional_tags" {
    description = "Additional tages to apply to all resources"
    type        = map(string)
    default     = {}
}