# ============================================================================
# Chicago 311 Intelligence Platform - Azure Infrastructure
# ============================================================================
# Provisions: Resource Group, ADLS Gen2, Databricks Workspace, Monitor
# ============================================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
        source = "hashicorp/azurerm"
        version = "~> 3.80"
    }
    databricks = {
        source = "databricks/databricks"
        version = "~> 1.30"
    }
  }
}

# ============================================================================
# Providers
# ============================================================================

provider "azurerm" {
    features {}
    subscription_id =  var.azure_subscription_id
}

provider "databricks" {
    host                        = var.databricks_workspace_url
    azure_workspace_resource_id = var.databricks_workspace_resource_id
}

# ============================================================================
# Data Sources
# ============================================================================

data "azurerm_client_config" "current" {}

# ============================================================================
# Locals
# ============================================================================

locals {
    prefix = "chi311-${var.environment}"

    common_tags = merge(var.additional_tags, {
        Project     = "chi311-intelligence-platform"
        Environment = var.environment
        ManagedBy   = "terraform"
        Owner       = var.owner_email
    })
}

# ============================================================================
# Resource Group
# ============================================================================ 

data "azurerm_resource_group" "this" {
    name     = "DnA-Playground"
}

# ============================================================================
# Storage Account (ADLS Gen2 for Medallion Architecture)
# ============================================================================ 

resource "azurerm_storage_account" "data" {
    name                        = replace("${local.prefix}data", "-", "")
    resource_group_name         = data.azurerm_resource_group.this.name 
    location                    = data.azurerm_resource_group.this.location
    account_tier                = "Standard"
    account_replication_type    = "LRS"
    account_kind                = "StorageV2"
    is_hns_enabled              = true # Required for ADLS Gen2 / hierarchical namespace

    blob_properties {
        delete_retention_policy {
          days = var.environment == "prod" ? 30 : 7
        }
        container_delete_retention_policy {
          days = var.environment == "prod" ? 30 : 7
        }
    }  
    
    tags     = local.common_tags

    lifecycle {
        ignore_changes = [tags["CreatedOnDate"]]
    }
}

# Medallion Architecture containers
resource "azurerm_storage_container" "bronze" {
    name                  = "bronze"
    storage_account_name    = azurerm_storage_account.data.name
    container_access_type = "private"  
}

resource "azurerm_storage_container" "silver" {
    name                  = "silver"
    storage_account_name    = azurerm_storage_account.data.name
    container_access_type = "private"  
}

resource "azurerm_storage_container" "gold" {
    name                  = "gold"
    storage_account_name    = azurerm_storage_account.data.name
    container_access_type = "private"  
}

resource "azurerm_storage_container" "landing" {
    name                  = "landing"
    storage_account_name    = azurerm_storage_account.data.name
    container_access_type = "private"  
}

resource "azurerm_storage_container" "checkpoints" {
    name                 = "checkpoints"
    storage_account_name    = azurerm_storage_account.data.name
    container_access_type = "private"  
}

# ============================================================================
# Azure Databricks Workspace
# ============================================================================ 

resource "azurerm_databricks_workspace" "this" {
    name                                     = "${local.prefix}-dbw"
    resource_group_name                      = data.azurerm_resource_group.this.name
    location                                 = data.azurerm_resource_group.this.location
    sku                                      = var.databricks_sku
    managed_resource_group_name = "${local.prefix}-dbw-managed-rg"

    tags = local.common_tags

    lifecycle {
        ignore_changes = [tags["CreatedOnDate"]]
    }
}

# ============================================================================
# Databricks Secret Scope (for APIs tokens, etc.)
# ============================================================================ 

resource "databricks_secret_scope" "chi311" {
    count = var.socrata_app_token != "" ? 1 : 0
    name = "chi311-${var.environment}"
}

resource "databricks_secret" "socrata_app_token" {
    count           = var.socrata_app_token != "" ? 1 : 0 
    key             = "socrata-app-token"
    string_value    = var.socrata_app_token
    scope           = databricks_secret_scope.chi311[0].name 
}

# ============================================================================
# Azure Monitor - Log Analytics Workspace (for cost/usage monitoring)
# ============================================================================ 

resource "azurerm_log_analytics_workspace" "this" {
    name                = "${local.prefix}-logs"
    location            = data.azurerm_resource_group.this.location
    resource_group_name = data.azurerm_resource_group.this.name
    sku                 = "PerGB2018"
    retention_in_days   = 30

    tags = local.common_tags

    lifecycle {
        ignore_changes = [tags["CreatedOnDate"]]
    }
}

# Diagnostic settings for Databricks workspace
resource "azurerm_monitor_diagnostic_setting" "databricks" {
    name                        = "${local.prefix}-dbw-diag"
    target_resource_id          = azurerm_databricks_workspace.this.id
    log_analytics_workspace_id  = azurerm_log_analytics_workspace.this.id

    enabled_log {
        category = "dbfs"
    }

    enabled_log {
        category = "clusters"
    }

    enabled_log {
        category = "jobs"
    }
} 

# ============================================================================
# Budget Alert (Azure Cost Management)
# ============================================================================ 

resource "azurerm_consumption_budget_resource_group" "this" {
    name                  = "${local.prefix}-budget"
    resource_group_id     = data.azurerm_resource_group.this.id

    amount                = var.monthly_budget
    time_grain            = "Monthly"

    time_period {
      start_date = "2026-04-01T00:00:00Z"
      end_date   = "2026-06-30T00:00:00Z" 
    }

    notification {
      enabled   = true
      threshold = 80
      operator  = "GreaterThan"

      contact_emails = [var.alert_email] 
    }

    notification {
      enabled        = true
      threshold      = 100
      operator       = "GreaterThan"
      threshold_type = "Actual"

      contact_emails = [var.alert_email] 
    }
}

# ============================================================================
# Azure Monitor - Alert: Databricks Job Failures
# ============================================================================

resource "azurerm_monitor_action_group" "email" {
    name                = "${local.prefix}-alert-ag"
    resource_group_name = data.azurerm_resource_group.this.name
    short_name          = "chi311-ag"

    email_receiver {
        name          = "owner"
        email_address = var.alert_email
    }
}

resource "azurerm_monitor_scheduled_query_rules_alert_v2" "job_failures" {
    name                = "${local.prefix}-dbw-job-failures"
    resource_group_name = data.azurerm_resource_group.this.name
    location            = data.azurerm_resource_group.this.location

    description = "Alert when any Databricks job run fails"
    severity    = 2
    enabled     = true

    scopes                  = [azurerm_log_analytics_workspace.this.id]
    evaluation_frequency    = "PT15M"
    window_duration         = "PT15M"

    criteria {
        query = <<-QUERY
            DatabricksJobs
            | where TimeGenerated > ago(15m)
            | where ActionName == "runFailed"
            | summarize FailedRuns = count() by bin(TimeGenerated, 15m)
        QUERY

        time_aggregation_method = "Count"
        threshold               = 0
        operator                = "GreaterThan"

        failing_periods {
            minimum_failing_periods_to_trigger_alert = 1
            number_of_evaluation_periods             = 1
        }
    }

    action {
        action_groups = [azurerm_monitor_action_group.email.id]
    }

    tags = local.common_tags
}

