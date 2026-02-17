# ============================================================================
# Chicago 311 Intelligence Platform - Terraform Outputs
# ============================================================================

output "resource_group_name" {
    description = "Name of the Azure resource group"
    value       = azurerm_resource_group.this.name
}

output "storage_account_name" {
    description = "Name of the ADLS Gen2 storage account"
    value       = azurerm_storage_account.data.name
}

output "storage_account_primary_dfs_endpoint" {
    description = "Primary DFS endpoint for ADLS Gen2 (abfss:// access)"
    value       = azurerm_storage_account.data.primary_dfs_endpoint
}

output "databricks_workspace_url" {
    description = "URL of the Azure Databricks workspace"
    value       = "https://${azurerm_databricks_workspace.this.workspace_url}"
}

output "databricks_workspace_id" {
    description = "Azure resource ID of the Databricks workspace"
    value       = azurerm_databricks_workspace.this.id
}

output "log_analytics_workspace_id" {
    description = "ID of the Log Analytics workspace"
    value       = azurerm_log_analytics_workspace.this.id
}

output "adls_connection_string" {
    description = "ABFSS connection string pattern for Spark"
    value       = "abfss://<container>@${azurerm_storage_account.data.name}.dfs.core.windows.net/"
}