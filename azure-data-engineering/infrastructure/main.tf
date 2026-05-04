# ==============================================================================
# infrastructure/main.tf
# Azure Data Engineering Platform — Terraform IaC
# ==============================================================================

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
    azuread = {
      source  = "hashicorp/azuread"
      version = "~> 2.46"
    }
  }
  backend "azurerm" {
    resource_group_name  = "rg-terraform-state"
    storage_account_name = "sttfstatedataeng"
    container_name       = "tfstate"
    key                  = "data-engineering.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}

# ==============================================================================
# Variables
# ==============================================================================

variable "environment" {
  type    = string
  default = "prod"
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod"
  }
}

variable "location" {
  type    = string
  default = "eastus2"
}

variable "alert_email" {
  type = string
}

locals {
  prefix = "de-${var.environment}"
  tags = {
    Environment = var.environment
    Project     = "DataEngineering"
    ManagedBy   = "Terraform"
    CreatedAt   = timestamp()
  }
}

# ==============================================================================
# Resource Group
# ==============================================================================

resource "azurerm_resource_group" "main" {
  name     = "rg-${local.prefix}"
  location = var.location
  tags     = local.tags
}

# ==============================================================================
# Azure Data Lake Storage Gen2
# ==============================================================================

resource "azurerm_storage_account" "adls" {
  name                     = replace("st${local.prefix}adls", "-", "")
  resource_group_name      = azurerm_resource_group.main.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "GRS"
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # HNS = hierarchical namespace = ADLS Gen2

  blob_properties {
    versioning_enabled = true
    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 7
    }
  }

  network_rules {
    default_action = "Deny"
    bypass         = ["AzureServices"]
  }

  tags = local.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "bronze" {
  name               = "bronze"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "silver" {
  name               = "silver"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "gold" {
  name               = "gold"
  storage_account_id = azurerm_storage_account.adls.id
}

resource "azurerm_storage_data_lake_gen2_filesystem" "archive" {
  name               = "archive"
  storage_account_id = azurerm_storage_account.adls.id
}

# ==============================================================================
# Azure Key Vault
# ==============================================================================

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "main" {
  name                       = "kv-${local.prefix}-de"
  location                   = var.location
  resource_group_name        = azurerm_resource_group.main.name
  sku_name                   = "standard"
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  soft_delete_retention_days = 90
  purge_protection_enabled   = true

  network_acls {
    bypass         = "AzureServices"
    default_action = "Deny"
  }

  tags = local.tags
}

# ==============================================================================
# Azure Event Hubs
# ==============================================================================

resource "azurerm_eventhub_namespace" "main" {
  name                = "evhns-${local.prefix}-de"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "Standard"
  capacity            = 4
  auto_inflate_enabled     = true
  maximum_throughput_units = 20
  tags                = local.tags
}

resource "azurerm_eventhub" "transactions" {
  name                = "eh-transactions"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 16
  message_retention   = 7
}

resource "azurerm_eventhub" "clickstream" {
  name                = "eh-clickstream"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 32
  message_retention   = 3
}

resource "azurerm_eventhub" "iot_telemetry" {
  name                = "eh-iot-telemetry"
  namespace_name      = azurerm_eventhub_namespace.main.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 8
  message_retention   = 1
}

# ==============================================================================
# Azure Databricks
# ==============================================================================

resource "azurerm_databricks_workspace" "main" {
  name                = "dbw-${local.prefix}-de"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "premium"

  custom_parameters {
    no_public_ip             = true
    virtual_network_id       = azurerm_virtual_network.main.id
    private_subnet_name      = azurerm_subnet.databricks_private.name
    public_subnet_name       = azurerm_subnet.databricks_public.name
  }

  tags = local.tags
}

# ==============================================================================
# Azure Synapse Analytics
# ==============================================================================

resource "azurerm_synapse_workspace" "main" {
  name                                 = "syn-${local.prefix}-de"
  resource_group_name                  = azurerm_resource_group.main.name
  location                             = var.location
  storage_data_lake_gen2_filesystem_id = azurerm_storage_data_lake_gen2_filesystem.gold.id
  sql_administrator_login              = "sqladmin"
  sql_administrator_login_password     = random_password.synapse_admin.result

  identity {
    type = "SystemAssigned"
  }

  tags = local.tags
}

resource "azurerm_synapse_spark_pool" "main" {
  name                 = "sppool01"
  synapse_workspace_id = azurerm_synapse_workspace.main.id
  node_size_family     = "MemoryOptimized"
  node_size            = "Medium"

  auto_scale {
    max_node_count = 20
    min_node_count = 3
  }

  auto_pause {
    delay_in_minutes = 15
  }

  spark_version = "3.4"
  tags          = local.tags
}

# ==============================================================================
# Log Analytics & Azure Monitor
# ==============================================================================

resource "azurerm_log_analytics_workspace" "main" {
  name                = "law-${local.prefix}-de"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  sku                 = "PerGB2018"
  retention_in_days   = 90
  tags                = local.tags
}

resource "azurerm_monitor_action_group" "alerts" {
  name                = "ag-${local.prefix}-de-alerts"
  resource_group_name = azurerm_resource_group.main.name
  short_name          = "de-alerts"

  email_receiver {
    name          = "data-engineering-team"
    email_address = var.alert_email
  }

  tags = local.tags
}

# ==============================================================================
# Networking (VNet for Databricks private deployment)
# ==============================================================================

resource "azurerm_virtual_network" "main" {
  name                = "vnet-${local.prefix}-de"
  location            = var.location
  resource_group_name = azurerm_resource_group.main.name
  address_space       = ["10.0.0.0/16"]
  tags                = local.tags
}

resource "azurerm_subnet" "databricks_public" {
  name                 = "snet-databricks-public"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.1.0/24"]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
    }
  }
}

resource "azurerm_subnet" "databricks_private" {
  name                 = "snet-databricks-private"
  resource_group_name  = azurerm_resource_group.main.name
  virtual_network_name = azurerm_virtual_network.main.name
  address_prefixes     = ["10.0.2.0/24"]

  delegation {
    name = "databricks-delegation"
    service_delegation {
      name    = "Microsoft.Databricks/workspaces"
      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
    }
  }
}

# ==============================================================================
# Random password (Synapse admin)
# ==============================================================================

resource "random_password" "synapse_admin" {
  length           = 24
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "azurerm_key_vault_secret" "synapse_admin_password" {
  name         = "synapse-admin-password"
  value        = random_password.synapse_admin.result
  key_vault_id = azurerm_key_vault.main.id
}

# ==============================================================================
# Outputs
# ==============================================================================

output "adls_account_name" {
  value = azurerm_storage_account.adls.name
}

output "event_hub_namespace" {
  value = azurerm_eventhub_namespace.main.name
}

output "databricks_workspace_url" {
  value = azurerm_databricks_workspace.main.workspace_url
}

output "synapse_workspace_name" {
  value = azurerm_synapse_workspace.main.name
}

output "key_vault_uri" {
  value = azurerm_key_vault.main.vault_uri
}

output "log_analytics_workspace_id" {
  value     = azurerm_log_analytics_workspace.main.workspace_id
  sensitive = false
}
