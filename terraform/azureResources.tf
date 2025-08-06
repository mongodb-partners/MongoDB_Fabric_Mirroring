resource "random_string" "random_suffix" {
  length  = 5
  lower   = true
  numeric = true
  special = false
  upper   = false
}
resource "azurerm_resource_group" "mongodb-atlas-fabric-resourceGroup" {
  name = "mongodb-atlas-fabric-resourceGroup"
  location = var.azure_region
  tags = {
    owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
}

resource "azurerm_virtual_network" "mongodb-atlas-fabric-vNet" {
  name      = "mongodb-atlas-fabric-vNet"
  location  = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.location
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  address_space = ["10.0.0.0/16"]
  tags = {
    owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

resource "azurerm_subnet" "mongodb-atlas-fabric-subnet1" {
  name = "subnet1"
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  virtual_network_name = azurerm_virtual_network.mongodb-atlas-fabric-vNet[0].name
  address_prefixes = [ "10.0.2.0/24" ]

  delegation {
    name = "Microsoft.Web.hostingEnvironments"
    service_delegation {
      name = "Microsoft.Web/hostingEnvironments"
      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
    }
  }
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

resource "azurerm_subnet" "mongodb-atlas-fabric-PrivateLinkSubnet" {
  name = "PrivateLinkSubnet"
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  virtual_network_name = azurerm_virtual_network.mongodb-atlas-fabric-vNet[0].name
  address_prefixes = [ "10.0.1.0/24" ]
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

resource "azurerm_subnet" "mongodb-atlas-fabric-webAppSubnet" {
  name = "webAppSubnet"
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  virtual_network_name = azurerm_virtual_network.mongodb-atlas-fabric-vNet[0].name
  address_prefixes = [ "10.0.0.0/24" ]
  delegation {
    name = "Microsoft.Web.ServerFarms"
    service_delegation {
      name = "Microsoft.Web/serverFarms"
      actions = ["Microsoft.Network/virtualNetworks/subnets/action"]
    }
  }
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

resource "azurerm_app_service_environment_v3" "mongodb-atlas-fabric-appServiceEnv" {
  name = "mongodb-atlas-fabric-asev3-${random_string.random_suffix.result}"
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  subnet_id = azurerm_subnet.mongodb-atlas-fabric-subnet1[0].id
  tags = {
      owner = var.owner_tag
    }
  lifecycle {
    ignore_changes = [ tags ]
  }
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}
//Service plan for App Service Environment and Private Endpoint deployment
resource "azurerm_service_plan" "mongodb-atlas-fabric-service-plan" {
  name = "mongodb-atlas-fabric-service-plan-${random_string.random_suffix.result}"
  location = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.location
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  os_type = "Linux"
  sku_name = var.azure_sku_name
  app_service_environment_id = azurerm_app_service_environment_v3.mongodb-atlas-fabric-appServiceEnv[0].id
  tags = {
    owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

//Service plan for simple deployment without private endpoint
resource "azurerm_service_plan" "mongodb-atlas-fabric-service-simple-plan" {
  name = "mongodb-atlas-fabric-service-plan-${random_string.random_suffix.result}"
  location = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.location
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  os_type = "Linux"
  sku_name = var.azure_sku_name
  tags = {
    owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
  //
  count = try(var.deployPrivateEnvironment ? 0 : 1, 0)
}

resource "azurerm_key_vault" "mongodb-atlas-fabric-mirrordb-vault" {
  # name = "mongodbatlasfabricvault3"
  name = coalesce(var.azure_vault_name, "vault-${random_string.random_suffix.result}")
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  sku_name = var.azure_vault_sku_name
  tenant_id = data.azuread_client_config.current.tenant_id
  location = var.azure_region
  access_policy  {
    tenant_id = data.azuread_client_config.current.tenant_id
    object_id = data.azuread_client_config.current.object_id
      key_permissions = [  
        "Get",  
        "List",  
        "Create",  
        "Delete",  
        "Purge",  
      ]  
      secret_permissions = [  
        "Get",  
        "List",  
        "Set",  
        "Delete",  
        "Purge",  
      ]  
      certificate_permissions = [  
        "Get",  
        "List",  
        "Create",  
        "Delete",  
        "Purge",  
      ]
  }
  tags = {
      owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
}

resource "azuread_application_registration" "mongodb-atlas-fabric-mirrordb-integration" {
    display_name = "mongodb-atlas-fabric-mirrordb-integration"
}

resource "azuread_application_password" "mongodb-atlas-fabric-mirrordb-integration-secret" {
  application_id = azuread_application_registration.mongodb-atlas-fabric-mirrordb-integration.id
}

resource "azurerm_key_vault_secret" "mongodb-atlas-fabric-mirrordb-integration-secretValue" {
  name         = "mongodb-atlas-fabric-app-secret-v2"
  value        = azuread_application_password.mongodb-atlas-fabric-mirrordb-integration-secret.value
  key_vault_id = azurerm_key_vault.mongodb-atlas-fabric-mirrordb-vault.id
}

resource "azurerm_private_endpoint" "mongodb-atlas-fabric-mirrordb-integration-privateEndpoint" {
  name                = "${var.project_name}-private-endpoint"
  location            = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.location
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  subnet_id           = azurerm_subnet.mongodb-atlas-fabric-PrivateLinkSubnet[0].id
  private_service_connection {
    name                           = mongodbatlas_privatelink_endpoint.mongodb-atlas-fabric-privateEndpoint[0].private_link_service_name
    private_connection_resource_id = mongodbatlas_privatelink_endpoint.mongodb-atlas-fabric-privateEndpoint[0].private_link_service_resource_id
    is_manual_connection           = true
    request_message = "Azure Private Link Connection"
  }
  tags = {
        owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
  depends_on = [ mongodbatlas_privatelink_endpoint.mongodb-atlas-fabric-privateEndpoint ]
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

# https://registry.terraform.io/providers/hashicorp/azurerm/latest/docs/resources/linux_web_app
resource "azurerm_linux_web_app" "mongodb-atlas-fabric-mirrordb-integration-webapp" {
  name                = "mongodbatlasfabricmirrordbsynch"
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  location            = azurerm_service_plan.mongodb-atlas-fabric-service-plan[0].location
  service_plan_id     = azurerm_service_plan.mongodb-atlas-fabric-service-plan[0].id
  site_config {
    application_stack {
      python_version = "3.12"
    }
    minimum_tls_version = "1.2"
    ftps_state = "FtpsOnly"
    vnet_route_all_enabled = true
  }
  app_settings = {
    "APP_LOG_LEVEL"           = var.applicationLogLevel
    "MONGO_CONN_STR"          = replace(data.mongodbatlas_advanced_cluster.mongodb-atlas-fabric-integration-connectData[0].connection_strings[0].private_endpoint[0].srv_connection_string,"mongodb+srv://","mongodb+srv://${var.mongodbatlas_userpass}@")
    "MONGO_DB_NAME"           = var.mongodbatlas_dbName
    "MONGO_COLLECTION"        = var.mongodbatlas_collectionName
    "LZ_URL"                  = replace(fabric_mirrored_database.mongodb-atlas-mirrored-database.properties.onelake_tables_path,"/Tables","/Files/LandingZone/")
    "APP_ID"                  = azuread_application_registration.mongodb-atlas-fabric-mirrordb-integration.client_id
    "SECRET"                  = azurerm_key_vault_secret.mongodb-atlas-fabric-mirrordb-integration-secretValue.value
    "TENANT_ID"               = data.azuread_client_config.current.tenant_id
    "INIT_LOAD_BATCH_SIZE"    = var.initSyncBatchSize
    "DELTA_SYNC_BATCH_SIZE"   = var.incrementalSyncBatchSize
    "TIME_THRESHOLD_IN_SEC"   = var.incrementalSyncMaxTimeInterval
  }
  tags = {
        owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
  depends_on = [ azurerm_private_endpoint.mongodb-atlas-fabric-mirrordb-integration-privateEndpoint ]
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

resource "azurerm_linux_web_app" "mongodb-atlas-fabric-mirrordb-integration-simple-webapp" {
  count               = try(var.deployPrivateEnvironment ? 0 : 1, 0)
  name                = "mongodbatlasfabricmirrordbsynch"
  resource_group_name = azurerm_resource_group.mongodb-atlas-fabric-resourceGroup.name
  location            = azurerm_service_plan.mongodb-atlas-fabric-service-simple-plan[0].location
  service_plan_id     = azurerm_service_plan.mongodb-atlas-fabric-service-simple-plan[0].id
  site_config {
    application_stack {
      python_version = "3.12"
    }
    minimum_tls_version = "1.2"
    ftps_state = "FtpsOnly"
    vnet_route_all_enabled = true
    always_on = false
  }
  app_settings = {
    "APP_LOG_LEVEL"           = var.applicationLogLevel
    "MONGO_CONN_STR"          = replace(data.mongodbatlas_advanced_cluster.mongodb-atlas-fabric-integration-connectData-noEndpoint.connection_strings[0].standard_srv,"mongodb+srv://","mongodb+srv://${var.mongodbatlas_userpass}@")
    "MONGO_DB_NAME"           = var.mongodbatlas_dbName
    "MONGO_COLLECTION"        = var.mongodbatlas_collectionName
    "LZ_URL"                  = replace(fabric_mirrored_database.mongodb-atlas-mirrored-database.properties.onelake_tables_path,"/Tables","/Files/LandingZone/")
    "APP_ID"                  = azuread_application_registration.mongodb-atlas-fabric-mirrordb-integration.client_id
    "SECRET"                  = azurerm_key_vault_secret.mongodb-atlas-fabric-mirrordb-integration-secretValue.value
    "TENANT_ID"               = data.azuread_client_config.current.tenant_id
    "INIT_LOAD_BATCH_SIZE"    = var.initSyncBatchSize
    "DELTA_SYNC_BATCH_SIZE"   = var.incrementalSyncBatchSize
    "TIME_THRESHOLD_IN_SEC"   = var.incrementalSyncMaxTimeInterval
  }
  tags = {
        owner = var.owner_tag
  }
  lifecycle {
    ignore_changes = [ tags ]
  }
}


resource "azurerm_app_service_source_control" "mongodb-atlas-fabric-mirrordb-integration-sourceControl" {
  app_id   = azurerm_linux_web_app.mongodb-atlas-fabric-mirrordb-integration-webapp[0].id
  repo_url = "https://github.com/mongodb-partners/MongoDB_Fabric_Mirroring.git"
  branch   = "main"
  use_manual_integration = true
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

resource "azurerm_app_service_source_control" "mongodb-atlas-fabric-mirrordb-integration-sourceControl-noEndpoint" {
  app_id   = azurerm_linux_web_app.mongodb-atlas-fabric-mirrordb-integration-simple-webapp[0].id
  repo_url = "https://github.com/mongodb-partners/MongoDB_Fabric_Mirroring.git"
  branch   = "main"
  use_manual_integration = true
  depends_on = [ azurerm_linux_web_app.mongodb-atlas-fabric-mirrordb-integration-simple-webapp[0] ]
  //
  count = try(var.deployPrivateEnvironment ? 0 : 1, 0)
}
