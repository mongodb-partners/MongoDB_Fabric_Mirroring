data "azuread_client_config" "current" {}
data "fabric_capacity" "mongodb-fabric-capacity" {
  display_name = var.fabric_capacity_sku
}

data "mongodbatlas_advanced_cluster" "mongodb-atlas-fabric-integration-connectData" {
  project_id = var.mongodbatlas_group_id
  name = var.mongodbatlas_clustername
  depends_on = [ azurerm_app_service_environment_v3.mongodb-atlas-fabric-appServiceEnv[0] ]
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

data "mongodbatlas_advanced_cluster" "mongodb-atlas-fabric-integration-connectData-noEndpoint" {
  project_id = var.mongodbatlas_group_id
  name = var.mongodbatlas_clustername
}