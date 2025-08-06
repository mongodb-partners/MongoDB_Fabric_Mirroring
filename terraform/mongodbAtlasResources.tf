resource "mongodbatlas_privatelink_endpoint" "mongodb-atlas-fabric-privateEndpoint" {
  project_id    = var.mongodbatlas_group_id
  provider_name = "AZURE"
  region        = var.azure_region
  timeouts {
    create = "30m"
    delete = "20m"
  }
  depends_on = [ azurerm_virtual_network.mongodb-atlas-fabric-vNet ]
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}

#
# https://registry.terraform.io/providers/mongodb/mongodbatlas/latest/docs/resources/privatelink_endpoint_service
#  NOTE: Create and delete wait for all clusters on the project to IDLE in order for their operations to complete. 
#  This ensures the latest connection strings can be retrieved following creation or deletion of this resource. 
#  Default timeout is 2hrs.
resource "mongodbatlas_privatelink_endpoint_service" "mongodb-atlas-fabric-mirrordb-integration-privateEndpointService" {
  project_id                  = mongodbatlas_privatelink_endpoint.mongodb-atlas-fabric-privateEndpoint[0].project_id
  private_link_id             = mongodbatlas_privatelink_endpoint.mongodb-atlas-fabric-privateEndpoint[0].private_link_id
  endpoint_service_id         = azurerm_private_endpoint.mongodb-atlas-fabric-mirrordb-integration-privateEndpoint[0].id
  private_endpoint_ip_address = azurerm_private_endpoint.mongodb-atlas-fabric-mirrordb-integration-privateEndpoint[0].private_service_connection.0.private_ip_address
  provider_name               = "AZURE"
  depends_on = [ azurerm_private_endpoint.mongodb-atlas-fabric-mirrordb-integration-privateEndpoint ]
  timeouts {
    create = "20m"
    delete = "20m"
  }
  //
  count = try(var.deployPrivateEnvironment ? 1 : 0, 0)
}
