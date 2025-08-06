# output "fabric_id" {
#   value = fabric_workspace.mongodb-atlas-mirrored-db
# }

# output "APP_ID" {
#   value = azuread_application_registration.mongodb-atlas-fabric-mirrordb-integration.client_id
# }
# output "Onelake_LZ" {
#   value = replace(fabric_mirrored_database.mongodb-atlas-mirrored-database.properties.onelake_tables_path,"/Tables","/Files/LandingZone/")
# }

# output "mongodb_connectionstring" {
#   # value = data.mongodbatlas_cluster.mongodb-atlas-fabric-integration-connectData.connection_strings[0].private_endpoint[0].connection_string
#   # value = data.mongodbatlas_advanced_cluster.mongodb-atlas-fabric-integration-connectData.connection_strings[0].private_endpoint[0].srv_connection_string
#   value = replace(data.mongodbatlas_advanced_cluster.mongodb-atlas-fabric-integration-connectData.connection_strings[0].private_endpoint[0].srv_connection_string,"mongodb+srv://","mongodb+srv://${var.mongodbatlas_userpass}@")
# }

# output "azure_private_endpoint" {
#   value = data.mongodbatlas_advanced_cluster.mongodb-atlas-fabric-integration-connectData.connection_strings[0].private_endpoint[0].endpoints[0].endpoint_id
# }

# output "azure_private_endpoint_id" {
#   value = azurerm_private_endpoint.mongodb-atlas-fabric-mirrordb-integration-privateEndpoint.id
# }

# output "target_endpoint" {  
#   value = [ for entry in data.mongodbatlas_advanced_cluster.mongodb-atlas-fabric-integration-connectData.connection_strings : { for endpoint_key,endpoints_value in entry : endpoints_key => endpoints_value } ] #if private_endpoint.endpoints.endpoint_id == azurerm_private_endpoint.mongodb-atlas-fabric-mirrordb-integration-privateEndpoint.id]  
# } 