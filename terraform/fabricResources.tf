resource "fabric_workspace" "mongodb-atlas-mirrored-db-workspace" {
    display_name = "MongoDB Atlas Azure Fabric Mirrored Database Integration"
    identity = {
      type = "SystemAssigned"
    }
    capacity_id = data.fabric_capacity.mongodb-fabric-capacity.id
    depends_on = [ azuread_application_registration.mongodb-atlas-fabric-mirrordb-integration ]
}

resource "azuread_service_principal" "azuread_sp" {
  client_id = azuread_application_registration.mongodb-atlas-fabric-mirrordb-integration.client_id
  use_existing   = true
}

resource "fabric_workspace_role_assignment" "fabric-workspace-admin" {
    workspace_id = fabric_workspace.mongodb-atlas-mirrored-db-workspace.id
    principal = {
      id = azuread_service_principal.azuread_sp.object_id
      type = "ServicePrincipal"
    }
    role = "Admin"
}
# Mirrored Database definition: https://learn.microsoft.com/en-gb/rest/api/fabric/articles/item-management/definitions/mirrored-database-definition
resource "fabric_mirrored_database" "mongodb-atlas-mirrored-database" {
  display_name = "MongoDB Atlas - Fabric Mirrored Database"
  workspace_id = fabric_workspace.mongodb-atlas-mirrored-db-workspace.id
  format = "Default"
  definition = {
    "mirroring.json" = {
      source = "${path.root}/mirroring.json.tmpl"
      }
  }
}
