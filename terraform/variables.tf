variable "project_name" {
    default = "mongodb-atlas-fabric"
}
variable "mongodbatlas_group_id" { 

}
variable "mongodbatlas_clustername" {
    
}
variable "mongodbatlas_public_key" { 

}
variable "mongodbatlas_private_key" { 

}
variable "mongodbatlas_dbName" {

}
variable "mongodbatlas_collectionName" {
    default = "all"
}
variable "mongodbatlas_userpass"{
    //MongoDB database username:password
    type = string
}
variable "owner_tag" {

}
variable "keep_until" {

}
variable "subscription_id" { 

}
variable "azure_region" {

}
variable azure_vault_name {
    //If not set will default to vault-${random_string}
    type = string
}
variable "azure_vault_sku_name" {
    //Name of the Azure SKU for Vaults
    default = "standard"
}

variable "fabric_capacity_sku" { 

}
variable "azure_sku_name" {
    //Name of the SKU for Azure isolated servers. For deployments without private endpoints, use F1 (Free): https://azure.microsoft.com/en-us/pricing/details/app-service/windows/
    type = string
    default = "I1v2"
}
variable "applicationLogLevel" {
    //Allowed values: DEBUG, INFO, WARNING, ERROR
    type = string
    default = "INFO"
}
variable "initSyncBatchSize" {
    //Batch size (rows) used for initial sync
    type = string
    default ="100000"
}
variable "incrementalSyncBatchSize" {
    //Batch size (rows) used for incremental sync
    type = string
    default = "1000"
}
variable "incrementalSyncMaxTimeInterval" {
    //Time interval (in secs), incremental sync waits before replicating accumulated changes when next event occurs
    type = string
    default = "180"
}

variable "deployPrivateEnvironment" {
    //Set to "false" to skip the app service environment creation. No private endpoint will be created
    type = bool
    default = true
}