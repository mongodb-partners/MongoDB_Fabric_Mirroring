terraform {
  required_providers {
    fabric = {
        source = "microsoft/fabric"
        version = "1.3.0"
    }
    azurerm = {
      source = "hashicorp/azurerm"
      version = "4.35.0"
    }
    azuread = {
      source = "hashicorp/azuread"
      version = "3.4.0"
    }
    azapi = {
      source = "Azure/azapi"
      version = "2.5.0"
    }
    mongodbatlas = {
        source = "mongodb/mongodbatlas"
        version = "1.37.0"
    }
  }
}

provider "fabric" {
  # Configuration options
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

provider "azapi" {
  subscription_id = var.subscription_id
}

provider "mongodbatlas" {
  # Configuration options
  public_key = var.mongodbatlas_public_key
  private_key  = var.mongodbatlas_private_key
}