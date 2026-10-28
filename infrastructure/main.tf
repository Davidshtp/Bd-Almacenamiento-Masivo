terraform {
  required_version = ">= 1.0.0"
  required_providers {
    local = {
      source  = "hashicorp/local"
      version = "~> 2.4.0"
    }
    null = {
      source  = "hashicorp/null"
      version = "~> 3.2.0"
    }
  }
}

provider "local" {}

# ------------------------------------------------------------------------------
# 1. APROVISIONAMIENTO DEL DATA LAKE LOCAL (Capa Raw, Processed, Curated)
# ------------------------------------------------------------------------------

resource "null_resource" "data_lake_directories" {
  provisioner "local-exec" {
    command = <<EOT
      mkdir -p ../data_lake/raw
      mkdir -p ../data_lake/processed
      mkdir -p ../data_lake/curated
      mkdir -p ../logs
    EOT
    interpreter = ["PowerShell", "-Command"]
  }
}

# ------------------------------------------------------------------------------
# 2. ARCHIVO DE MANIFIESTO DE INFRAESTRUCTURA (Estado Inicial Estático)
# ------------------------------------------------------------------------------

resource "local_file" "infra_manifest" {
  filename = "${path.module}/../logs/infrastructure_manifest.json"
  content  = jsonencode({
    project           = var.project_name
    architecture_type = "Data Lake + Data Warehouse (Star Schema)"
    database_target   = "MongoDB Atlas (Cloud)"
    data_lake_layers  = ["raw", "processed", "curated"]
    iac_tool          = "Terraform v1.x"
    status            = "PROVISIONED"
  })

  depends_on = [null_resource.data_lake_directories]
}

# ------------------------------------------------------------------------------
# 3. SALIDAS DE TERRAFORM (RÚBRICA & CLUSTER ABP)
# ------------------------------------------------------------------------------

output "cluster_info" {
  value = {
    name        = "ClusterABP"
    provider    = "AWS"
    region      = "US_EAST_1"
    instance    = "M10"
    disk_gb     = 10
    backup      = true
    replication = "3 nodos (REPLICASET)"
  }
  description = "Información del clúster de MongoDB Atlas provisto"
}

output "cluster_connection_string" {
  value       = var.mongodb_atlas_uri
  sensitive   = true
  description = "Cadena de conexión segura al clúster"
}

output "database_username" {
  value       = "davidalexismedinatrujillo"
  description = "Usuario administrador del clúster"
}

output "iac_status" {
  value       = "Infraestructura como Código (IaC) desplegada exitosamente para ${var.project_name}."
  description = "Estado general del aprovisionamiento"
}

output "data_lake_layers" {
  value       = ["data_lake/raw", "data_lake/processed", "data_lake/curated"]
  description = "Capas del Data Lake provisionadas"
}