variable "mongodb_atlas_uri" {
  type        = string
  description = "Cadena de conexión de MongoDB Atlas para HeavyCult"
  sensitive   = true
}

variable "project_name" {
  type        = string
  description = "Nombre del proyecto de Big Data"
  default     = "heavycult"
}