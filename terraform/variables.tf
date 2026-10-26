# =============================================================================
# VARIABLES DE INFRAESTRUCTURA COMO CÓDIGO (TERRAFORM) - MERCADO NUBE
# =============================================================================

variable "aws_region" {
  description = "Región de AWS para el despliegue de recursos"
  type        = string
  default     = "us-east-1"
}

variable "environment" {
  description = "Ambiente de ejecución (dev, lab, prod)"
  type        = string
  default     = "lab"
}

variable "project_name" {
  description = "Nombre del proyecto"
  type        = string
  default     = "mercado-nube"
}

variable "s3_bucket_name" {
  description = "Nombre globalmente único del bucket S3 para el Data Lake"
  type        = string
  default     = "mercado-nube-lakehouse-datalake"
}

variable "redshift_db_name" {
  description = "Nombre de la base de datos analítica en Redshift Serverless"
  type        = string
  default     = "mercado_nube_dwh"
}

variable "redshift_admin_username" {
  description = "Usuario administrador de Redshift Serverless"
  type        = string
  default     = "adminuser"
}

variable "redshift_admin_password" {
  description = "Contraseña administrativa de Redshift Serverless"
  type        = string
  sensitive   = true
  default     = "MercadoNube2026SecurePass!"
}
