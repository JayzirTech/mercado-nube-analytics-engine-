# =============================================================================
# OUTPUTS DE INFRAESTRUCTURA (TERRAFORM) - MERCADO NUBE
# =============================================================================

output "s3_bucket_name" {
  description = "Nombre del bucket S3 aprovisionado"
  value       = aws_s3_bucket.lakehouse_bucket.id
}

output "s3_bucket_arn" {
  description = "ARN del bucket S3"
  value       = aws_s3_bucket.lakehouse_bucket.arn
}

output "pipeline_role_arn" {
  description = "ARN del rol de ejecución IAM con permisos mínimos"
  value       = aws_iam_role.pipeline_execution_role.arn
}

output "redshift_endpoint" {
  description = "Endpoint de conexión para Amazon Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.redshift_wg.endpoint
}

output "redshift_workgroup_name" {
  description = "Nombre del grupo de trabajo de Redshift Serverless"
  value       = aws_redshiftserverless_workgroup.redshift_wg.workgroup_name
}
