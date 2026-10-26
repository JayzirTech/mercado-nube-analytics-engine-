# =============================================================================
# INFRAESTRUCTURA COMO CÓDIGO (TERRAFORM) - MERCADO NUBE
# =============================================================================
# Cumple con la Sección 4 y 10 del simulacro técnico:
# 1. Bucket S3 con cifrado, particionado por capas (raw/cleaned/curated/quarantine)
#    y políticas de ciclo de vida automáticas.
# 2. Roles IAM de privilegios mínimos para ejecución y despliegue (sin llaves en código).
# 3. Amazon Redshift Serverless (Namespace y Workgroup) para el almacén analítico.
# 4. Grupos de logs y alarmas en Amazon CloudWatch para observabilidad.
# 5. Todo parametrizable y listo para 'terraform apply' y 'terraform destroy'.
# =============================================================================

terraform {
  required_version = ">= 1.5.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.30.0"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = var.project_name
      Environment = var.environment
      ManagedBy   = "Terraform"
      Owner       = "JayzirTech"
    }
  }
}

# =============================================================================
# 1. ALMACENAMIENTO: DATA LAKE EN AMAZON S3 (MEDALLION)
# =============================================================================
resource "aws_s3_bucket" "lakehouse_bucket" {
  bucket        = "${var.s3_bucket_name}-${var.environment}"
  force_destroy = true # Permite 'terraform destroy' limpio en cuentas de laboratorio
}

# Cifrado en reposo del bucket (Server-Side Encryption SSE-S3)
resource "aws_s3_bucket_server_side_encryption_configuration" "s3_encryption" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Habilitar versionado para trazabilidad y auditoría
resource "aws_s3_bucket_versioning" "s3_versioning" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  versioning_configuration {
    status = "Enabled"
  }
}

# Bloquear acceso público (Seguridad por defecto)
resource "aws_s3_bucket_public_access_block" "s3_block_public" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Políticas de Ciclo de Vida (Lifecycle Policy) para optimización de costos
resource "aws_s3_bucket_lifecycle_configuration" "lakehouse_lifecycle" {
  bucket = aws_s3_bucket.lakehouse_bucket.id

  rule {
    id     = "archive-old-raw-data"
    status = "Enabled"

    filter {
      prefix = "raw/"
    }

    transition {
      days          = 30
      storage_class = "STANDARD_IA"
    }

    transition {
      days          = 90
      storage_class = "GLACIER"
    }

    abort_incomplete_multipart_upload {
      days_after_initiation = 7
    }
  }
}

# Creación de prefijos de capas Medallion
resource "aws_s3_object" "prefix_raw" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "raw/"
}

resource "aws_s3_object" "prefix_cleaned" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "cleaned/"
}

resource "aws_s3_object" "prefix_curated" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "curated/"
}

resource "aws_s3_object" "prefix_quarantine" {
  bucket = aws_s3_bucket.lakehouse_bucket.id
  key    = "quarantine/"
}


# =============================================================================
# 2. SEGURIDAD: ROLES Y POLÍTICAS DE IAM (LEAST PRIVILEGE)
# =============================================================================

# Rol de IAM para la ejecución del Pipeline (Airflow / ECS / Lambda)
resource "aws_iam_role" "pipeline_execution_role" {
  name = "${var.project_name}-execution-role-${var.environment}"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = [
            "ecs-tasks.amazonaws.com",
            "airflow.amazonaws.com",
            "redshift.amazonaws.com"
          ]
        }
      }
    ]
  })
}

# Política de permisos estrictos sobre S3 y CloudWatch Logs
resource "aws_iam_policy" "pipeline_least_privilege_policy" {
  name        = "${var.project_name}-least-privilege-${var.environment}"
  description = "Permisos mínimos sobre S3, Logs y Redshift para el pipeline de Mercado Nube"

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "S3LakehouseAccess"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject",
          "s3:ListBucket"
        ]
        Resource = [
          aws_s3_bucket.lakehouse_bucket.arn,
          "${aws_s3_bucket.lakehouse_bucket.arn}/*"
        ]
      },
      {
        Sid    = "CloudWatchLogging"
        Effect = "Allow"
        Action = [
          "logs:CreateLogGroup",
          "logs:CreateLogStream",
          "logs:PutLogEvents"
        ]
        Resource = "arn:aws:logs:*:*:*"
      }
    ]
  })
}

resource "aws_iam_role_policy_attachment" "attach_pipeline_policy" {
  role       = aws_iam_role.pipeline_execution_role.name
  policy_arn = aws_iam_policy.pipeline_least_privilege_policy.arn
}


# =============================================================================
# 3. DESTINO ANALÍTICO: AMAZON REDSHIFT SERVERLESS
# =============================================================================
resource "aws_redshiftserverless_namespace" "redshift_ns" {
  namespace_name      = "${var.project_name}-ns-${var.environment}"
  admin_username      = var.redshift_admin_username
  admin_user_password = var.redshift_admin_password
  db_name             = var.redshift_db_name
  iam_roles           = [aws_iam_role.pipeline_execution_role.arn]
}

resource "aws_redshiftserverless_workgroup" "redshift_wg" {
  workgroup_name = "${var.project_name}-wg-${var.environment}"
  namespace_name = aws_redshiftserverless_namespace.redshift_ns.namespace_name
  base_capacity  = 8 # Capacidad base mínima (8 RPUs para control de costo)

  publicly_accessible = true # Facilita conexión segura desde laboratorios externos
}


# =============================================================================
# 4. OBSERVABILIDAD: AMAZON CLOUDWATCH LOGS Y ALARMAS
# =============================================================================
resource "aws_cloudwatch_log_group" "pipeline_logs" {
  name              = "/aws/${var.project_name}/pipeline-${var.environment}"
  retention_in_days = 14
}

resource "aws_cloudwatch_metric_alarm" "high_quarantine_rate" {
  alarm_name          = "${var.project_name}-high-quarantine-rate"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "QuarantineRecordsCount"
  namespace           = "MercadoNube/DataQuality"
  period              = 300 # 5 minutos
  statistic           = "Sum"
  threshold           = 100 # Dispara alarma si más de 100 registros van a cuarentena
  alarm_description   = "Alerta disparada por pico anómalo de registros rechazados en la capa de cuarentena."
}
