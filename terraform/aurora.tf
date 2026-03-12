# ================================
# Aurora Serverless v2 PostgreSQL
# ================================

# -----------------------------
# Data Sources
# -----------------------------
data "aws_secretsmanager_secret_version" "sbm_aurora" {
  secret_id = "prod/db/sbm-aurora"
}

locals {
  aurora_credentials = jsondecode(data.aws_secretsmanager_secret_version.sbm_aurora.secret_string)
}

data "aws_vpc" "default" {
  id = "vpc-06cf402dcf817e812"
}

data "aws_db_subnet_group" "default" {
  name = "default"
}

data "aws_iam_role" "rds_monitoring" {
  name = "rds-monitoring-role"
}

# -----------------------------
# Security Group
# -----------------------------
resource "aws_security_group" "sbm_aurora" {
  name        = "sbm-aurora-sg"
  description = "Security group for SBM Aurora Serverless v2 cluster"
  vpc_id      = data.aws_vpc.default.id

  tags = merge(local.common_tags, {
    Name = "sbm-aurora-sg"
  })
}

resource "aws_vpc_security_group_ingress_rule" "sbm_aurora_ipv4" {
  security_group_id = aws_security_group.sbm_aurora.id
  description       = "PostgreSQL from anywhere (IPv4)"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = "0.0.0.0/0"

  tags = merge(local.common_tags, {
    Name = "sbm-aurora-pg-ipv4"
  })
}

resource "aws_vpc_security_group_ingress_rule" "sbm_aurora_ipv6" {
  security_group_id = aws_security_group.sbm_aurora.id
  description       = "PostgreSQL from anywhere (IPv6)"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv6         = "::/0"

  tags = merge(local.common_tags, {
    Name = "sbm-aurora-pg-ipv6"
  })
}

resource "aws_vpc_security_group_egress_rule" "sbm_aurora_all" {
  security_group_id = aws_security_group.sbm_aurora.id
  description       = "All outbound traffic"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"

  tags = merge(local.common_tags, {
    Name = "sbm-aurora-egress"
  })
}

# -----------------------------
# CloudWatch Log Group
# -----------------------------
resource "aws_cloudwatch_log_group" "sbm_aurora" {
  name              = "/aws/rds/cluster/sbm-aurora/postgresql"
  retention_in_days = var.log_retention_days
}

# -----------------------------
# Aurora Cluster
# -----------------------------
resource "aws_rds_cluster" "sbm_aurora" {
  cluster_identifier = "sbm-aurora"
  engine             = "aurora-postgresql"
  engine_mode        = "provisioned"
  engine_version     = "16.11"

  database_name   = var.aurora_db_name
  master_username = var.aurora_master_username

  master_password = local.aurora_credentials["password"]

  db_subnet_group_name   = data.aws_db_subnet_group.default.name
  vpc_security_group_ids = [aws_security_group.sbm_aurora.id]

  serverlessv2_scaling_configuration {
    min_capacity = 0.5
    max_capacity = 2.0
  }

  storage_encrypted       = true
  deletion_protection     = true
  backup_retention_period = 7

  enabled_cloudwatch_logs_exports = ["postgresql"]

  tags = merge(local.common_tags, {
    Name = "sbm-aurora"
  })

  depends_on = [aws_cloudwatch_log_group.sbm_aurora]
}

# -----------------------------
# Aurora Instance
# -----------------------------
resource "aws_rds_cluster_instance" "sbm_aurora" {
  identifier         = "sbm-aurora-instance-1"
  cluster_identifier = aws_rds_cluster.sbm_aurora.id
  instance_class     = "db.serverless"
  engine             = aws_rds_cluster.sbm_aurora.engine
  engine_version     = aws_rds_cluster.sbm_aurora.engine_version

  publicly_accessible = true

  performance_insights_enabled = true
  monitoring_interval          = 60
  monitoring_role_arn          = data.aws_iam_role.rds_monitoring.arn

  tags = merge(local.common_tags, {
    Name = "sbm-aurora-instance-1"
  })
}

# -----------------------------
# Outputs
# -----------------------------
output "aurora_cluster_endpoint" {
  description = "Aurora cluster writer endpoint"
  value       = aws_rds_cluster.sbm_aurora.endpoint
}

output "aurora_cluster_reader_endpoint" {
  description = "Aurora cluster reader endpoint"
  value       = aws_rds_cluster.sbm_aurora.reader_endpoint
}

output "aurora_security_group_id" {
  description = "Security group ID for the Aurora cluster"
  value       = aws_security_group.sbm_aurora.id
}
