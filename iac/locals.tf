locals {
  common_tags = {
    project     = var.project_name
    managed_by  = "terraform"
    environment = var.environment
  }

  lambda_s3_prefix = "sbm-files-ingester"

  neptune_subnet_ids = [
    "subnet-0b7ffe958514b2615",
    "subnet-02306ea93a94a2fcf",
    "subnet-0928e926296546e03",
  ]

  neptune_security_group_ids = ["sg-02ece37ea391fba00"]
}
