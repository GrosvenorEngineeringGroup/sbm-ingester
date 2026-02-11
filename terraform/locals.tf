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

  # Glue job configuration
  glue_assets_bucket = "aws-glue-assets-318396632821-ap-southeast-2"
  glue_script_name   = "hudiImportScript"
  hudi_output_bucket = "318396632821sydneyhudibucketsrc"
  hudi_source_bucket = "hudibucketsrc"
}
