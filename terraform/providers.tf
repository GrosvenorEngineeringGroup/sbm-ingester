provider "aws" {
  region = var.aws_region

  default_tags {
    tags = local.common_tags
  }

  # "Department" tag is managed externally (by IT / org policy) on a subset of
  # resources. Terraform ignores it so plan/apply neither adds nor removes it.
  ignore_tags {
    keys = ["Department"]
  }
}
