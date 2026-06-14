terraform {
  required_version = ">= 1.0"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
    # MediaConnect lives in the Cloud-Control-API provider, not the regular
    # AWS provider. Shares the same credentials/region as `aws`.
    awscc = {
      source  = "hashicorp/awscc"
      version = "~> 1.0"
    }
    archive = {
      source  = "hashicorp/archive"
      version = "~> 2.0"
    }
  }

  backend "s3" {
    bucket       = "nlr-root-terraform-state"
    key          = "apps/bumps-video/terraform.tfstate"
    region       = "eu-west-2"
    encrypt      = true
    use_lockfile = true
  }
}

provider "aws" {
  region = "eu-west-2"

  default_tags {
    tags = {
      ManagedBy = "terraform"
      App       = "bumps-video"
    }
  }
}

provider "awscc" {
  region = "eu-west-2"
}
