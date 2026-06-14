# =============================================================================
# Network Discovery (via SSM parameters written by core)
# =============================================================================

data "aws_ssm_parameter" "vpc_id" {
  name = "/infra/vpc/id"
}

data "aws_ssm_parameter" "public_subnet_ids" {
  name = "/infra/vpc/public-subnet-ids"
}

data "aws_ssm_parameter" "private_subnet_ids" {
  name = "/infra/vpc/private-subnet-ids"
}

locals {
  vpc_id             = data.aws_ssm_parameter.vpc_id.value
  public_subnet_ids  = jsondecode(data.aws_ssm_parameter.public_subnet_ids.value)
  private_subnet_ids = jsondecode(data.aws_ssm_parameter.private_subnet_ids.value)
}
