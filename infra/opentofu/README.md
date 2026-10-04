# OpenTofu infrastructure

Atlas uses OpenTofu rather than Terraform for the default open-source infrastructure-as-code path. The local MVP needs no cloud resources, so `main.tf` is deliberately empty except for a version constraint.

Add small, environment-specific modules only after selecting a host and documenting its free-tier limits or budget. Never commit provider credentials or state containing secrets.

