# PharmaFlow infra — Milestone 5/6 review (2026-10-22).
#
# STATUS: UNAPPLIED and UNVALIDATED. No terraform binary exists in the
# build environment, no cloud account is configured, and nothing here
# has been planned or applied. This file is a reviewed skeleton whose
# variables/defaults are kept consistent with the architecture that
# actually runs locally (docker-compose.yml + Dockerfile + api/app.py);
# treat every resource below as a design sketch, not infrastructure.
#
# Reviewed for consistency in M6:
#   * Postgres 16 (matches docker-compose postgres:16-alpine and
#     db/migrations/001_initial_schema.sql).
#   * API container port 8000 (matches Dockerfile CMD / compose api).
#   * API config: locally the app uses PHARMAFLOW_DB (SQLite, verified);
#     a deployed container would use DATABASE_URL -> PostgresStore,
#     which is still UNVERIFIED (docs/POSTGRES.md). The variable wiring
#     below reflects that target, not a working deployment.
#   * db_name / db_username defaults match the compose service.

terraform {
  required_version = ">= 1.5"
}

variable "project" {
  type    = string
  default = "pharmaflow"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "db_name" {
  type    = string
  default = "pharmaflow" # matches docker-compose POSTGRES_DB
}

variable "db_username" {
  type    = string
  default = "pharmaflow" # matches docker-compose POSTGRES_USER
}

variable "db_engine_version" {
  type    = string
  default = "16" # matches postgres:16-alpine in docker-compose.yml
}

variable "db_password" {
  type      = string
  sensitive = true
  # Supply via TF_VAR_db_password / a secret manager, never committed.
}

variable "api_image" {
  type    = string
  default = "pharmaflow-api:latest" # built from the repo Dockerfile
}

variable "api_port" {
  type    = number
  default = 8000 # matches Dockerfile / uvicorn / docker-compose api
}

# PLACEHOLDER: managed Postgres (provider block omitted until a cloud is chosen)
# resource "aws_db_instance" "pharmaflow" {
#   identifier     = "${var.project}-${var.environment}"
#   engine         = "postgres"
#   engine_version = var.db_engine_version
#   db_name        = var.db_name
#   username       = var.db_username
#   password       = var.db_password
#   instance_class = "db.t4g.micro"
#   ...
# }

# PLACEHOLDER: container service for the FastAPI app (ECS Fargate / Cloud Run —
# decision recorded in ARCHITECTURE.md once a cloud is chosen).
# The container would run var.api_image on var.api_port with
# DATABASE_URL pointing at the Postgres instance above.
# resource "aws_ecs_service" "api" { ... }

# PLACEHOLDER: versioned object storage for raw CSV landing + quarantine exports.

output "next_steps" {
  value = "Choose cloud, add provider block, run terraform plan in CI (never apply unreviewed)."
}

output "architecture_summary" {
  value = "Postgres ${var.db_engine_version} (${var.db_name}) + FastAPI container (${var.api_image}, port ${var.api_port}) for project ${var.project} (${var.environment}). Unapplied skeleton — see header."
}
