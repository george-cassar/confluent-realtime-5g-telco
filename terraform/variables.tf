variable "confluent_cloud_api_key" {
  description = "Confluent Cloud API key (Cloud-level, not cluster-level)"
  type        = string
  sensitive   = true
}

variable "confluent_cloud_api_secret" {
  description = "Confluent Cloud API secret"
  type        = string
  sensitive   = true
}

variable "environment_name" {
  description = "Name of the Confluent Cloud environment"
  type        = string
  default     = "celltower-env-dev"
}

variable "cluster_name" {
  description = "Name of the Kafka cluster"
  type        = string
  default     = "celltower-kafka-dev"
}

variable "cloud_provider" {
  description = "Cloud provider for the Kafka cluster"
  type        = string
  default     = "AWS"
}

variable "region" {
  description = "Cloud region for the Kafka cluster (and Flink compute pool)"
  type        = string
  default     = "eu-central-1"
}

variable "flink_region" {
  description = "Region for the Flink compute pool — must match a Confluent Flink-supported region. Defaults to the same as var.region."
  type        = string
  default     = "eu-central-1"
}

variable "flink_max_cfu" {
  description = "Maximum Confluent Flink Units for the compute pool"
  type        = number
  default     = 5
}

variable "env_suffix" {
  description = "Short suffix appended to resource names (e.g. dev, prod)"
  type        = string
  default     = "dev"
}
