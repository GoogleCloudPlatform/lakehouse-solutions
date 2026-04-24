# ======================================================================================
# ABOUT
# In this PySpark script, perform analytics on the augmented telco customer churn data
# ======================================================================================
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ======================================================================================

variable "project_id" {
  type        = string
  description = "project id required"
}
variable "project_name" {
 type        = string
 description = "project name"
}
variable "project_number" {
 type        = string
 description = "project number"
}
variable "gcp_account_name" {
 description = "lab user's FQN"
}
variable "deployment_service_account_name" {
 description = "Cloudbuild_Service_account/lab user having permission to deploy terraform resources"
}
variable "org_id" {
 description = "Organization ID in which project created"
}
variable "cloud_composer_image_version" {
 description = "Version of Cloud Composer 3 image to use"
 default     = "composer-3-airflow-2.10.5-build.29"
}
variable "gcp_region" {
 description = "The GCP region you want to use"
 default     = "us-central1"
}

variable "spark_runtime_version" {
  description = "The runtime version for Dataproc Serverless"
  default     = "2.3"
}

