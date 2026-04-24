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

#############################################################################################################################################################
# About
# In this script, we will -
# 1. Apply necessary updates to organization policies
# 2. Enable google APIs for the Google Cloud services we will use in the lab/demo
#############################################################################################################################################################



# The following are the lab specific org policy updates
resource "google_project_organization_policy" "orgPolicyUpdate_disableSerialPortLogging" {
  project     = var.project_id
  constraint = "compute.disableSerialPortLogging"
  boolean_policy {
    enforced = false
  }
}

resource "google_project_organization_policy" "orgPolicyUpdate_requireOsLogin" {
  project     = var.project_id
  constraint = "compute.requireOsLogin"
  boolean_policy {
    enforced = false
  }
}

resource "google_project_organization_policy" "orgPolicyUpdate_requireShieldedVm" {
  project     = var.project_id
  constraint = "compute.requireShieldedVm"
  boolean_policy {
    enforced = false
  }
}

resource "google_project_organization_policy" "orgPolicyUpdate_vmCanIpForward" {
  project     = var.project_id
  constraint = "compute.vmCanIpForward"
  list_policy {
    allow {
      all = true
    }
  }
}

resource "google_project_organization_policy" "orgPolicyUpdate_vmExternalIpAccess" {
  project     = var.project_id
  constraint = "compute.vmExternalIpAccess"
  list_policy {
    allow {
      all = true
    }
  }
}

resource "google_project_organization_policy" "orgPolicyUpdate_restrictVpcPeering" {
  project     = var.project_id
  constraint = "compute.restrictVpcPeering"
  list_policy {
    allow {
      all = true
    }
  }
}

resource "google_project_service" "enable_orgpolicy_google_apis" {
  project = var.project_id
  service = "orgpolicy.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_compute_google_apis" {
  project = var.project_id
  service = "compute.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_container_google_apis" {
  project = var.project_id
  service = "container.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_containerregistry_google_apis" {
  project = var.project_id
  service = "containerregistry.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_dataproc_google_apis" {
  project = var.project_id
  service = "dataproc.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_bigquery_google_apis" {
  project = var.project_id
  service = "bigquery.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_storage_google_apis" {
  project = var.project_id
  service = "storage.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_composer_google_apis" {
  project = var.project_id
  service = "composer.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_dataplex_google_apis" {
  project = var.project_id
  service = "dataplex.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_biglake_google_apis" {
  project = var.project_id
  service = "biglake.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_datacatalog_google_apis" {
  project = var.project_id
  service = "datacatalog.googleapis.com"
  disable_dependent_services = true
}

resource "google_project_service" "enable_datalineage_google_apis" {
  project = var.project_id
  service = "datalineage.googleapis.com"
  disable_dependent_services = true
}


resource "google_project_service" "enable_notebooks_google_apis" {
  project = var.project_id
  service = "notebooks.googleapis.com"
  disable_dependent_services = true
  
}

resource "google_project_service" "enable_aiplatform_google_apis" {
  project = var.project_id
  service = "aiplatform.googleapis.com"
  disable_dependent_services = true

}

resource "google_project_service" "enable_logging_google_apis" {
  project = var.project_id
  service = "logging.googleapis.com"
  disable_dependent_services = true

}

resource "google_project_service" "enable_monitoring_google_apis" {
  project = var.project_id
  service = "monitoring.googleapis.com"
  disable_dependent_services = true

}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_api_enabling" {
  create_duration = "180s"
  depends_on = [
    google_project_service.enable_orgpolicy_google_apis,
    google_project_service.enable_compute_google_apis,
    google_project_service.enable_container_google_apis,
    google_project_service.enable_containerregistry_google_apis,
    google_project_service.enable_dataproc_google_apis,
    google_project_service.enable_bigquery_google_apis,
    google_project_service.enable_storage_google_apis,
    google_project_service.enable_composer_google_apis,
    google_project_service.enable_dataplex_google_apis,
    google_project_service.enable_biglake_google_apis,
    google_project_service.enable_datacatalog_google_apis,
    google_project_service.enable_datalineage_google_apis,
    google_project_service.enable_notebooks_google_apis,
    google_project_service.enable_aiplatform_google_apis,
    google_project_service.enable_logging_google_apis,
    google_project_service.enable_monitoring_google_apis
  ]
}







