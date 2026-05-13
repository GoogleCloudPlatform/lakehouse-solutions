/**
 * Copyright 2026 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/******************************************
Local variables declaration
 *****************************************/

locals {
solution_prefix                     = "spark-froyo-lab"
project_id                          = "${var.project_id}"
project_nbr                         = "${var.project_number}"
admin_upn_fqn                       = "${var.gcp_account_name}"
location                            = "${var.gcp_region}"
umsa                                = "${local.solution_prefix}-umsa"
umsa_fqn                            = "${local.umsa}@${local.project_id}.iam.gserviceaccount.com"
spark_bucket                        = "${local.solution_prefix}-spark-bucket-${local.project_nbr}"
spark_bucket_fqn                    = "gs://{local.spark_bucket}-${local.project_nbr}"
vpc_nm                              = "${local.solution_prefix}-vpc-${local.project_nbr}"
spark_subnet_nm                     = "spark-snet"
spark_subnet_cidr                   = "10.2.0.0/16"
data_and_code_bucket                = "${local.solution_prefix}-data_and_code_bucket-${local.project_nbr}"
bq_dataset                          = "froyo_ds"
CC_GMSA_FQN                         = "service-${local.project_nbr}@cloudcomposer-accounts.iam.gserviceaccount.com"
GCE_GMSA_FQN                        = "${local.project_nbr}-compute@developer.gserviceaccount.com"
CLOUD_COMPOSER3_IMG_VERSION         = "${var.cloud_composer_image_version}"
S8S_SPARK_RUNTIME_VERSION           = "${var.spark_runtime_version}"
lakehouse_iceberg_warehouse_bucket  = "froyo-lakehouse-iceberg-${local.project_nbr}"
lakehouse_hive_warehouse_bucket     = "froyo-lakehouse-hive-${local.project_nbr}"
lakehouse_stage_bucket              = "froyo-lakehouse-hive-${local.project_nbr}"
lakehouse_hive_catalog_name         = "froyo_hive_catalog"
lakehouse_iceberg_catalog_name      = "froyo_iceberg_catalog"
}

/******************************************
1. User Managed Service Account Creation
 *****************************************/
module "umsa_creation" {
  source     = "terraform-google-modules/service-accounts/google"
  project_id = local.project_id
  names      = ["${local.umsa}"]
  display_name = "User Managed Service Account"
  description  = "User Managed Service Account for Serverless Spark"

}

/******************************************
2a. IAM role grants to User Managed Service Account
 *****************************************/

module "umsa_role_grants" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "${local.umsa_fqn}"
  prefix                  = "serviceAccount"
  project_id              = local.project_id
  project_roles = [  
    "roles/iam.serviceAccountUser",
    "roles/iam.serviceAccountTokenCreator",
    "roles/storage.objectViewer",
    "roles/storage.admin",
    "roles/dataproc.worker",
    "roles/dataproc.editor",
    "roles/bigquery.dataEditor",
    "roles/bigquery.admin",
    "roles/composer.worker",
    "roles/composer.admin",
    "roles/aiplatform.admin",
    "roles/aiplatform.user",
    "roles/biglake.admin"
  ]
  depends_on = [
    module.umsa_creation
  ]
}

/******************************************
2b. IAM role grants to Google Managed Service Account for Cloud Composer 3
 *****************************************/

module "gmsa_role_grants_cc" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "${local.CC_GMSA_FQN}"
  prefix                  = "serviceAccount"
  project_id              = local.project_id
  project_roles = [
    
    "roles/composer.ServiceAgentV2Ext",
  ]
  depends_on = [
    module.umsa_role_grants
  ]
}

/******************************************
2c. IAM role grants to Google Managed Service 
Account for Compute Engine (for Cloud Composer 3 to download images)
 *****************************************/

module "gmsa_role_grants_gce" {
  source                  = "terraform-google-modules/iam/google//modules/member_iam"
  service_account_address = "${local.GCE_GMSA_FQN}"
  prefix                  = "serviceAccount"
  project_id              = local.project_id
  project_roles = [
    
    "roles/editor",
  ]
  depends_on = [
    module.umsa_role_grants
  ]
}

/******************************************************
3. Service Account Impersonation Grants to Admin User
 ******************************************************/

module "umsa_impersonate_privs_to_admin" {
  source  = "terraform-google-modules/iam/google//modules/service_accounts_iam/"

  service_accounts = ["${local.umsa_fqn}"]
  project          = local.project_id
  mode             = "additive"
  bindings = {
    "roles/iam.serviceAccountUser" = [
      "user:${local.admin_upn_fqn}"
    ],
    "roles/iam.serviceAccountTokenCreator" = [
      "user:${local.admin_upn_fqn}"
    ]

  }
  depends_on = [
    module.umsa_creation
  ]
}

/******************************************************
4. IAM role grants to Admin User
 ******************************************************/

module "administrator_role_grants" {
  source   = "terraform-google-modules/iam/google//modules/projects_iam"
  projects = ["${local.project_id}"]
  mode     = "additive"

  bindings = {
    "roles/storage.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/dataproc.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.user" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.dataEditor" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/bigquery.jobUser" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/composer.environmentAndStorageObjectViewer" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/iam.serviceAccountUser" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/iam.serviceAccountTokenCreator" = [
      "user:${local.admin_upn_fqn}",
    ]
    "roles/composer.admin" = [
      "user:${local.admin_upn_fqn}",
    ]
  }
  depends_on = [
    module.umsa_role_grants,
    module.umsa_impersonate_privs_to_admin
  ]

  }

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_identities_permissions" {
  create_duration = "120s"
  depends_on = [
    module.umsa_creation,
    module.umsa_role_grants,
    module.umsa_impersonate_privs_to_admin,
    module.administrator_role_grants,
    module.gmsa_role_grants_cc,
    module.gmsa_role_grants_gce
  ]
}

/******************************************
5. VPC Network & Subnet Creation
 *****************************************/
module "vpc_creation" {
  source                                 = "terraform-google-modules/network/google"
  version                                = "~> 9.0"
  project_id                             = local.project_id
  network_name                           = local.vpc_nm
  routing_mode                           = "REGIONAL"

  subnets = [
    {
      subnet_name           = "${local.spark_subnet_nm}"
      subnet_ip             = "${local.spark_subnet_cidr}"
      subnet_region         = "${local.location}"
      subnet_range          = local.spark_subnet_cidr
      subnet_private_access = true
    }
  ]
  depends_on = [
    time_sleep.sleep_after_identities_permissions
  ]
}

/******************************************
6. Firewall rules creation
 *****************************************/

resource "google_compute_firewall" "allow_intra_snet_ingress_to_any" {
  project   = local.project_id 
  name      = "allow-intra-snet-ingress-to-any"
  network   = local.vpc_nm
  direction = "INGRESS"
  source_ranges = [local.spark_subnet_cidr]
  allow {
    protocol = "all"
  }
  description        = "Creates firewall rule to allow ingress from within Spark subnet on all ports, all protocols"
  depends_on = [
    module.vpc_creation, 
    module.administrator_role_grants
  ]
}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_network_and_firewall_creation" {
  create_duration = "120s"
  depends_on = [
    module.vpc_creation,
    google_compute_firewall.allow_intra_snet_ingress_to_any
  ]
}

/******************************************
7. Storage bucket creation
 *****************************************/

resource "google_storage_bucket" "spark_bucket_creation" {
  name                              = local.spark_bucket
  project                           = local.project_id
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_network_and_firewall_creation
  ]
}


resource "google_storage_bucket" "data_and_code_bucket_creation" {
  name                              = local.data_and_code_bucket
  project                           = local.project_id
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  depends_on = [
      time_sleep.sleep_after_network_and_firewall_creation
  ]
}

resource "google_storage_bucket" "iceberg_warehouse_bucket_creation" {
  name                          = local.lakehouse_iceberg_warehouse_bucket
  project                       = local.project_id
  location                      = local.location
  uniform_bucket_level_access   = true
  force_destroy                 = true
  depends_on = [
    time_sleep.sleep_after_network_and_firewall_creation
  ]
}

resource "google_storage_bucket" "hive_warehouse_bucket_creation" {
  name                          = local.lakehouse_hive_warehouse_bucket
  project                       = local.project_id
  location                      = local.location
  uniform_bucket_level_access   = true
  force_destroy                 = true
  depends_on = [
    time_sleep.sleep_after_network_and_firewall_creation
  ]
}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_bucket_creation" {
  create_duration = "60s"
  depends_on = [
    google_storage_bucket.data_and_code_bucket_creation,
    google_storage_bucket.spark_bucket_creation,
    google_storage_bucket.iceberg_warehouse_bucket_creation,
    google_storage_bucket.hive_warehouse_bucket_creation
  ]
}

/******************************************
8a. Copy the Pyspark scripts to data_and_code_bucket
 *****************************************/

resource "google_storage_bucket_object" "pyspark_scripts_upload_to_gcs" {
  for_each = fileset("../scripts/pyspark/", "*")
  source = "../scripts/pyspark/${each.value}"
  name = "scripts/pyspark/${each.value}"
  bucket = "${local.data_and_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
8b. Copy the notebooks scripts to data_and_code_bucket
 *****************************************/

resource "google_storage_bucket_object" "notebooks_upload_to_gcs" {
  for_each = fileset("../notebooks/", "*")
  source = "../notebooks/${each.value}"
  name = "notebooks/${each.value}"
  bucket = "${local.data_and_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
8c. Copy the Airflow DAG scripts to data_and_code_bucket
 *****************************************/

resource "google_storage_bucket_object" "airflow_dag_upload_to_gcs" {
  name   = "scripts/airflow-dag/pipeline.py"
  source = "../scripts/airflow-dag/pipeline.py"
  bucket = "${local.data_and_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
8d. Copy the data to data_and_code_bucket
 *****************************************/

resource "null_resource" "unzip_datasets" {
  provisioner "local-exec" {
    command = <<EOT
      set -e
      TMP_DIR=$(mktemp -d)
      mkdir -p $TMP_DIR/datasets
      mkdir -p $TMP_DIR/froyo_recipe_pdfs
      echo "{\"tmp_dir\": \"$TMP_DIR\"}" > unzip_info.json
      find ../datasets -name '*.zip' -exec unzip -o {} -d $TMP_DIR/datasets/ \;
      find ../datasets -name '*.tgz' ! -name 'froyo_recipe_pdfs-*.tgz' -exec tar -xzf {} -C $TMP_DIR/datasets/ \;
      find ../datasets -maxdepth 1 -type f ! -name '*.zip' ! -name '*.tgz' ! -name 'froyo_recipe_pdfs-*.tgz' -exec cp {} $TMP_DIR/datasets/ \;
      find ../datasets -name 'froyo_recipe_pdfs-*.tgz' -exec tar -xzf {} -C $TMP_DIR/froyo_recipe_pdfs/ \;
EOT
  }

  provisioner "local-exec" {
    when    = destroy
    command = <<EOT
      set -e
      TMP_DIR_TO_DELETE=$(sed 's/.*: "\(.*\)"}/\1/' unzip_info.json)
      rm -rf "$TMP_DIR_TO_DELETE"
      rm -f unzip_info.json
EOT
  }

  triggers = {
    dataset_files = sha1(join("", [for f in fileset("../datasets/", "**/*") : filesha1("../datasets/${f}")]))
  }
}

resource "google_storage_bucket_object" "files_upload_to_gcs" {
  for_each = fileset("${jsondecode(file("unzip_info.json")).tmp_dir}/datasets", "**/*")
  source = "${jsondecode(file("unzip_info.json")).tmp_dir}/datasets/${each.value}"
  name = "datasets/${each.value}"
  bucket = "${local.data_and_code_bucket}"
  depends_on = [
    null_resource.unzip_datasets,
    time_sleep.sleep_after_bucket_creation
  ]
}

resource "google_storage_bucket_object" "froyo_pdfs_upload_to_gcs" {
  for_each = fileset("${jsondecode(file("unzip_info.json")).tmp_dir}/froyo_recipe_pdfs", "**/*")
  source   = "${jsondecode(file("unzip_info.json")).tmp_dir}/froyo_recipe_pdfs/${each.value}"
  name     = "datasets/froyo_recipe_pdfs/${each.value}"
  bucket   = local.data_and_code_bucket
  depends_on = [
    null_resource.unzip_datasets,
    time_sleep.sleep_after_bucket_creation
  ]
}


/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/

resource "time_sleep" "sleep_after_network_and_storage_steps" {
  create_duration = "120s"
  depends_on = [
      time_sleep.sleep_after_network_and_firewall_creation,
      time_sleep.sleep_after_bucket_creation
  ]
}



/******************************************
9b. BigQuery dataset creation
******************************************/

resource "google_bigquery_dataset" "bq_dataset_creation" {
  project                     = local.project_id
  dataset_id                  = local.bq_dataset
  location                    = "US"
}

/******************************************
10. Cloud Composer 3 creation
******************************************/

resource "google_composer_environment" "cloud_composer_env_creation" {
  project = local.project_id
  name   = "${local.project_id}-cc3"
  region = local.location
  provider = google-beta
  config {

    software_config {
      image_version = local.CLOUD_COMPOSER3_IMG_VERSION 
      env_variables = {
        AIRFLOW_VAR_CODE_BUCKET = "${local.data_and_code_bucket}"
        AIRFLOW_VAR_PROJECT_ID = "${local.project_id}"
        AIRFLOW_VAR_REGION = "${local.location}"
        AIRFLOW_VAR_SUBNET = "${local.spark_subnet_nm}"
        AIRFLOW_VAR_BQ_DATASET = "${local.bq_dataset}"
        AIRFLOW_VAR_UMSA = "${local.umsa}"
        AIRFLOW_VAR_SPARK_RUNTIME_VERSION = "${local.S8S_SPARK_RUNTIME_VERSION}"
      }
    }

    node_config {
      network    = local.vpc_nm
      subnetwork = local.spark_subnet_nm
      service_account = local.umsa_fqn
    }
  }

  depends_on = [
    time_sleep.sleep_after_network_and_storage_steps
  ] 

  timeouts {
    create = "75m"
  } 
}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_composer_creation" {
  create_duration = "180s"
  depends_on = [
      google_composer_environment.cloud_composer_env_creation
  ]
}

/*******************************************
11. Upload Airflow DAG to Managed Service for 
Apache Airflow DAG bucket
******************************************/
resource "google_storage_bucket_object" "upload_cc_dag_to_airflow_dag_bucket" {
  name   = "dags/pipeline.py"
  source = "../scripts/airflow-dag/pipeline.py"
  bucket = substr(substr(google_composer_environment.cloud_composer_env_creation.config.0.dag_gcs_prefix, 5, length(google_composer_environment.cloud_composer_env_creation.config.0.dag_gcs_prefix)), 0, (length(google_composer_environment.cloud_composer_env_creation.config.0.dag_gcs_prefix)-10))
  depends_on = [
    time_sleep.sleep_after_composer_creation
  ]
}

/*******************************************
12. Create Iceberg REST catalog in 
Lakehouse Runtime Catalog service
******************************************/
resource "null_resource" "iceberg_catalog" {
  provisioner "local-exec" {
    command = "gcloud alpha biglake catalogs create ${local.lakehouse_iceberg_catalog_name} --project=${local.project_id} --location=${local.location} --type=ICEBERG --impersonate-service-account=${local.umsa_fqn}"
  }
  depends_on = [
    google_storage_bucket.iceberg_warehouse_bucket_creation,
    time_sleep.sleep_after_composer_creation
  ]
}


/******************************************
13. Create Hive catalog in
Lakehouse Runtime Catalog service
******************************************/
resource "null_resource" "hive_catalog" {
  provisioner "local-exec" {
    command = "gcloud alpha biglake catalogs create ${local.lakehouse_hive_catalog_name} --project=${local.project_id} --location=${local.location} --type=HIVE --hive-options=location-uri=gs://${local.lakehouse_hive_warehouse_bucket} --impersonate-service-account=${local.umsa_fqn}"
  }
  depends_on = [
    google_storage_bucket.hive_warehouse_bucket_creation,
    time_sleep.sleep_after_composer_creation
  ]
}


/******************************************
14. Output important variable
******************************************/

output "PROJECT_ID" {
  value = local.project_id
}

output "PROJECT_NBR" {
  value = local.project_nbr
}

output "LOCATION" {
  value = local.location
}

output "VPC_NM" {
  value = local.vpc_nm
}

output "SPARK_SERVERLESS_SUBNET" {
  value = local.spark_subnet_nm
}


output "UMSA_FQN" {
  value = local.umsa_fqn
}

output "CODE_AND_DATA_BUCKET" {
  value = local.data_and_code_bucket
}

output "CLOUD_COMPOSER_DAG_BUCKET" {
  value = substr(substr(google_composer_environment.cloud_composer_env_creation.config.0.dag_gcs_prefix, 5, length(google_composer_environment.cloud_composer_env_creation.config.0.dag_gcs_prefix)), 0, (length(google_composer_environment.cloud_composer_env_creation.config.0.dag_gcs_prefix)-10))
}

output "LAKEHOUSE_ICEBERG_CATALOG_NAME" {
  value = local.lakehouse_iceberg_catalog_name
}

output "LAKEHOUSE_HIVE_CATALOG_NAME" {
  value = local.lakehouse_hive_catalog_name
}

/******************************************
DONE
******************************************/
