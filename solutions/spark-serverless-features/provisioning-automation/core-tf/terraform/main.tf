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
solution_prefix                     = "froyo-lab"
project_id                          = "${var.project_id}"
project_name                        = "${var.project_id}"
project_nbr                         = "${var.project_number}"
admin_upn_fqn                       = "${var.gcp_account_name}"
location                            = "${var.gcp_region}"
umsa                                = "${local.solution_prefix}-umsa"
umsa_fqn                            = "${local.umsa}@${local.project_id}.iam.gserviceaccount.com"
spark_bucket                        = "${local.solution_prefix}-spark-bucket-${local.project_nbr}"
spark_bucket_fqn                    = "gs://{local.spark_bucket}-${local.project_nbr}"
vpc_nm                              = "${local.solution_prefix}-vpc-${local.project_nbr}"
spark_subnet_nm                     = "spark-froyo-snet"
spark_subnet_cidr                   = "10.2.0.0/16"
bq_dataset                          = "froyo_ds"
CC_GMSA_FQN                         = "service-${local.project_nbr}@cloudcomposer-accounts.iam.gserviceaccount.com"
GCE_GMSA_FQN                        = "${local.project_nbr}-compute@developer.gserviceaccount.com"
CLOUD_COMPOSER3_IMG_VERSION         = "${var.cloud_composer_image_version}"
S8S_SPARK_RUNTIME_VERSION           = "${var.spark_runtime_version}"
lakehouse_staging_bucket            = "froyo-lakehouse-staging-${local.project_nbr}"
lakehouse_hive_catalog_name         = "froyo_hive_lakehouse_catalog_${local.project_nbr}"
lakehouse_iceberg_catalog_name      = "froyo_iceberg_lakehouse_catalog_${local.project_nbr}"
lakehouse_code_bucket               = "${local.solution_prefix}-code-bucket-${local.project_nbr}"
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


resource "google_storage_bucket" "lakehouse_staging_bucket_creation" {
  name                              = local.lakehouse_staging_bucket
  project                           = local.project_id
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  
}

resource "google_storage_bucket" "lakehouse_code_bucket_creation" {
  name                              = local.lakehouse_code_bucket
  project                           = local.project_id
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  
}

resource "google_storage_bucket" "lakehouse_iceberg_catalog_bucket_creation" {
  name                              = local.lakehouse_iceberg_catalog_name
  project                           = local.project_id
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  
}

resource "google_storage_bucket" "lakehouse_hive_catalog_bucket_creation" {
  name                              = local.lakehouse_hive_catalog_name
  project                           = local.project_id
  location                          = local.location
  uniform_bucket_level_access       = true
  force_destroy                     = true
  
}

/*******************************************
Introducing sleep to minimize errors from
dependencies having not completed
********************************************/
resource "time_sleep" "sleep_after_bucket_creation" {
  create_duration = "60s"
  depends_on = [
    google_storage_bucket.lakehouse_code_bucket_creation,
    google_storage_bucket.spark_bucket_creation,
    google_storage_bucket.lakehouse_staging_bucket_creation,
    google_storage_bucket.lakehouse_iceberg_catalog_bucket_creation,
    google_storage_bucket.lakehouse_hive_catalog_bucket_creation
  ]
}

/******************************************
8a. Copy the Pyspark scripts to lakehouse_code_bucket
 *****************************************/

resource "google_storage_bucket_object" "pyspark_scripts_upload_to_gcs" {
  for_each = fileset("../scripts/pyspark/", "**/*")
  source = "../scripts/pyspark/${each.value}"
  name = "scripts/pyspark/${each.value}"
  bucket = "${local.lakehouse_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
8b. Copy the notebooks scripts to lakehouse_code_bucket
 *****************************************/

resource "google_storage_bucket_object" "notebooks_upload_to_gcs" {
  for_each = fileset("../notebooks/", "**/*")
  source = "../notebooks/${each.value}"
  name = "notebooks/${each.value}"
  bucket = "${local.lakehouse_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
8c. Copy the Airflow DAG scripts to lakehouse_code_bucket
 *****************************************/

resource "google_storage_bucket_object" "airflow_dag_upload_to_gcs" {
  name   = "scripts/airflow-dag/pipeline.py"
  source = "../scripts/airflow-dag/pipeline.py"
  bucket = "${local.lakehouse_code_bucket}"
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
8d. Copy the data to lakehouse_staging_bucket
 *****************************************/

locals {
  unzip_and_upload_command = <<EOT
      set -e
      TMP_DIR_BASE=$(mktemp -d)

      # Process froyo_data.tgz
      TMP_FROYO_DATA_DIR="$TMP_DIR_BASE/froyo-data"
      mkdir -p "$TMP_FROYO_DATA_DIR"
      tar -xzf ../datasets/froyo_data.tgz --strip-components=1 -C "$TMP_FROYO_DATA_DIR"

      # Process froyo_recipe_pdfs-1.tgz and froyo_recipe_pdfs-2.tgz
      TMP_FROYO_RECIPES_PDFS_DIR="$TMP_DIR_BASE/froyo-recipes-pdfs"
      mkdir -p "$TMP_FROYO_RECIPES_PDFS_DIR"
      tar -xzf ../datasets/froyo_recipe_pdfs-1.tgz --strip-components=1 -C "$TMP_FROYO_RECIPES_PDFS_DIR"
      tar -xzf ../datasets/froyo_recipe_pdfs-2.tgz --strip-components=1 -C "$TMP_FROYO_RECIPES_PDFS_DIR"

      # Process froyo_recipe_ingredient_pdfs.tgz
      TMP_FROYO_INGREDIENTS_PDFS_DIR="$TMP_DIR_BASE/froyo-recipe-ingredients-pdfs"
      mkdir -p "$TMP_FROYO_INGREDIENTS_PDFS_DIR"
      tar -xzf ../datasets/froyo_recipe_ingredient_pdfs.tgz --strip-components=1 -C "$TMP_FROYO_INGREDIENTS_PDFS_DIR"

      # Remove .DS_Store files before upload
      find "$TMP_DIR_BASE" -name ".DS_Store" -delete

      # Upload to GCS
      gsutil -m cp -r "$TMP_FROYO_DATA_DIR"/* gs://${local.lakehouse_staging_bucket}/froyo-data/
      gsutil -m cp -r "$TMP_FROYO_RECIPES_PDFS_DIR"/* gs://${local.lakehouse_staging_bucket}/froyo-recipes-pdfs/
      gsutil -m cp -r "$TMP_FROYO_INGREDIENTS_PDFS_DIR"/* gs://${local.lakehouse_staging_bucket}/froyo-recipe-ingredients-pdfs/

      rm -rf "$TMP_DIR_BASE"
EOT
}

resource "null_resource" "unzip_and_upload_froyo_recipes" {
  provisioner "local-exec" {
    command = local.unzip_and_upload_command
  }
  triggers = {
    recipe_archives_hash = sha1(join("", [
      for f in fileset("../datasets/", "{froyo_data.tgz,froyo_recipe_pdfs-1.tgz,froyo_recipe_pdfs-2.tgz,froyo_recipe_ingredient_pdfs.tgz}") : filesha1("../datasets/${f}")
    ]))
    command_hash = sha1(local.unzip_and_upload_command)
  }
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

resource "google_storage_bucket_object" "files_upload_to_gcs" {
  for_each = toset(compact([for f in fileset("../datasets", "{*.zip,*.csv,*.parquet}") : (substr(f, -9, -1) == ".DS_Store" ? "" : f)]))
  source   = "../datasets/${each.value}"
  name     = "data/${each.value}"
  bucket   = local.lakehouse_staging_bucket
  depends_on = [
    time_sleep.sleep_after_bucket_creation
  ]
}

/******************************************
9b. BigQuery dataset creation
******************************************/

resource "google_bigquery_dataset" "bq_dataset_creation" {
  project                     = local.project_id
  dataset_id                  = local.bq_dataset
  location                    = local.location
}

/******************************************
10. Cloud Composer 3 creation
******************************************/

resource "google_composer_environment" "cloud_composer_env_creation" {
  project   = local.project_id
  name      = "${local.solution_prefix}-cc3"
  region    = local.location
  provider  = google-beta
  config {
    software_config {
      image_version = local.CLOUD_COMPOSER3_IMG_VERSION 
      env_variables = {
        AIRFLOW_VAR_CODE_BUCKET = "${local.lakehouse_code_bucket}"
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
    time_sleep.sleep_after_network_and_firewall_creation,
    time_sleep.sleep_after_bucket_creation,
    google_bigquery_dataset.bq_dataset_creation
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

/******************************************
12a. Install gcloud alpha components
******************************************/
resource "null_resource" "gcloud_alpha_install" {
  provisioner "local-exec" {
    command = "gcloud components install alpha -q"
  }
}

/*******************************************
12b. Create Iceberg REST catalog in 
Lakehouse Runtime Catalog service
******************************************/
resource "null_resource" "iceberg_catalog" {
  depends_on = [
    time_sleep.sleep_after_bucket_creation,
    null_resource.gcloud_alpha_install
  ]
  provisioner "local-exec" {
    command = "gcloud alpha biglake iceberg catalogs create ${local.lakehouse_iceberg_catalog_name} --project=${local.project_id} --catalog-type=gcs-bucket --credential-mode=end-user --primary-location=${local.location}"
  }
}


/******************************************
13. Create Hive catalog in
Lakehouse Runtime Catalog service
******************************************/
resource "null_resource" "hive_catalog" {
  depends_on = [
    null_resource.gcloud_alpha_install,
    time_sleep.sleep_after_bucket_creation
  ]
  provisioner "local-exec" {
    command = "gcloud alpha biglake hive catalogs create  ${local.lakehouse_hive_catalog_name} --project=${local.project_id} --location-uri=gs://${local.lakehouse_hive_catalog_name} --primary-location=${local.location} --impersonate-service-account=${local.umsa_fqn}"
  }
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

output "CODE_BUCKET" {
  value = local.lakehouse_code_bucket
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
