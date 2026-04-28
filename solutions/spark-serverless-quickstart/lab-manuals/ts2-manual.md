*Copyright 2026 Google LLC*

*Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at*

*http://www.apache.org/licenses/LICENSE-2.0*

*Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.*


<hr>

# Technical Solution 2: Spark notebooks with interactive serverless Managed Service for Apache Spark on Google Cloud

## 1.0. About the lab

### 1.1. Abstract
This lab is **introductory** in nature and showcases **serverless Managed Service for Apache Spark - Interactive Sessions** feature.  The goal of the lab is to demystify this feature through a (zero fluff, zero dazzle) minimum viable end to end sample to accelerate adoption. This hands-on lab complements the blog post [Lakehouse Demystified - Part 3: Managed service for Apache Spark - serverless interactive sessions for Spark notebooks]() and uses the same infrastructure as technical solution 1.


|  |  | 
| -- | :--- | 
| Technical Focus| **Spark notebooks powered by serverless interactive Managed Service for Apache Spark on Google Cloud** |
| Use case |  Chicago Crimes Analysis |
| Notebook platforms |  Colab notebooks in BigQuery Studio<br>Notebooks on Vertex AI Workbench<br>Notebooks on BYO Jupyter<br>Notebooks in VS Code |

#### What to expect:

See the "Lab Flow" section below.

<hr>

### 1.2. Lab format

- Is fully scripted - the entire solution is provided, and with instructions
- Is self-paced/self-service

<hr>


### 1.3. Duration
The hands-on lab takes ~0.5 hours or less to complete

<hr>

### 1.4. Prerequisites

- Completion of [technical solution 1](./ts1-manual.md).
- VS Code installed on your machine if you want to try out Spark notebooks powered by serverless interactive sessions

<hr>

### 1.5. Resources provisioned
We will reuse the resources provisioned in [technical solution 1](./ts1-manual.md), and only create something called 'Interactive Sessions Template'.

<hr>

### 1.6. Audience

- A quick read for architects
- Targeted for hands on practitioners, especially data engineers, data analysts and data scientists

<hr>

### 1.7. Table of contents

|Section | Focus | 
| -- | :--- | 
| 1.8 | [ARCHITECTURE: Reference architcture and lab solution architecture](./ts2-manual.md#18-architecture) |
| 1.9 | [ABOUT THE LAB: What to expect from this lab](./ts2-manual.md#19-lab-flow) |
| 2.0 | [PRODUCT: Highlights of the featured product](https://github.com/anagha-google/lakehouse-solutions-build/blob/TS2/solutions/spark-serverless-quickstart/lab-manuals/ts2-manual.md#2-product-highlights) |
| 3.1 | [SETUP: Initialize active gcloud configuration](./ts2-manual.md#31-setup----initialize-active-gcloud-configuration) |
| 3.2 | [SETUP: Create an interactive sessions template for Spark Connect, for PySpark and using a Service Account](./ts2-manual.md#32-setup---create-an-interactive-sessions-template-for-spark-connect-for-pyspark-and-using-a-service-account) |
| 3.3 | [SETUP: Create an interactive sessions template for Jupyter, for PySpark and using a Service Account](./ts2-manual.md#33-setup---create-an-interactive-sessions-template-for-jupyter-for-pyspark-and-using-a-service-account) |
| 3.4 | [SETUP: Provision Gemini Enterprise Workbench instance](./ts2-manual.md#34-setup---provision-gemini-enterprise-workbench-instance) |
| 3.5 | [SETUP: Install local Jupyter on your machine](./ts2-manual.md#35-setup---install-local-jupyter-on-your-machine) |
| 4.1 | [LAB: Execute a Spark notebook in Colab in BigQuery studio with Spark Connect](./ts2-manual.md#41-execute-a-spark-notebook-in-colab-in-bigquery-studio-with-spark-connect) |
| 4.2 | [LAB: Execute a Spark notebook on Gemini Agent Platform Workbench](./ts2-manual.md#42-execute-a-spark-notebook-on-gemini-agent-platform-workbench) |
| 4.3 | [LAB: Execute a Spark notebook on local Jupyter on your machine](./ts2-manual.md#43-execute-a-spark-notebook-on-local-jupyter-on-your-machine) |
| 4.4 | [LAB: Execute a Spark notebook in VS Code on your machine after installing Google Cloud Data Agent Kit extension (DAK)](./ts2-manual.md#44-execute-a-spark-notebook-in-vs-code-on-your-machine-after-installing-google-cloud-data-agent-kit-extension-dak) |
| 4.5 | [TIP: Creating an interactive session with gcloud command and reusing across notebooks](./ts2-manual.md#45-using-an-interactive-session-across-notebooks) |
| 5.0 | [PICTORIAL OVERVIEW: Data Agent Kit extension for VS Code](./ts2-manual.md#5-pictorial-overview-of-google-clouds-data-agent-kit-for-vs-code) |


Note: The catalog component, Lakehouse runtime catalog is covered in the next technical solution.

<hr>

### 1.8. Architecture

#### 1.8.1. Serverless Managed Service for Apache Spark - Integration Landscape

Please refer to the blog post [Lakehouse Demystified - Part 2: Managed service for Apache Spark - serverless batches]() for an explanation of this diagram.

![README](../images/ts2-02.png)   
<br><br>

<hr>

#### 1.8.2. Reference Architecture

Please refer to the [Lakehouse Demystified - Part 3: Managed service for Apache Spark - serverless interactive sessions for Spark notebooks]() blog post for a narrative.

![README](../images/ts2-03.png)   
<br><br>

A more simplistic version...

![README](../images/ts2-01.png)   
<br><br>

<hr>

#### 1.8.3. Lab Solution Architecture

In the lab, we will simulate a data analyst that reads data, transforms it a bit for analysis and visualizes it. We will read Chicago crimes public dataset in BigQuery with the Spark BigQuery connector from Google Cloud, from the different notebook flavors. We will analyze crimes and visualize them in the notebooks. Note that the lab is less about Apache Spark and more about the serverless Spark sessions support in notebooks, so the labs are basic.

![README](../images/ts2-05.png)   
<br><br>

<hr>


### 1.9. Lab Flow

![README](../images/ts2-04.png)   
<br><br>

<hr>

### 1.10. For success

Read the blog posts and the lab narrative below, review the code, and then start trying out the lab.

<br>

<hr>

### 1.11. Product direction

Google Cloud is strategically investing in support for Spark Connect. 

<hr>
<hr>


# 2. Product Highlights

Read the blog post [Lakehouse Demystified - Part 2: Managed service for Apache Spark - serverless batches]() for a comprehensive of managed serverless Spark on Google Cloud. Please refer to the [Lakehouse Demystified - Part 3: Managed service for Apache Spark - serverless interactive sessions for Spark notebooks]() blog post for comprehensive coverage on interactive sessions.

## 2.1.  Interactive sessions
Interactive sessions on serverless Managed Service for Apache Spark are essentially dedicated serverless Spark infrastructure that can be attached to notebooks for interactive authoring of Spark code. There are two options to connect notebooks to interactive sessions - Spark Connect and Jupyter. BigQuery studio offers Colab notebooks that work with Spark Connect, any Jupyter compatible notebook platform works well with remote Jupyter kernels. Google Cloud's Data Cloud Extension for IDEs allow you to author notebooks in your IDEs with remote Jupyter kernels. 

## 2.2. Language support
Google Cloud is strategically investing in supporting PySpark. 

## 2.3. Interactive session templates
Interactive session templates are a construct that can be used to define the core and common serverless infrastructure specifications & guardrails - such as runtime versions, baseline resources for driver & executor, min-max thresholds for executors, custom containers, packages, metastore/catalog configurations and more. These promote consistency and reduce toil of defining the configs over and over again in each notebook. These templates can be imported into the Spark session creation command to take advantage of the predefined configurations. This is demonstrated in the lab. [Read the product documenation](https://docs.cloud.google.com/dataproc-serverless/docs/guides/create-serverless-sessions-templates)

## 2.4. Identity supported
Interactive sessions support both User Managed Service Accounts (UMSAs - non human application IDs) as well as end user credentials (EUC). You can configure this at the session template level.

<hr>
<hr>

# 3. Lab Setup

<hr>

## 3.1. Setup -  initialize active gcloud configuration

Run the following commands in Cloud Shell to authenticate and configure your active project. Ensure its the same project as technical solution 1.

1. Initialization:
```
gcloud init
```

2. Set the active Google Cloud project target:
```
gcloud config set project <YOUR_GCP_PROJECT_ID>
```

3. Set the quota project for ADC (Application Default Credentials):
```
gcloud auth application-default set-quota-project <YOUR_GCP_PROJECT_ID>
```

<hr>


## 3.2. Setup - create an interactive sessions template for Spark Connect, for PySpark and using a Service Account 

To recap - session template is to bootstrap of common configurations to standardize, reduce toil, and spped development/analysis cycles. 
1. There is no gcloud command to create session templates, so we will use the Console.
2. We will use a service account for the session template instead of individual user for simplicity. Our service account already has all permissions. This means that Spark will run and access data as the configured service account.

### 3.2.1. Capture the service account fully qualified name (FQN)

Navigate to Cloud IAM and find the service account as show below and copy it.

![README](../images/ts2-06.png)   
<br><br>

<hr>

### 3.2.2. Navigate to the Managed Service for Apache Spark 'session templates' UI

Follow the screenshots below to navigate to the Managed Service for Apache Spark 'session templates' UI

![README](../images/ts2-07a.png)   
<br><br>

![README](../images/ts2-07b.png)   
<br><br>

<hr>

### 3.2.3. Create the session template

Follow the screenshots below to create a session template. Ensure you make the same selections. For your reference there is [gcloud](https://docs.cloud.google.com/sdk/gcloud/reference/beta/dataproc/session-templates) as well as [terraform](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataproc_session_template) support for session template creation.

![README](../images/ts2-08a.png)   
<br><br>

<hr>

![README](../images/ts2-08b.png)   
<br><br>

<hr>

![README](../images/ts2-08c.png)   
<br><br>

<hr>

![README](../images/ts2-08d.png)   
<br><br>

![README](../images/ts2-08e.png)   
<br><br>

<hr>

![README](../images/ts2-08f.png)   
<br><br>

<hr>

![README](../images/ts2-08g.png)   
<br><br>

<hr>

## 3.3. Setup - create an interactive sessions template for Jupyter, for PySpark and using a Service Account 


Follow the screenshots below to create a session template. Ensure you make the same selections. Once again, for your reference there is [gcloud](https://docs.cloud.google.com/sdk/gcloud/reference/beta/dataproc/session-templates) as well as [terraform](https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/dataproc_session_template) support for session template creation.


![README](../images/ts2-09a.png)   
<br><br>

<hr>

![README](../images/ts2-09b.png)   
<br><br>

<hr>

![README](../images/ts2-09c.png)   
<br><br>

<hr>

![README](../images/ts2-09d.png)   
<br><br>

![README](../images/ts2-09e.png)   
<br><br>

<hr>

![README](../images/ts2-09f.png)   
<br><br>

<hr>


## 3.4. Setup - Provision Gemini Enterprise Workbench instance

Gemini Enterprise Workbench instance (formerly called Vertex AI Workbench instance) is an optional notebook server that can be created for Data Scientists and ML engineers to experiment. Spark notebooks powered by Managed Service for Apache Spark interactive sessions are supported. Lets create the workbench instance.

Follow the screenshots below to stand up Gemini Enterprise Workbench instance and start up a notebook with a session.

![README](../images/ts2-10a.png)   
<br><br>

![README](../images/ts2-10b.png)   
<br><br>

![README](../images/ts2-10c.png)   
<br><br>

![README](../images/ts2-10d.png)   
<br><br>

![README](../images/ts2-10e.png)   
<br><br>

![README](../images/ts2-10f.png)   
<br><br>

<hr>

Lets try a notebook.

![README](../images/ts2-10g.png)   
<br><br>

![README](../images/ts2-10h.png)   
<br><br>

![README](../images/ts2-10i.png)   
<br><br>

![README](../images/ts2-10j.png)   
<br><br>

![README](../images/ts2-10k.png)   
<br><br>

![README](../images/ts2-10l.png)   
<br><br>


<hr>

## 3.5. Setup - Install local Jupyter on your machine

This is the documentation page. https://docs.cloud.google.com/dataproc-serverless/docs/quickstarts/jupyterlab-sessions#install-jupyterlab-extension
The author used a slightly different approach as detailed below.

### 3.5.1. Authenticate to gcloud

1. Run the command below - it will use the credentials obtained from the login flow to also update the Application Default Credentials, making them available to client libraries
```
gcloud auth login --update-adc
```

2. Create a virtual environment
```
python -m venv .venv
source .venv/bin/activate
```

3. Install jupyterlab and more
```
pip install --upgrade pip
pip install jupyterlab
pip install bigquery-jupyter-plugin
pip install pyOpenSSL
```

4. Launch JupyterLab
```
jupyter lab
```

![README](../images/ts2-11a.png)   
<br><br>



5. This will open a browser window with a launcher as shown below. Click on the session template.
![README](../images/ts2-11b.png)   
<br><br>

6. Quick test

![README](../images/ts2-11c.png)   
<br><br>

<hr>
<hr>

# 4. Lab

<h4>



## 4.1. Execute a Spark notebook in Colab in BigQuery studio with Spark Connect

In this section, we will try the [notebook](../notebooks/chicago-crimes-analytics.ipynb) in Colab in BigQuery studio.

### 4.1.1. Navigate to Colab

Follow the screnshots below to navigate to Colab.

![README](../images/ts2-12a.png)   
<br><br>

![README](../images/ts2-12b.png)   
<br><br>

### 4.1.2. Upload the lab notebook

1. Grab the URL for the lab notebook from [here](../notebooks/chicago-crimes-analytics-spark-connect.ipynb).

2. Upload the notebook as shown the screenshots below-

![README](../images/ts2-12c.png)   
<br><br>

![README](../images/ts2-12d.png)   
<br><br>

![README](../images/ts2-12e.png)   
<br><br>

### 4.1.3. Create a Colab runtime (compute) & connect to it

Follow the screenshots below to set up a runtime & connect to it

![README](../images/ts2-12f.png)   
<br><br>

![README](../images/ts2-12g.png)   
<br><br>

![README](../images/ts2-12h.png)   
<br><br>

### 4.1.4. Create a spark session off of the session template we created

We created a session template for Spark Connect on [section 3.2.3](./ts2-manual.md#323-create-the-session-template). Here is how we create a spark session in the notebook that connects with a managed serverless interactive session (infrastructure) off of a predefined template.<br>
To customize the session further, refer [docs](https://docs.cloud.google.com/bigquery/docs/use-spark).<br>

1. Run the first cell to see understand what happens.

![README](../images/ts2-12i.png)   
<br><br>

2. It takes 2 minutes or less to create the interactive session. 
   
![README](../images/ts2-12i.png)   
<br><br>

3. Run the second cell

![README](../images/ts2-12n.png)   
<br><br> 

4. From the first executable cell, click on 'View Session Details'

![README](../images/ts2-12j.png)   
<br><br>

5. It opens the serverless 'Interactive Sessions' UI of the Managed Service for Apache Spark product. Click on 'View Spark UI'

![README](../images/ts2-12k.png)   
<br><br>

![README](../images/ts2-12l.png)   
<br><br>

6. It opens the serverless managed Spark UI

![README](../images/ts2-12m.png)   
<br><br>

7. Run the rest of the notebook

![README](../images/ts2-12o.png)   
<br><br>

The BigQuery Spark connector was used to query Bigquery. This concludes the tutorial for Spark in Colab notebook.

8. Spark session configuration based off the session template can be added to the notebook via the Colab UI as shown below.

![README](../images/ts2-12p.png)   
<br><br>

![README](../images/ts2-12q.png)   
<br><br>

![README](../images/ts2-12r.png)   
<br><br>

![README](../images/ts2-12s.png)   
<br><br>
   
<hr>
<hr>

## 4.2. Execute a Spark notebook on Gemini Agent Platform Workbench 
In section [3.4](./ts2-manual.md#34-setup---provision-gemini-enterprise-workbench-instance), we created a workbench instance. Lets upload the crimes notebook [here](../notebooks/chicago-crimes-analytics-jupyter.ipynb) and execute it, this time with Jupyter kernel.<br>
Note: File-Open from URL option does not work. Please download the notebook [here](../notebooks/chicago-crimes-analytics-jupyter.ipynb) to local, we will upload from local.

1. Open 'JupyterLab' on your workbench instance

![README](../images/ts2-13a.png)   
<br><br>

2. Download the notebook [here](../notebooks/chicago-crimes-analytics-jupyter.ipynb) locally.

3. Upload the notebook from local

![README](../images/ts2-13b.png)   
<br><br>

![README](../images/ts2-13c.png)   
<br><br>

4. Choose the interactive session kernel

![README](../images/ts2-13d.png)   
<br><br>

![README](../images/ts2-13f.png)   
<br><br>

![README](../images/ts2-13e.png)   
<br><br>

5. Open another browser tab and navigate to the Managed Service for Apache Spark interactive session UI

![README](../images/ts2-13i.png)   
<br><br>

6. Switch back to Jupyter, you should see the kernel ready

![README](../images/ts2-13j.png)   
<br><br>

7. Run all cells


![README](../images/ts2-13g.png)   
<br><br>

![README](../images/ts2-13h.png)   
<br><br>


This concludes the tutorial for Spark notebooks in Gemini Agent Platform Workbench instance.

<hr>
<hr>


## 4.3. Execute a Spark notebook on local Jupyter on your machine

In section [3.5](./ts2-manual.md#35-setup---install-local-jupyter-on-your-machine), we installed local Jupyter. Lets upload the same notebook as the section 3.7 in the Jupyter UI.

1. Upload the notebook like you did in the previous section.

![README](../images/ts2-14a.png)   
<br><br>

![README](../images/ts2-14b.png)   
<br><br>
   
3. Note the remote kernel automatically starting - defaults to the session template for Jupyter

![README](../images/ts2-14c.png)   
<br><br>

4. Note the interactive session being created

![README](../images/ts2-14d.png)   
<br><br>

5. Run the entire notebook like you did in the previous section.

![README](../images/ts2-14e.png)   
<br><br>

This concludes the tutorial for Spark notebooks in local Jupyter.

<hr>
<hr>

## 4.4. Execute a Spark notebook in VS Code on your machine after installing Google Cloud Data Agent Kit extension (DAK)

### 4.4.1. Install Google Cloud Data Agent Kit extension (DAK) in VS Code

Install VS Code if you dont already have it, and then follow the steps below.

![README](../images/ts2-15a.png)   
<br><br>

![README](../images/ts2-15b.png)   
<br><br>

![README](../images/ts2-15c.png)   
<br><br>

![README](../images/ts2-15d.png)   
<br><br>

<hr>

### 4.4.2. Login to Google Cloud in VS Code

![README](../images/ts2-15i.png)   
<br><br>

![README](../images/ts2-15e.png)   
<br><br>

![README](../images/ts2-15f.png)   
<br><br>

![README](../images/ts2-15g.png)   
<br><br>

![README](../images/ts2-15h.png)   
<br><br>

<hr>

### 4.4.3. Configure your Google Cloud project and location

![README](../images/ts2-15j.png)   
<br><br>

![README](../images/ts2-15k.png)   
<br><br>

![README](../images/ts2-15l.png)   
<br><br>



### 4.4.4. Clone this repo locally and open it in VS Code

![README](../images/ts2-15m.png)   
<br><br>

![README](../images/ts2-15n.png)   
<br><br>

![README](../images/ts2-15o.png)   
<br><br>

<hr>

### 4.4.5. Open the lab notebook for Jupyter

Follow the steps detailed in the screenshots below:

![README](../images/ts2-15p.png)   
<br><br>

<hr>

### 4.4.6. Select remote kernel & connect

Follow the steps detailed in the screenshots below:

![README](../images/ts2-15q.png)   
<br><br>

![README](../images/ts2-15r.png)   
<br><br>

![README](../images/ts2-15s.png)   
<br><br>

![README](../images/ts2-15t.png)   
<br><br>

![README](../images/ts2-15u.png)   
<br><br>


<hr>

### 4.4.7. Run the notebook

![README](../images/ts2-15v.png)   
<br><br>

![README](../images/ts2-15w.png)   
<br><br>

<hr>

### 4.5. Using an interactive session across notebooks

There is an unavoidable startup latency with interactive sessions. When there are cases where resource contention is not a concern, you can create an interactive session manually and connect multiple notebooks to it.

Here is the gcloud command.

```
gcloud beta dataproc sessions create spark $SESSION_NAME  \
--project=${PROJECT_ID} \
--location=${REGION} \
--subnet=$SUBNET \
--version $RUNTIME_VERSION
```

<hr>
<hr>

## 5. Pictorial overview of Google Cloud's Data Agent Kit for VS Code

I have a separate blog post on DAK, but here is a visual tour.

### 5.1. Features at a glance
![README](../images/ts2-15x.png)   
<br><br>

### 5.2. Skills at a glance

![README](../images/ts2-15y.png)   
<br><br>

![README](../images/ts2-15z.png)   
<br><br>

![README](../images/ts2-15aa.png)   
<br><br>


<hr>
<hr>


##### =====================================================================================================
##### THIS CONCLUDES THE LAB - SPARK NOTEBOOKS POWERED BY MANAGED SPARK SERVERLESS
##### SHUT DOWN THE LAB PROJECT TO AVOID BILLING OR PROCEED TO NEXT TECHNICAL SOLUTION
##### =====================================================================================================


