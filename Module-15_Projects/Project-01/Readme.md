# Project-01: End-to-End Data processing using Azure Databricks

## Step-01: Project Overview

## Step-02: Solution Architecture

## Step-XX: Create ADLS Gen2 for storing Raw data

### Create an ADLS Gen2 instance

- Subscription: <your_subscription_name>
- Resource Group: <rg_name>
- Storage Account Name: bindeshdatalake
- Region: US East
- Performance: Standard
- Redundancy: LRS
- Enable Hierarchical namespace: ENABLE
- Soft delete options: DISABLE
- Leave Encryption and Tags values to defaults

### Create a Containers inside the ADLS Gen2 instance

- **Container-01**

  - Name: rawdata
  - Public access level: Private (no anonymous access)

- **Container-02**

  - Name: processed-data
  - Public access level: Private (no anonymous access)

- **Container-03**

  - Name: presentation-data
  - Public access level: Private (no anonymous access)

- **Container-04**
  - Name: random
  - Public access level: Private (no anonymous access)

## Step-XX: Upload the Datasets

- Refer to the [datasets](./datasets) folder of this module to get all the relevant datasets.

## Step-XX: Create a Service Principal for Azure Databricks to authenticate

## Step-XX: Create an Azure Key Vault for storing secrets

## Step-XX: Upload the secrets (service principal) in Azure Key Vault

- Create three secrets in azure key vault, for:
  1. service principal **client_id**
  2. service principal **tenant_id**
  3. service principal **client_secret**

## Step-XX: Create an Azure Databricks workspace and Cluster

- Navigate to Azure Portal >> Azure Databricks

## Step-XX: Create a secret scope for Databricks cluster

- This secret scope will be used for authentication databricks to access ADLS data.
- To create secret scope, kindly navigate to the following link:

```
# Navigate to the home page of databricks workspace
https://adb-4169435311569004.4.azuredatabricks.net/?o=4169435311569004#secrets/createScope
```

## Step-03: Ingest the Data from ADLS using ADB Notebooks

### 3.1 Create a Notebook to mount the dataset from ADLS to DBFS (using service principal)

- Navigate to your Azure Databricks workspace >> Create a new folder "proj-01-nbs" >> New Notebook
- For notebook code, kindly refer [mount-adls-container-ondbfs.ipynb](./notebooks/mount-adls-container-ondbfs.ipynb) notebook from _notebooks_ folder of this module.

## Step-XX: Creating Azure Databricks Workflow

## Step-XX: Performing Transformations

## Step-XX: Aggregating the Data

## Step-XX: Writing the data to sink storage service

## Step-XX: Automated deployment using CI/CD Pipelines
