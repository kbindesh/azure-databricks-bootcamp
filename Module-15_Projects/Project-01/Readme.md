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

## Step-XX: Create Azure Key Vault for storing secrets

## Step-XX: Setup Azure Databricks workspace and Cluster

## Step-XX: Create a Service Principal for Azure DB to authenticate

## Step-03: Ingest the Data from ADLS using ADB Notebooks

### 3.1 Create a Notebook to mount the dataset from ADLS to DBFS (using service principal)

- Navigate to your Azure Databricks workspace >> Create a new folder "proj-01-nbs" >> New Notebook
- For notebook code, kindly refer notebook from notebooks folder of this module.

### 3.2 Ingesting multiple files

## Step-XX: Creating Azure Databricks Workflow

## Step-XX: Performing Transformations

## Step-XX: Aggregating the Data

## Step-XX:
