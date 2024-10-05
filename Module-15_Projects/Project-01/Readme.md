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
- Upload all the files to Azure Data Lake Storage _raw_ container we created in the last step.

## Step-XX: Create and Configure `Service principal` for Azure Databricks

### Register a new App (service principal)

- Azure Portal >> Microsoft Entra ID >> Under **Manage** section, select **App Registrations**
- Click New Registrations

  - **Name**:
  - **Who can use this application or access this API?**: Accounts in this organizational directory only (Default Directory only - Single tenant)
  - Click on **Register** button

- **IMP**: Copy the following information about the application, as we'll need it in later steps.:
  - **Client ID (App ID)**
  - **Tenant ID**

### Generate the `Client secret` for the App

- **Microsoft Entra ID** >> **Manage** >> **App Registrations** >> Select **All application** tab >> Select your application
- From the left-side panel, expand Manage section >> Certificates & Secrets
- Select the **Client secrets** tab >> **New client secret**
  - Description
  - Expires: 180 days
- - **IMP**: Copy the client secret and store it at a safe place. We'll need it in later steps.

## Step-XX: Authorize the Service Principal to access ADLS objects (RBAC)

- Navigate to the **Storage Accounts** service >> select the storage account that we created for our datasets in Step#3.
- Select Access Control (IAM) >> Add >> Add Role Assignment
- **Role** tab
  - select Job functions role tab >> select Storage Data Blob Contributor >> Next
- **Members** tab
  - Selected role:
  - Assign access to: <select_the_sp_created_in_last_step>
- **Conditions** tab: <leave_to_defaults>
- **Review and Create**

## Step-XX: Create an Azure Key Vault for storing secrets

- Azure portal >> **Key Vaults** >> Click on **Create** button
- **Basics** tab
  - Subscription
  - Resource group
  - Key vault name: databricks-labs-vault
  - Region: US East
  - Pricing Tier: Standard
  - Days to retain deleted vaults: 7
  - Purge protection: Disable
- **Access Configuration** tab
  - Permission model: Vault access policy
- **Networking** tab

  - Enable public access: Enable
  - Allow access from: All Networks

- **Review & Create**

## Step-XX: Upload the secrets (service principal) in Azure Key Vault

- Select the Azure Key Vault you created in the preceding step >> Expand **Objects** section >> **Secrets**

- Create three secrets in azure key vault, for:
  1. service principal **client_id**
  2. service principal **tenant_id**
  3. service principal **client_secret**

## Step-XX: Create an Azure Databricks workspace and Cluster

- Navigate to Azure Portal >> **Azure Databricks** >> **Create**
- **Basics** tab
  - Workspace name: adb-workspace
  - Region: East US
  - Pricing Tier: Trial
  - Managed Resource Group name:
- Leave the Networking, Encryption, Security & Compliance tab values to defaults
- **Review & Create**

## Step-XX: Create a Secret scope for Databricks cluster

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

### 3.2 Create a Notebook to ingest the .csv dataset

- Navigate to your Azure Databricks workspace >> Select folder "proj-01-nbs" >> New Notebook
- For notebook code, kindly refer [mount-adls-container-ondbfs.ipynb](./notebooks/mount-adls-container-ondbfs.ipynb) notebook from _notebooks_ folder of this module.

## Step-XX: Creating Azure Databricks Workflow

## Step-XX: Performing Transformations

## Step-XX: Aggregating the Data

## Step-XX: Writing the data to sink storage service

## Step-XX: Automated deployment using CI/CD Pipelines
