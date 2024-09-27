# Read and Write data using Azure Databricks

In this section, we will learn following concepts:

- Mounting Azure Data Lake Storage Gen2 (ADLS Gen2) and Azure Blob storage to Azure Databricks File System (DBFS)
- Reading and writing data from and to Azure Blob storage
- Reading and writing data from and to ADLS Gen2
- Reading and writing data from and to an Azure SQL database using native connectors
- Reading and writing data from and to Azure Synapse Dedicated Structured Query Language (SQL) Pool using native connectors
- Reading and writing data from and to the Azure Cosmos DB
- Reading and writing data from and to CSV and Parquet
- Reading and writing data from and to JSON, including nested JSON

## Technical requirements

- An Azure subscription (Active)
- ADLS Gen2 and Blob storage accounts
- Azure SQL DB and Azure Synapse Dedicated SQL Pool
  - For creating Azure Synapse Analytics and Dedicated SQL Pool, you may refer this link: https://learn.microsoft.com/en-us/azure/synapse-analytics/quickstart-create-sql-pool-studio
- An Azure Cosmos DB account

  - You can find out more info about this here: https://learn.microsoft.com/en-us/azure/cosmos-db/nosql/quickstart-portals

- You can find the scripts for this chapter in [scripts](./scripts/) folder of this module which contains all the notebooks written for this module.

## Step-01: Mounting ADLS Gen2 and Azure Blob storage to Azure Databricks DBFS

### 1.1 Create ADLS Gen2 and Azure Blob storage accounts

- **binadlsgen2storage** - ADLS Gen2 storage account
- **binblobstorage** - Azure Blob storage account

### 1.2 Create a Service principal for mounting ADLS to DBFS

- Azure portal >> **Microsoft Entra ID** >> **App Registrations** >> **New Registrations**.
- **Name**: ADLSGen2App
- **Who can use this application or access this API?**: Accounts in this Organizational directory only (single tenants).
- Click on **Register** button.
- Once an application is created, you will see it listed on the **App registrations** page of Microsoft Entra ID.

- To create a secret, click on **Certificates & secrets** under the _Manage heading_ >>
  click on the **+ New client secret** option listed under _Client secrets_.
- You can provide any description for the secret and provide expiry as 1 year for this lab.
- Make sure you copy the value of the _secret_, else you cannot get the value of the existing secret later. You will have to create a new secret if the secret value is not copied immediately after it is created.

- You now have an _application ID_, a _tenant ID_, and a _Secret_. These are required to
  mount an ADLS Gen2 account to DBFS.

### 1.3 Provide necessary access to ADLSGen2App on the ADLS Gen2 storage account

- Azure portal >> **Storage Accounts** >> select **binadlsgenstorage** storage account that you created in _step#1.1_.
- Click **Access Control (IAM)** >> **+ Add** >> select the **Add role assignment** option.
- On the Add role assignment blade, assign the **Storage Blob Data Contributor** role to our service principal (that is, ADLSGen2App):
  - **Role**: Storage Blob Data Contributor
  - **Assign access to**: User group or Service Principal
  - **Select**: ADLSGen2App

### 1.4 Get a Storage Access Key of ADLS (storage account)

### 1.5 Write Notebook to mount Azure Blob storage & ADLS on DBFS

- You can find the notebook that we will be using to mount Azure Blob storage and
  ADLS Gen2 in the Chapter02 folder of

- Create a container named rawdata in both the cookbookadlsgen2storage and binblobstorage accounts you have already created, and upload the Orders.csv file, which you will find in the [scripts](./scripts/) folder of this module.
