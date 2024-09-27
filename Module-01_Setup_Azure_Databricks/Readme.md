# Create and Setup Azure Databricks Workspace

In this section, we will learn following recipes:

- Creating an Azure Databricks workspace using Azure portal
- Creating an Azure Databricks workspace using the Azure CLI
- Creating an Azure Databricks workspace using Azure Resource Manager (ARM) templates
- Adding users and groups to the workspace
- Creating a cluster from the user interface (UI)
- Getting started with Notebooks and jobs in Azure Databricks
- Authenticating to Databricks using a personal access token (PAT)

## Technical requirements

- An Azure subscription (Active)
- Azure CLI installed
  - https://learn.microsoft.com/en-us/cli/azure/install-azure-cli
- As an Azure AD user, you will need the _Contributor_ role in your subscription and create the Azure Databricks service via the Azure portal.
- You must also be the _admin_ of the Azure Databricks workspace.
- The latest version of Power BI Desktop
  - https://www.microsoft.com/en-in/download/details.aspx?id=58494
  - We will use this to access
    Spark tables using PAT authentication.

## Step-01: Creating a Databricks workspace using Azure portal

- Log into the Azure portal (https://portal.azure.com) and click on Create a resource.
- Then, search for **Azure Databricks** and click on **Create** button.
- Enter the requested details:

  - **Basics**
    - Resource Group: Databricks-Labs-RG
    - Workspace Name: bin-db-workspace
    - Region: us-east-1
    - Pricing Tier: Standard
  - **Networking**: <leave_to_defaults>
  - **Advanced**: <leave_to_defaults>
  - **Review + Create**

- You can check the Azure Databricks workspace that you've created in the _Databricks-Labs-RG_ resource group.

## Step-02: Creating an Azure Databricks workspace using the Azure CLI

- This step will use a **service principal** to authenticate to Azure so that we can deploy the Azure Databricks workspace.

- You can find out how to create an Azure AD app and a SP from Azure portal using Microsoft identity platform | Microsoft Docs (https://learn.microsoft.com/en-us/entra/identity-platform/howto-create-service-principal-portal)

- Now, change the variable values that are used in the following powershell script:

```
# Script Variables
$appId=""
$appSecret=""
$tenantId=""
$subscriptionName=""
$resourceGroup = "Databricks-Labs-RG"

# Connecting to azure account using service principle
az login --service-principal --username $appId --password $appSecret --tenant $TenantId

# Set the subscription
az account set --subscription $subscriptionName

# Create a Resource group
az group create --name $resourceGroup --location "East US"

# Create an azure databricks workspace
az databricks workspace create --resource-group $resourceGroup --name BigDataWorkspace --location "East US" --sku standard

# List all the azure databricks workspace
az databricks workspace list
```

## Step-03: Creating a Databricks service using Azure Resource Manager (ARM) templates

## Step-04: Adding Users and Groups to the workspace

- In this step, we will learn how to add _users_ and _groups_ to the workspace so that they can collaborate when creating data applications.

- From the Azure Databricks service details page, click on the _Launch workspace_ option
- After launching the workspace, click the user icon at the top right >> click on **Admin Console**.

- You can click on **Add User** and invite users who are part of your Microsoft Entra ID (prev known as Azure AD).

- Here, I have added **demouser1@gmail.com**.

- You can grant admin privileges or allow cluster creation permissions based on your requirements while adding users.
- You can create a group from the admin console and add users to that group.
- You can do this to classify groups such as data engineers and data scientists and then provide access accordingly.

- Once we've created a group, we can add users to it, or we can add an ADD group to it. Here, I'm adding the Azure AD user to the group that we have created (**demouser1@gmail.com**)

## Step-05: Create an `Azure Databricks Cluster` from the Azure portals

- **Azure Databricks workspace** >> **Launch workspace** >> **Compute** >> Create Cluster.

- Enter the cluster details:
  - Cluster Type: All-Purpose
  - Cluster Name: DevCluster
  - Mode: Standard
  - Runtime Version:
  - Terminate after **15** mins of inactivity
  - Worker Type: <select_the_smallest_instance>

## Step-06: Getting started with `Notebooks` and `Jobs` in Azure Databricks

- In this step, we will learn:

  - how to import a notebook into our workspace?
  - how to create and execute a notebook?
  - how to execute and schedule a notebook using jobs?

- Ensure the Azure Databricks cluster is up and running.

### 6.1 Import the project and execute the Notebook

### 6.2 Schedule a Notebook using Jobs
