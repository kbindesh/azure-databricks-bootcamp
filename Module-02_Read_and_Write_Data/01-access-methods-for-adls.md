# Authentication mechanisms for accessing ADLS from Databricks Notebooks

## 01. Access ADLS using `Access keys`

```
# Set the ADLS access key configuration
spark.conf.set (
  "fs.azure.account.key.<storage_account_name>.dfs.core.windows.net",
  "<storage_account_access_key>"
)

# List the contents of above ADLS
display(dbutils.fs.ls("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net"))

# Read any file from ADLS (e.g reading csv file here)
display(spark.read.csv("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<filename>.csv"))
```

## 02. Access ALDS using `SAS Tokens`

## 03. Access ALDS using `Service Principal`

- Declare & initialize variables for storing service principal details:

```
client_id = dbutils.secrets.get(scope="project1-scope",key="clientid")
tenant_id = dbutils.secrets.get(scope="project1-scope",key="tenantid")
client_secret = dbutils.secrets.get(scope="project1-scope",key="clientsecret")
storage_account = "databrickslabsadls"
```

- Set configurations

```
spark.conf.set("fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", client_id)
spark.conf.set("fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net", client_secret)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", "https://login.microsoftonline.com/{tenant_id}/oauth2/token")
```

- Now, list all the files present in ADLS container.

```
display(dbutils.fs.ls("abfss://<container>@<storage_account>.dfs.core.windows.net/"))
```

## 04. Access ALDS using `Cluster scoped authentication`

## 05. Access ADLS using `Microsoft Entra ID credentials pass-through`
