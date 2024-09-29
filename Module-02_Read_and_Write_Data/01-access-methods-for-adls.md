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

## 04. Access ALDS using `Cluster scoped authentication`

## 05. Access ADLS using `Microsoft Entra ID credentials pass-through`
