# Securing Access to ADLS

## Step-01: Create an Azure Key Vault

## Step-02: Create a Secret scope

## Step-03: Using Databricks Secrets utility (dbutils.secrets)

- You may use databricks secrets utility (dbutils.secrets) to read the secrets from databricks secrets scope and azure key vault.

```
# To get help regarding dbutils.secrets
dbutils.secrets.help()

# List all the databricks secrets scope
dbutils.secrets.listscopes()

# Lists the metadata for secrets within the specified scope
dbutils.secrets.list("scope_names")

# To get the string representation of a secret value for the specified secrets scope and key
dbutils.secrets.get(scope="bin-scope", key="adls-access-key")
```

## Step-04: Using Secrets to access ADLS using Databricks Notebooks

```
# Get the access key from the Databricks secrets scope
adls_access_key = dbutils.secrets.get(scope="bin-scope", key="adls-access-key")

# Set the ADLS access key configuration securely without exposing the keys
spark.conf.set (
  "fs.azure.account.key.<storage_account_name>.dfs.core.windows.net", adls_access_key
)

# List the contents of above ADLS
display(dbutils.fs.ls("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net"))

# Read any file from ADLS (e.g reading csv file here)
display(spark.read.csv("abfss://<container_name>@<storage_account_name>.dfs.core.windows.net/<filename>.csv"))
```

## Step-05: Configuring Cluster for accessing secrets from Databricks secrets utility

- Navigate to the Databricks cluster advanced settings and add following config:

```
fs.azure.account.key.<storage_account_name>.dfs.core.windows.net {{ secrets/scope_name/scope_key_name }}
```

## References

- https://learn.microsoft.com/en-us/azure/databricks/security/secrets/secrets
- https://docs.databricks.com/en/dev-tools/databricks-utils.html
