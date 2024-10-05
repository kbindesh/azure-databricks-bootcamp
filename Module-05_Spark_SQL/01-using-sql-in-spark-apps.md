# Using SQL in Spark Applications

You can access the Dataframe from SQL using following methods:

1. Local Temporary View
2. Global Temporary View

## 01. Local Temp View - Create & Access dataframe

- The view only available within a particular spark session (current notebooks).s

### Step-1.1: Create Local temporary view on dataframes

```
# Read a file and store it in a dataframe
sample_df = spark.read.parquet("/mnt/project1/covid_cases")

# Create a temp view
sample_df.createTempView("sampleTempView")
```

### Step-1.2: Access the Local temporary view using SQL

- Create a new cell of type SQL in Notebook

```
%sql
SELECT * FROM sampleTempView

# [The preceding code should display the contents of dataframe]

```

### Step-1.3: Access the Local temporary view using Python

- Create a new cell in the notebook of type python

```
# Create a new dataframe with queried records
new_df = spark.sql("SELECT * from sampleTempView")

# Display the new dataframe
display(new_df)
```

## 02. Global Temp View - Create & Access Dataframes

### Step-2.1: Create a Global temp view

- Create a new Notebook

```
df_for_gtv = df.createOrReplaceGlobalTempView("sample_gtv")
```

### Step-2.2: Access the Global temp view using SQL

### Step-2.3: Access the Global temp view using Python

### Step-2.4: Access the Glocal temp view from another notebook

## References

- [pyspark.sql.DataFrame.createTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createTempView.html)
- [pyspark.sql.DataFrame.createOrReplaceTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceTempView.html)
- [pyspark.sql.DataFrame.createGlobalTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createGlobalTempView.html)
- [pyspark.sql.DataFrame.createOrReplaceGlobalTempView](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.createOrReplaceGlobalTempView.html)
