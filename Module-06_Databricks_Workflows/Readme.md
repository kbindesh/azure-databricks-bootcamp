# Schedule and orchestrate Databricks workflows

## 01. What are Databricks workflows?

- Databricks Workflows has tools that allow you to schedule and orchestrate data processing tasks on Databricks.
- You use Databricks Workflows to configure Databricks Jobs.

## 02. What are Databricks Jobs?

- A **job** is the primary unit for scheduling and orchestrating production workloads on Databricks.
- Jobs can consist of one or more _tasks_.
  - A **task** represents a unit of logic to be run as a step in a job.
  - Tasks can range in complexity and can include the following:
    - A notebook
    - SQL queries
    - Another job
    - Control flow tasks
- Jobs provide a procedural approach to defining relationships between tasks.
- Jobs can vary in complexity from a single task running a Databricks notebook to 1000s of tasks running with conditional logic and dependencies.

## 03. What is the minimum configuration needed for a job?

- **Source code (e.g. Databricks notebook)** - It contains logic to be run.
- **Compute resource (cluster)**
- **Schedule to run**
- **A unique name**

## 04. Controlling flow of Jobs and Tasks

- When configuring jobs and tasks, you can customize settings that control how the entire job and individual tasks run.
- Some of the control flow options are as follows:
  - [Triggers](https://docs.databricks.com/en/jobs/index.html#triggers)
  - [If conditional tasks](https://docs.databricks.com/en/jobs/index.html#run-if)
  - If[-else conditional tasks](https://docs.databricks.com/en/jobs/index.html#if-else)
  - [For-each tasks](https://docs.databricks.com/en/jobs/index.html#for-each)

### 4.1 Trigger types

- Scheduled
- File arrival
- Continuous

## 05. Including a one notebook in another notebook (reusability)

### Step-5.1: Include notebook (%run magic command)

- Develop Notebook-01 (Parent)

```
# Define and initialize few variables which we'll import in Notebook-02
dataset_folder_path = "/mnt/project1/raw"
sink_adls_path = "/mnt/project1/processed"
```

- Develop Notebook-02 (Child)

```
# Call notebook-01 to access it's contents using run magic command
%run "name_of_notebook1_here"

# To check if you're able to get the values from notebook-01
dataset_folder_path

# Now, you're good to use these values anywhere in your logic
new_df = spark.read.option("header", True).csv(f"{dataset_folder_path}/covid_cases.csv")
```

### Step-5.2: Reusuable functions with multi-notebook setup

- Create **Notebook-01** with following function definition:

```
from pyspark.sql.functions import current_timestamp

def add_a_column(input_df):
  output_df = input_df.withColumn("new_col_name", "col_value")
  return output_df
```

- Create Notebook-02 and make a callout to function defined in Notebook-01

- Import Notebook-01 in Notebook-02 using run magic command as follows:

```
%run "common_functions"
```

- Call the function defined on Notebook-01 to add a column

```
final_df = add_a_col(df)

display(final_df)
```

## 06. Databricks Notebook Parameters (widgets)

### Step-6.1: Get help

```
dbutils.widget.help()
```

#### Step-6.2: Get the string value from user

```
# Create a widgets
dbutils.widgets.text("some_label", "")

# Store the entered value in a variable
v_input_text = dbutils.widgets.get("some_label")

# Print the entered input value
v_input_text
```

- Now, you can use the above variable anywhere in your logic

```
new_df = old_df.withColumnRenamed("newColName", lit(v_input_text))
```

## 07. Notebook Workflows

- Use notebook utility | dbutils.notebook

```
# Get help
dbutils.notebook.help()

[run and exit function]
```

### 7.1: Invoke other notebooks from the master notebook

- Create a new notebook i.e master-ingest-all-notebooks

```
response = dbutils.notebook.run("notebook_name", 0, {"notebook1": "col_value"})

[Should call notebook1 and execute it]
```

- In order to return some exit code after execution of the notebook1, add following statement:

```
dbutils.notebook.exit("Notebook1 executed successfully..")
```

- Invoke all other notebooks also into the same master notebook as we did for notebook1

## 08. Databricks Jobs

- Navigate to Job Runs >> Create new job
