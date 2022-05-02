# Additional Resources

## Getting started with Prefect Cloud
Follow the instructions [here](https://orion-docs.prefect.io/ui/cloud/) to create a Prefect Cloud account, and to tie it to your machine. There is no cost for Prefect Cloud at the scale that you'll be using it for this task. 

## Creating a SQLite Database
Follow the instructions [here](https://www.tutorialspoint.com/sqlite/sqlite_create_database.htm) to create a new SQLite database.

You can use the following command to create the necessary table:
```sql
CREATE TABLE speeds (id INTEGER PRIMARY KEY, direction TEXT, speed INT, timestamp DATETIME);
```

## Using the S3 Task with Prefect
Prefect provide a simple S3 API which you can read about [here](https://prefecthq.github.io/prefect-aws/). You can also use [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-examples.html) to perform your S3 uploads if you would prefer.

## Writing parameterized statements 
A best practice when writing SQL statements is to parameterize them. See [the documentation here](https://docs.python.org/3/library/sqlite3.html) for an explanation of how to do it in python with SQLite

## Other things to consider
* How many results does the API return? Should we expect this many results every time? What would it mean if this number were to change? How should be handle it?