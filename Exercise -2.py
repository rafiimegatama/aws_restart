# SELECT FROM 
query = """
        SELECT Name
        FROM `bigquery-public-datta.pet_records.pets`
        WHERE Animal = `Cat`
        """

# open the U.S Cities in the Open AQ Dataset
from google.cloud import bigquery

#Create a "Client" object
client = bigquery.Client()

# Consturct a reference to the "openaq"
dataset_ref = client.dataset("openaq", project = "bigquery-public-data")

# API request - fetch the dataset (get_dataset())
dataset = client.get_dataset(dataset_ref)

# List all tables in openAQ dataset, with list_tables()
tables = list(client.list_tables(dataset))

# print names of all tables in the dataset
for table in tables:
    print(table.table_id)

# Construct a reference to the "global_airquality" table, using table()
table_ref = dataset_ref.table("global_air_quality")

# API Request, with get_table()
table = client.get_table("table_ref")

# preview the first five lines of the "global_air_quality" table, using list_rows()
client.list_rows(table, max_results=5).to_dataframe()


# Query to select all the items from the "City" column where the "Country" column is `US`
query = """
        SELECT city
        FROM `bigquery-public-data.openaq.global_air_quality'
        WHERE country = `US`
        """


# submitting the query to dataset (now ready to use query to get information from the (OpenAQ dataset), create a Client object
# Creating client object
client = bigquery.Client()

# Set up the Query, using query()
query_job = client.query(query)

# Convert the result to a pandas Dataframe,

# API request - run the query, and return a pandas Dataframe
us_cities = query_job.to_dataframe()

# now weve got a pandas Dataframe called us_cities,  we can use like any other Dataframe

# what fiv e cities have the most measurements?
us_cities.city.value_counts().head()

# OTHER IMPORTANT NOTES:
# if  we want a multiple columns, we can select them with a comma between the names:
query = """
        SELECT city, country
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

# You can select all columns with a * like this:
query = """
        SELECT *
        FROM `bigquery-public-data.openaq.global_air_quality`
        WHERE country = 'US'
        """

# DRY RUN TO KNOW the size of any query before running it.
# Query to get the score column from every row where the type column has value "job"
query = """
        SELECT score, title
        FROM `bigquery-public-data.hacker_news.full`
        WHERE type = "job" 
        """

# Create a QueryJobConfig object to estimate size of query without running it
dry_run_config = bigquery.QueryJobConfig(dry_run=True)

# API request - dry run query to estimate costs
dry_run_query_job = client.query(query, job_config=dry_run_config)

print("This query will process {} bytes.".format(dry_run_query_job.total_bytes_processed))

# Specifiy a parameter when running the query to limit how much data we are willing to scan
# Only run the query if it's less than 1 MB
ONE_MB = 1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_MB)

# Set up the query (will only run if it's less than 1 MB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
safe_query_job.to_dataframe()

# the query was cancelled because the limit of 1 MB, and we can increase the limit to run the query succesfully
# Only run the query if it's less than 1 GB
ONE_GB = 1000*1000*1000
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=ONE_GB)

# Set up the query (will only run if it's less than 1 GB)
safe_query_job = client.query(query, job_config=safe_config)

# API request - try to run the query, and return a pandas DataFrame
job_post_scores = safe_query_job.to_dataframe()

# Print average score for job posts
job_post_scores.score.mean()



### ===== EXERCISE ====== ####
# Set up feedback system
from learntools.core import binder
binder.bind(globals())
from learntools.sql.ex2 import *
print("Setup Complete")

from google.cloud import bigquery

# Create a "Client" object
client = bigquery.Client()

# Construct a reference to the "openaq" dataset
dataset_ref = client.dataset("openaq", project="bigquery-public-data")

# API request - fetch the dataset
dataset = client.get_dataset(dataset_ref)

# Construct a reference to the "global_air_quality" table
table_ref = dataset_ref.table("global_air_quality")

# API request - fetch the table
table = client.get_table(table_ref)

# Preview the first five lines of the "global_air_quality" table
client.list_rows(table, max_results=5).to_dataframe()


# ==== QUESTION 01 --=====#
from google.cloud import bigquery
# Query to select countries with units of "ppm"
first_query = """
             SELECT country
             FROM `bigquery-public-data.openaq.global

              """

first_query = """
             SELECT DISTINCT country
             FROM `bigquery-public-data.openaq.global_air_quality`
             WHERE unit = `ppm`

            """

# Set up the query (cancel the query if it would use too much of 
# your quota, with the limit set to 10 GB)
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
first_query_job = client.query(first_query, job_config=safe_config)

# API request - run the query, and return a pandas DataFrame
first_results = first_query_job.to_dataframe()

# View top few rows of results
print(first_results.head())

# Check your answer
q_1.check()

# ======== QUESTION 2 -=-======# which pollution level were reportedly to be exactly 0?
# Query to select all columns where pollution levels are exactly 0
zero_pollution_query = """
                        SELECT *
                        FROM `bigquery-public-data.openaq.global_air_quality`
                        WHERE value = 0
                        """ 
# Your code goes here

# Set up the query
safe_config = bigquery.QueryJobConfig(maximum_bytes_billed=10**10)
query_job = client.query(zero_pollution_query, job_config=safe_config)

# API request - run the query and return a pandas DataFrame
zero_pollution_results = query_job.to_dataframe() # this is using groupby()


                     # Your code goes here

print(zero_pollution_results.head())

# Check your answer
q_2.check()

