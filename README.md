# Data Modeling with Cassandra
## Introduction
Sparkify, a startup that recently developed a new music streaming app, aims to analyze the collected data on songs and user activity. The objective is to understand which songs users are listening to. However, the data is currently stored in a directory of CSV files, making it difficult to query and generate meaningful insights.

In this project, the task was to create an Apache Cassandra database capable of running queries on the song play data to answer specific questions. Tables were designed to address the queries, and a Python-based ETL pipeline was implemented to transfer the data from CSV files to the Cassandra database. Queries were then executed to analyze the data and generate results for the Sparkify team.

## Dataset
The project utilized the event_data directory, which contains CSV files partitioned by date. Each file holds data on user activity within the Sparkify app, including artist, song title, song length, as well as user information such as first name, last name, and location.

## Project Overview
The project encompassed two main components:

1. Data Modeling: Tables were designed to address the given queries, a keyspace was created, and tables were established to load the data.
2. ETL Pipeline: The event data CSV files were processed to generate a denormalized dataset. Apache Cassandra CREATE and INSERT statements were employed to load the processed records into the relevant tables. SELECT statements were executed to validate the tables and generate query results.

## Data Modeling
For this project, the task involved addressing three queries that needed to be answered:

1. Give me the artist, song title, and song's length in the music app history that was heard during sessionId=338, and itemInSession=4
2. Give me only the following: name of artist, song (sorted by itemInSession), and user (first and last name) for userid=10, sessionid=182
3. Give me every user name (first and last) in my music app history who listened to the song 'All Hands Against His Own'

To address the provided queries, the following tables were designed:


![image](https://github.com/letapicode/dataengineering_cassandra_1/assets/102399773/974b9e71-811d-4adc-b539-8b247b417bbb)

## Create a keyspace and tables to load the data into
The Cassandra keyspace and tables were created using the Python Cassandra driver. CREATE statements were utilized with appropriate IF NOT EXISTS clauses to ensure tables were created only if they did not exist. DROP TABLE statements were also incorporated to facilitate database reset and ETL pipeline testing.

## Load the data into the tables
Data from the CSV files was loaded into the Cassandra database using an ETL pipeline. The event_data CSV files were processed, generating a denormalized dataset. INSERT statements were employed to load the data into the corresponding tables in the Cassandra database.

## ETL Pipeline
### Process the event_data CSV files
An iterative process was implemented to process the event_data CSV files. Filepaths were collected, and the files were parsed to generate a denormalized dataset. The Python CSV module was utilized, configuring a specific dialect for CSV file handling. The relevant data points required to answer the queries were extracted and written to a new CSV file.

## Write Apache Cassandra CREATE and INSERT statements
CREATE and INSERT statements for Apache Cassandra were generated using Python. These statements were responsible for creating the tables and loading the processed data into the respective tables. A function was utilized to execute the INSERT statements, and the CREATE statements were executed using the Cassandra session.

## Test the tables by running SELECT statements
Tables were tested using SELECT statements, validating the data loading process and generating query results. Python was utilized to execute the SELECT queries, and the results were displayed in the console.

## Conclusion
The project involved designing tables to answer specific queries, establishing a keyspace and tables in Apache Cassandra, loading data into the tables via a Python-based ETL pipeline, and executing SELECT statements to generate results. The successful completion of this project showcases expertise in data modeling with Apache Cassandra and the ability to implement an effective ETL pipeline using Python.

