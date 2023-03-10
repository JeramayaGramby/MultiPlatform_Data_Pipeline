<br>

## This README file contains:
-Purpose of the project <br>
-Use Cases<br>
-Project Instructions<br>
-Project Weaknesses<br>
-Project Photos <br><br>
## Project Purpose:

This Spotify Data ETL finds a list of albums for each artist that you put in. After entering your Spotify API credentials, it converts the Spotify API response to a json file. The json file gets converted into the file type of the user's choice (csv, xlsv or sqlite).

A problem many data engineers have is that they have to create adhoc data pipelines from scratch. When working with tight deadlines and massive amounts of data, it can be challenging to find a reliable framework that can be used to generate data pipelines. 

This exploratory project was initially created to for the sole purpose of pulling data from the Spotify API. In the process of building the pipeline I realized that I would need to create an individualized pipeline for each data-based question I wanted to solve. In cases where I wanted to analyze data streams or run data workflows, I would need to build pipelines from a tested and reliable framework that I understood every component of. 

This can be used to help data enthusiasts be more efficient and find data-driven solutions faster.   My future data-based solutions would also need to be more adaptable and versatile. 
 is designed to simplify big data processing to make handling gigabytes, terabytes or even petabytes of data much easier. This ETL comes with a built-in pub-sub model, a pipeline for S3, and a Spark/Hadoop workflow.

First, it queries the Spotify API to find the list of albums for a specified Spotify artist. Then it converts the data to a Spark Dataframe, CSV file, Excel file (xlsv), or Sqlite table. 

The script then allows users to send the file to cloud storage on S3, or by email. Users can send data to multiple emails and future versions will allow the ability to send data to a user specified list of cloud data storage and analytics devices on more AWS cloud storage platforms (S3, Kinesis, Redshift).

Because the ETL only processes one Spotify 

## Use Cases<br>

This data pipeline script has many use cases. The primary use case is as an automated data pipeline for Spotify data. 
There are many solutions and technologies designed to improve data engineering. However, many data engineers create adhoc solutions
for creating adhoc solutions. This ETL pipeline can be used  

You must configure apache airflow using the environment variables

With all the complexities surrounding AWS resources, completing rudimentary AWS tasks, such as uploading a file to an S3 bucket or launching an EC2 instance, can appear daunting to new cloud professionals. 

This project introduces a simple, programmatic solution for completing some of the most common tasks in AWS. The project files automate S3 bucket creation, S3 bucket deletion, S3 object uploading, S3 object deletion, EC2 instance creation, EC2 instance rebooting, EC2 instance stopping, EC2 instance starting, and EC2 state management.

The second version of the project includes an automation script which can perform all the previously listed EC2 and S3 tasks. (boto3script.py). It is a stateless automation script that can save time for DevOps professionals and increase their overall efficiency.<br><br>

## Instructions:

1. Ensure you are connected to the internet, registered as an IAM administrative user in AWS, the AWS CLI is installed and all necessary dependencies are installed.

2. Create a key pair in AWS and save the pem file to the main project directory or create an environment variable pointing to the path. This project's gitignore is already preconfigured to ignore your pem file.

3. You will need to create a new session token every eighteen hours.<br><br>

To create a new token you will need to open the AWS CLI and type:
> aws configure

After entering your IAM credentials type:
> aws sts get-session-token<br>


<br> 

4. Create a .env file in the main directory to hold the
Spotify API credentials (CLIENT_ID, CLIENT_SECRET), AWS credentials (AWS_CONFIG_FILE, AWS_SHARED_CREDENTIALS_FILE) and the Spark environment variables with "PYSPARK_PYTHON" pointing to the local Python Interpreter and the "HADOOP_HOME" variable pointing to the spark_setup folder.
<br><br>

5. Go to open.spotify.com and find the artist you would like to see a list of albums for. To find the Spotify Artist ID, go to the URL of the Spotify artist page and copy every character after open.spotify.com/artist/

6. Paste the Artist ID into the script to initialize the data collection process.<br><br>

 
## Project Weaknesses:
Finding and sending a list of albums that an Spotify artist created could be completed faster with a Google search and an empty clipboard. However, this project was intended to test data engineering features
that the project files including the automation script can only perform EC2 and S3 related functions. The next versions of this project will incrementally introduce more AWS services that can be utilized. <br><br>

## Project Photos:<br><br>
![photo](photos/screenshot4.png)<br><br>
![photo](photos/screenshot1.png)<br><br>
![photo](photos/screenshot3.png)<br><br>
![photo](photos/screenshot2.png)