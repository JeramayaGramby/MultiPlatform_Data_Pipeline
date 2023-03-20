<br>

## This README file contains:
-Purpose of the project <br>
-Use Cases<br>
-Project Instructions<br>
-Project Weaknesses<br>
-Project Photos <br><br>
## Project Purpose:
This exploratory project was initially created to for the sole purpose of pulling data from the Spotify API. In the process of building the pipeline I realized that I would need to create an individualized pipeline for each data-based question I wanted to solve. In cases where I wanted to analyze data streams or run data workflows, I would need to build pipelines from a tested and reliable framework that I understood every component of.<br><br>

This Spotify Data ETL finds a list of albums for each artist that you put in. After entering your Spotify API credentials, it converts the Spotify API response to a json file. The json file gets converted into the file type of the user's choice (csv, xlsv or sqlite). This ETL comes with a built-in pub-sub model, and a pipeline for S3. <br><br>

Users can send data to multiple emails and future versions will allow the ability to send data to a user specified list of cloud data storage and analytics devices on more AWS cloud storage platforms (Kinesis, Redshift, RDS).<br><br>
 

## Use Cases<br>

This data pipeline script has many use cases. The primary use case is to serve as an automated data pipeline for Spotify data. However, it can also serve as a data pipeline template that can be used to create more complex pipelines.<br><br>

## Instructions:

1. Ensure you are connected to the internet.

2. If you will be sending the data to an S3 bucket, make sure you are registered as an IAM administrative user in AWS, the AWS CLI is installed and all necessary dependencies are installed. Don't forget to create a key pair in AWS and save the pem file to the main project directory or create an environment variable pointing to the path. This project's gitignore is already preconfigured to ignore your pem file.<br><br>

3. If you will be sending the data through email, make sure you use a Gmail account that allows less secure applications to access the account. If you have a list of emails, save that list as a csv titled "email_list.csv" to the main directory. That list should
include the respective name and email of each recipient.<br><br>

4. If you are using S3, you will need to create a new session token every eighteen hours.<br><br>

To create a new token you will need to open the AWS CLI and type:
> aws configure

After entering your IAM credentials type:
> aws sts get-session-token<br>


<br> 

5. Create a .env file in the main directory to hold the
Spotify API credentials (CLIENT_ID, CLIENT_SECRET), AWS credentials (AWS_CONFIG_FILE, AWS_SHARED_CREDENTIALS_FILE) and/or your email
address credentials ("EMAIL_ADDRESS" "EMAIL_PASSWORD")
<br>


6. Go to https://open.spotify.com and find the artist you would like to see a list of albums for. To find the Spotify Artist ID, go to the URL of the Spotify artist page and copy every character after open.spotify.com/artist/

7. Run main.py and paste the Artist ID into the script to initialize the data collection process.<br><br>

 
## Project Weaknesses:
Finding and sending a list of albums that an Spotify artist created could be completed faster with a Google search and an empty clipboard. However, this project was intended to test the full features and capabilities of data pipelines and its communication channels that will be implemented in future data projects. Future versions of this project will include the ability to enter multiple Spotify artists and more cloud data platforms where data can be sent.

 <br><br>

## Project Photos:<br><br>
![photo](photos/screenshot1.png)<br><br>
![photo](photos/screenshot2.png)<br><br>
![photo](photos/screenshot3.png)<br><br>