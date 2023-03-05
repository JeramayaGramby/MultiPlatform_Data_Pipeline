# Importing all necessary libraries
import os
import json
from pathlib import Path
import pandas as pd
from decouple import config
import sqlite3
import sqlalchemy
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from sparktest import sparktester
import spotipy
import spotipy.oauth2 as oauth2
import boto3
import smtplib,ssl
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import date
# List all of the songs that Drake appears on

# List all the songs that Drake's competition has appeared on
# Find all your spotify recent songs, create a pyspark dataframe, transform the dataframe and then send it to a file that gets sent
# automatically to an S3 bucket.
# and use an asyncio function in a loop to automatically run the data pipeline every 24 hours.
# current_user_recently_played(limit=50, after=None, before=None)

# Return Top posts by Location in Instagram

# Data validator function
# These commands are for the data loader. This function should go first before data validator
def spotify_data_loader():
    file_location_question=str(input(f'Enter 1 to send the data to your Gmail, Enter 2 to send the data to S3, Enter 3 to send the data to Spark/Hadoop:'))
    if file_location_question == '1':
        additional_email_question=str(input(f'Are there any other email addresses you would like to send the data to? y/n:'))
            
        if additional_email_question == 'y':
            email_list=[]
            additional_emails=list(str(input(f'Please enter any additional email addresses and separate them with a comma:')))
            for email in additional_emails:
                email_list.append(additional_emails)
                receiver=additional_emails
        
        if additional_email_question == 'n':
                receiver=str(input(f'Enter the email address you would like to send the Spotify data to:'))
        
        if additional_email_question != 'y' or 'n':
            print(f'Invalid response. Please rerun the script')        
            return additional_email_question

        file_type_question=str(input(f'Enter 1 to send/receive the file as a csv file, 2 for xlsv file, 3 for sqlite file:'))
        
        if file_type_question == '1':
            try:
                with open('albums.csv', 'w') as csv_file:
                    parsed_spotify_data.to_csv(csv_file)

                with open('albums.csv', 'r', encoding='utf-8') as csv_file:              
                    spotify_data_csv=parsed_spotify_data.from_csv(csv_file.read())
                                
                file=spotify_data_csv

                print(f'Successfully created database file')
            except:
                print(f'Could not send csv file. Please rerun the script')
        if file_type_question == '2':
            try:
                with open('albums.xlsv', 'w') as csv_file:
                    parsed_spotify_data.to_csv(csv_file)

                with open('albums.xlsv', 'r', encoding='utf-8') as csv_file:              
                    spotify_data_csv=parsed_spotify_data.from_csv(csv_file.read())
                                
                file=spotify_data_csv
                
                print(f'Successfully created database file')
            except:
                print(f'Could not send csv file. Please rerun the script')
        if file_type_question == '3':
            try:
                database = 'spotify_artist_albums.sqlite'
                conn = sqlite3.connect(database)
                with open(database, 'w') as sql_file:
                    parsed_spotify_data.to_sql(name='albums', con=conn)
                    conn.close()
                    print(f'Successfully created database file')

                with open(database, 'r', encoding='utf-8') as sql_file:              
                    spotify_data_sql=parsed_spotify_data.from_sql(sql_file.read())

                    file=spotify_data_sql
            except:
                print(f'Could not send sqlite file. Please rerun the script')

        if file_type_question != '1' or '2' or '3':
            print(f'Invalid response. Please rerun the script')
            return file_type_question
        
        with smtplib.SMTP_SSL('smtp.gmail.com', port, context=context) as server:
            port=465
            print(f'Ensure that your Gmail security settings are properly configured for Python apps')
            EMAIL= str(input(f'Enter your email:'))
            PASSWORD=str(input(f'Enter your password:'))
            context = ssl.create_default_context()
            message = MIMEMultipart()
        
            with open(file, "rb") as attachment:
                part = MIMEBase("application", "octet-stream")
                part.set_payload(attachment.read())          
                server.sendmail(EMAIL,email_list,message.as_string())
                server.quit()


    if file_location_question == '2':
            try:
                ACCESS_KEY=str(input(f'Enter your Access Key:'))
                SECRET_ACCESS_KEY=str(input(f'Enter your Secret Access Key:'))
                SESSION_TOKEN=str(input(f'Enter your session token:'))
                region_name=str(input('What region would you like to work from? (ex: us-east-1):'))
                session = boto3.Session(
                aws_access_key_id=ACCESS_KEY, 
                aws_secret_access_key=SECRET_ACCESS_KEY, 
                aws_session_token=SESSION_TOKEN
                )
                s3_client = boto3.client('s3')
                file_type_question=str(input(f'Enter 1 to send the file as a csv file, 2 for xlsv file, 3 for sqlite file:'))
                if file_type_question == '1':
                        try:
                            with open('albums.csv', 'w') as csv_file:
                                parsed_spotify_data.to_csv(csv_file)

                            with open('albums.csv', 'r', encoding='utf-8') as csv_file:              
                                spotify_data_csv=parsed_spotify_data.from_csv(csv_file.read())
                                
                            file=spotify_data_csv
                            print(f'Successfully created database file')
                        except:
                            print(f'Could not send csv file. Please rerun the script')
                if file_type_question == '2':
                        try:
                            with open('albums.xlsv', 'w') as excel_file:
                                parsed_spotify_data.to_excel(excel_file)

                            with open('albums.xlsv', 'r', encoding='utf-8') as excel_file:              
                                spotify_data_xlsv=parsed_spotify_data.from_excel(api_output_file.read())
                            
                            file=spotify_data_xlsv
                            
                            print(f'Successfully created Excel file')
                        except:
                            print(f'Could not create Excel file. Please rerun the script')
                if file_type_question == '3':
                        try:
                            database = 'spotify_artist_albums.sqlite'
                            conn = sqlite3.connect(database)
                            with open(database, 'w') as sql_file:
                                parsed_spotify_data.to_sql(name='albums', con=conn)
                                conn.close()
                                print(f'Successfully created database file')

                            with open(database, 'r', encoding='utf-8') as sql_file:              
                                file=parsed_spotify_data.from_sql(sql_file.read())
                        except:
                            print(f'Could not send sqlite file. Please rerun the script')

                if file_type_question != '1' or '2' or '3':
                        print(f'Invalid response. Please rerun the script')
                        return file_type_question

                bucket=str(input(f'Enter the name of the bucket you would like to send the file to:'))
                object_name=str(f'{today} {file}')
                s3_client.upload_file(file,bucket,object_name)
                print(f'Your database file named {file} has been successfully uploaded to {bucket} with the name {object_name}')
                return file,object_name,bucket
            
            except Exception as e:
                print(f'There was an issue uploading the Spotify data to your S3 bucket. Please try again')
                print(e)

    if file_location_question == '3':
        try:
            sparktester()
            sparked_spotify_data=SparkSession.createDataFrame(parsed_spotify_data)
            print(sparked_spotify_data)
        except:
            print(f'Could not create/send dataframe to Apache Spark. Please rerun the script')
                            
    if file_location_question != '1' or '2' or '3':
        print(f'Invalid response. Please rerun the script')


def spotify_data_validator(df: pd.DataFrame) -> bool:
    if df.empty:
        print(f'The Spotify API returned empty data. Please check your credentials')
        return False
    
    if pd.Series(df['uri']).is_unique:
        print(f'Primary key check succeeded')
    else: 
        raise Exception(f'Duplicate primary keys were found inside dataframe. Check Spotify API')
        
if __name__ == '__main__':

    today=date.today()
    PROJECT_ROOT=os.path.dirname(os.path.abspath(__file__))
    BASE_DIR=os.path.dirname(PROJECT_ROOT)

    CLIENT_ID=config('CLIENT_ID')
    CLIENT_SECRET=config('CLIENT_SECRET')
    USERNAME=config('USERNAME')
# The first part of the ETL must be global because the program needs to call on the dataframe after creation
    album_name=[]
    artist_name=[]
    release_date=[]
    total_tracks=[]
    type=[]
    uri=[]
    
    try:
        artist_id=str(input(f'Enter the Spotify Artist ID:'))
        auth=oauth2.SpotifyClientCredentials(client_id=CLIENT_ID,client_secret=CLIENT_SECRET)
        token=auth.get_access_token(as_dict=False)
        sp=spotipy.Spotify(auth=token)
        spotify_data=sp.artist_albums(artist_id=artist_id, album_type='album', country='US', limit=50, offset=0)   
        # The issue is that you cant use api output file for everything and must somehow use the output file only
        # The output.json needs to be saved as a variable along with being used in a context manager?
        # Set a variable equal to open(output.json location)
        # Set a variable equal to the path of the new output file
        with open('output.json', 'w') as api_output_file:
            json.dump(spotify_data,api_output_file,sort_keys=True)
        
        with open('output.json', 'r', encoding='utf-8') as api_output_file:
            imported_output_file = json.loads(api_output_file.read())
            
        for album in imported_output_file["items"]:
            album_name.append(album["name"])
            artist_name.append(album["artists"][0]["name"])
            release_date.append(album["release_date"])
            total_tracks.append(album["total_tracks"])
            type.append(album["type"])
            uri.append(album["uri"])

        spotify_data_dict= {
            "album_name": album_name,
            "artist_name": artist_name,
            "release_date": release_date,
            "total_tracks": total_tracks,
            "type": type,
            "uri": uri,
            }
        parsed_spotify_data=pd.DataFrame(spotify_data_dict, columns=["album_name","release_date","total_tracks","artist_name","type","uri"])
        print(f'Data extraction process completed')

    except Exception as e:
        print(f'Data extraction process could not finish. Please try again.')
        print(e)
    
    spotify_data_validator()
    spotify_data_loader()