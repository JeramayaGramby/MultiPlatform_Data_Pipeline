# Importing all necessary libraries
import os
import json
from pathlib import Path
import pandas as pd
from decouple import config
import sqlite3
import spotipy
import spotipy.oauth2 as oauth2
import boto3
import smtplib,ssl
from email import encoders
import yagmail
from datetime import date
import openpyxl
# List all of the songs that Drake appears on

# List all the songs that Drake's competition has appeared on
# Find all your spotify recent songs, create a pyspark dataframe, transform the dataframe and then send it to a file that gets sent
# automatically to an S3 bucket.
# and use an asyncio function in a loop to automatically run the data pipeline every 24 hours.
# current_user_recently_played(limit=50, after=None, before=None)

# Put your email and password in environment variables

# Data validator function
# These commands are for the data loader. This function should go first before data validator
def spotify_data_loader():
    try:
        file_location_question=str(input(f'Enter 1 to send the data through Gmail, Enter 2 to send the data to S3:'))
        
        if file_location_question == '1':
            EMAIL_ADDRESS=config('EMAIL_ADDRESS')
            EMAIL_PASSWORD=config('EMAIL_PASSWORD')
            yagmail.register(EMAIL_ADDRESS, EMAIL_PASSWORD)
            yag=yagmail.SMTP(EMAIL_ADDRESS,EMAIL_PASSWORD)
            
            email_location_question=str(input(f'Do you have a list of emails saved to the home directory you would like to use? (y/n):'))
            
            if email_location_question == 'y':
                try:
                    # File conversion
                    file_type_question=str(input(f'Enter 1 to send the file as a csv file, 2 for xlsx file, 3 for sqlite file:'))
                    
                    if file_type_question == '1':
                        try:
                            with open('albums.csv', 'w') as csv_file:
                                parsed_spotify_data.to_csv(csv_file)
                            
                            print(f'Successfully created Excel file')

                            control_flow_maintainer=f'Your emails have sent!'
                            artist=str(input(f'Enter the name of the artist:'))

                            with open('email_list.csv', 'r') as email_list:
                                imported_file = pd.read_csv(email_list)
                            
                            print(f'Successfully loaded email list')
            
                            for name in imported_file['name']:

                                text = f"""
                                    Hello {name} Thank you for using the Spotify Data ETL.
                                    Your file is attached to this email."""
                                
                                subject = f'{artist} Spotify Albums'
                
                            for email in imported_file['email']:
                                with open('albums.csv', 'rb') as attachment:
                                    yag.send(to=[email],
                                    subject=subject,
                                    contents=text,
                                    attachments=attachment
                                    ) 
                        except Exception as e:
                            print(f'Could not send csv file. Please rerun the script')
                            print(e)
                        
                        print(control_flow_maintainer)
              
                    if file_type_question == '2':
                        try:
                            with open('albums.xlsx', 'wb') as xlsx_file:
                                parsed_spotify_data.to_excel(xlsx_file)
                            
                            print(f'Successfully created Excel file')

                            with open('email_list.csv', 'r') as email_list:
                                imported_file = pd.read_csv(email_list)
                
                            print(f'Successfully loaded email list')

                            control_flow_maintainer=f'Your emails have sent!'
                            artist=str(input(f'Enter the name of the artist:'))
            
                            for name in imported_file['name']:

                                text = f"""
                                    Hello {name} Thank you for using the Spotify Data ETL.
                                    Your file is attached to this email."""
                                
                                subject = f'{artist} Spotify Albums'
                
                            for email in imported_file['email']:
                                with open('albums.xlsv', 'rb') as attachment:
                                    yag.send(to=[email],
                                    subject=subject,
                                    contents=text,
                                    attachments=attachment
                                    ) 
                        except Exception as e:
                            print(f'Could not send the Excel file. Please rerun the script')
                            print(e)
        
                        print(control_flow_maintainer)
            
                    if file_type_question == '3':
                        try:
                            database = 'spotify_artist_albums.sqlite'
                            conn = sqlite3.connect(database)
                            with open(database, 'w'):
                                parsed_spotify_data.to_sql(name='albums', con=conn)
                                conn.close()
                    
                            print(f'Successfully created database file')
                            
                            with open('email_list.csv', 'r') as email_list:
                                imported_file = pd.read_csv(email_list)
                
                            print(f'Successfully loaded email list')

                            control_flow_maintainer=f'Your emails have sent!'
                            artist=str(input(f'Enter the name of the artist:'))
            
                            for name in imported_file['name']:

                                text = f"""
                                    Hello {name} Thank you for using the Spotify Data ETL.
                                    Your file is attached to this email."""
                                
                                subject = f'{artist} Spotify Albums'
                
                            for email in imported_file['email']:
                                with open(database, 'r') as attachment:
                                    yag.send(to=[email],
                                    subject=subject,
                                    contents=text,
                                    attachments=attachment
                                    ) 

                        except Exception as e:
                            print(f'Could not send sqlite file. Please rerun the script')
                            print(e)
        
                    print(control_flow_maintainer)        
        
                except Exception as e:
                    print(f'Sorry there was an error')
                    print(e)

            if email_location_question == 'n':
                try:
                    file_type_question=str(input(f'Enter 1 to send the file as a csv file, 2 for xlsx file, 3 for sqlite file:'))
                    
                    if file_type_question == '1':
                        try:
                            artist=str(input(f'Enter the name of the artist:'))
                            additional_emails=str(input(f'Enter the list of emails separated with a comma:'))
                            additional_email_list=list(additional_emails.split(sep=','))
                            control_flow_maintainer=f'Your emails have sent!'
                            with open('albums.csv', 'w') as csv_file:
                                parsed_spotify_data.to_csv(csv_file)
                            
                            print(f'Successfully created csv file')
            

                            text = f"""
                                    Hello! Thank you for using the Spotify Data ETL.
                                    Your file is attached to this email."""
                                
                            subject = f'{artist} Spotify Albums'
                
                            with open('albums.csv', 'rb') as attachment:
                                    yag.send(to=additional_email_list,
                                    subject=subject,
                                    contents=text,
                                    attachments=attachment
                                    ) 
                        except Exception as e:
                            print(f'Could not send csv file. Please rerun the script')
                            print(e)
                            
                        
                        print(control_flow_maintainer)
              
                    if file_type_question == '2':
                        try:
                            artist=str(input(f'Enter the name of the artist:'))
                            additional_emails=str(input(f'Enter the list of emails separated with a comma:'))
                            additional_email_list=list(additional_emails.split(sep=','))
                            control_flow_maintainer=f'Your emails have sent!'
                            
                            with open('albums.xlsx', 'wb') as xlsx_file:
                                parsed_spotify_data.to_excel(xlsx_file)
                            
                            print(f'Successfully created Excel file')
            
                            text = f"""
                                    Hello! Thank you for using the Spotify Data ETL.
                                    Your file is attached to this email."""
                                
                            subject = f'{artist} Spotify Albums'
                
                            with open('albums.xlsx', 'rb') as attachment:
                                yag.send(to=additional_email_list,
                                subject=subject,
                                contents=text,
                                attachments=attachment
                                ) 
                        except Exception as e:
                            print(f'Could not send the Excel file. Please rerun the script')
                            print(e)
                            
                        
                        print(control_flow_maintainer)
            
                    if file_type_question == '3':
                        try:
                            artist=str(input(f'Enter the name of the artist:'))
                            additional_emails=str(input(f'Enter the list of emails separated with a comma:'))
                            additional_email_list=list(additional_emails.split(sep=','))
                            database = 'spotify_artist_albums.sqlite'
                            control_flow_maintainer=f'Your emails have sent!'
                            conn = sqlite3.connect(database)
                                
                            with open(database, 'w'):
                                parsed_spotify_data.to_sql(name='albums', con=conn)
                                conn.close()
                    
                            print(f'Successfully created database file')

                            text = f"""
                                    Hello! Thank you for using the Spotify Data ETL.
                                    Your file is attached to this email."""
                                
                            subject = f'{artist} Spotify Albums'
                
                            with open(database, 'r') as attachment:
                                yag.send(to=additional_email_list,
                                subject=subject,
                                contents=text,
                                attachments=attachment
                                )
                        except Exception as e:
                            print(f'Could not send the sqlite file. Please rerun the script')
                            print(e)
                            
                            
                        print(control_flow_maintainer)
    
                except Exception as e:
                    print(f'Sorry there was an error')
                    print(e)
       
        if file_location_question == '2':
            try:
                ACCESS_KEY=str(input(f'Enter your Access Key:'))
                SECRET_ACCESS_KEY=str(input(f'Enter your Secret Access Key:'))
                SESSION_TOKEN=str(input(f'Enter your session token:'))
                session = boto3.Session(
                aws_access_key_id=ACCESS_KEY, 
                aws_secret_access_key=SECRET_ACCESS_KEY, 
                aws_session_token=SESSION_TOKEN
                )
                s3_client = boto3.client('s3')
                file_type_question=str(input(f'Enter 1 to send the file as a csv file, 2 for xlsx file, 3 for sqlite file:'))
                if file_type_question == '1':
                    try:
                        with open('albums.csv','w') as csv_file:
                            parsed_spotify_data.to_csv(csv_file)
                            
                        file=Path('albums.csv')
                        print(f'Successfully created csv file')
                    except Exception as e:
                        print(f'Could not send csv file. Please rerun the script')
                        print(e)
                        return file_type_question

                if file_type_question == '2':
                    try:
                        with open('albums.xlsx','wb') as xlsx_file:
                            parsed_spotify_data.to_excel(xlsx_file)

                        file=Path('albums.xlsx')
                        print(f'Successfully created Excel file')
                    except Exception as e:
                        print(f'Could not create Excel file. Please rerun the script')
                        print(e)
                        return file_type_question

                if file_type_question == '3':
                    try:
                        database = 'spotify_artist_albums.sqlite'
                        conn = sqlite3.connect(database)
                        with open(database, 'w'):
                            parsed_spotify_data.to_sql(name='albums', con=conn)
                            conn.close()
              
                        file=Path(database)
                        print(f'Successfully created database file')
                    except Exception as e:
                        print(f'Could not send sqlite file. Please rerun the script')
                        return file_type_question

                bucket=str(input(f'Enter the name of the bucket you would like to send the file to:'))
                object_name=str(f'{today} {file}')
                s3_client.upload_file(file,bucket,object_name)
                print(f'Your database file has been successfully uploaded to {bucket} with the name {object_name}')
                return file,object_name,bucket
            
            except Exception as e:
                print(f'There was an issue uploading the Spotify data to your S3 bucket. Please try again')
                print(e)
                            
    except Exception as e:
        print(f'Something went wrong. Please rerun the script')
        print(e)
        pass


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
    
    spotify_data_validator(df=parsed_spotify_data)
    spotify_data_loader()