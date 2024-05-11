from googleapiclient.discovery import build
from pprint import pprint
import pymongo
import mysql.connector as sql
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine
from datetime import datetime
from PIL import Image
import re

# Upload to MongoDB
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["youTubeDB"]
collection = mydb["all_channel_details"]

# To connect with MySQL DATABASE
db = sql.connect(host="localhost",
                   user="root",
                   password="",
                   database= "youtube_data_harvesting",                   
                  )
mycursor = db.cursor(buffered=True)

# Connect to the database
engine = create_engine("mysql+mysqlconnector://root:@localhost/youtube_data_harvesting")

# Test the connection
connection = engine.connect()

# To build connection with YouTube API
def api_connection():
    api_id = "Use your API key"
    api_service_name = "youtube"
    api_version= "v3"

    youtube = build(api_service_name,api_version,developerKey=api_id)

    return youtube
youtube = api_connection()

#To get Channel Details

def get_channel_info(channel_id):
    channel_details=[]
    request = youtube.channels().list(part = "snippet,contentDetails,Statistics",id = channel_id)
    response=request.execute()

    for i in response['items']:
        channel_informations = {
            'Channel_id' : i['id'],
            'Channel_name':i['snippet']['title'],
            'Playlist_id': i['contentDetails']['relatedPlaylists']['uploads'],
            'Subscribers': i['statistics']['subscriberCount'],
            'Views':i['statistics']['viewCount'],
            'Total_videos': i['statistics']['videoCount'],
            'Description':i['snippet']['description']
        }
        channel_details.append(channel_informations)
    return channel_details

#To get Video Id's

def get_channel_videos(channel_id):
    video_ids = []
    next_page_token=None

    # get Uploads playlist id
    request = youtube.channels().list(id=channel_id, part='contentDetails')
    response=request.execute()

    playlist_id = response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    next_page_token = None

    while True:
        request = youtube.playlistItems().list(playlistId=playlist_id, part='contentDetails', maxResults=50, pageToken=next_page_token)
        response = request.execute()
        for i in response['items']:
            video_ids.append(i['contentDetails']['videoId'])
        next_page_token = response.get('nextPageToken')
        if not next_page_token:
            break

    return video_ids

#To change Duration from PT to HHMMSS format

def duration_to_seconds(duration_str):
    # Define a regular expression pattern to match durations
    pattern = r'PT(?:(\d+)H)?(?:(\d+)M)?(?:(\d+)S)?'

    # Use regular expression to find the numerical values of hours, minutes, and seconds
    match = re.match(pattern, duration_str)

    if match:
        hours = int(match.group(1)) if match.group(1) else 0
        minutes = int(match.group(2)) if match.group(2) else 0
        seconds = int(match.group(3)) if match.group(3) else 0

        # Calculate total duration in seconds
        
        return (f"{hours}:{minutes}:{seconds}")
    else:
        return "0:0:0"  # Return None if the format is not matched

def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(part="snippet,ContentDetails,statistics", id=video_id)
        response=request.execute()

        for i in response['items']:
            duration_str = i['contentDetails']['duration']
            duration_seconds = duration_to_seconds(duration_str)

            video_information={
                'Channel_Name' :i['snippet']['channelTitle'],
                'Channel_Id':i['snippet']['channelId'],
                'Video_Id':i['id'],
                'Title':i['snippet']['title'],
                'Tags':i['snippet'].get('tags'),
                'Thumbnail':i['snippet']['thumbnails']['default']['url'],
                'Description':i['snippet'].get('description'),
                'Published_Date':i['snippet']['publishedAt'],
                'Duration':duration_to_seconds(i['contentDetails']['duration']),
                'Views':i['statistics'].get('viewCount'),
                'Likes':i['statistics'].get('likeCount'),
                'Comments':i['statistics'].get('commentCount'),
                'Favorite_Count':i['statistics']['favoriteCount'],
                'Definition':i['contentDetails']['definition'],
                'Caption_Status':i['contentDetails']['caption']
            }
            video_data.append(video_information)    
    return video_data


#To get Comment Details

def get_comment_info(video_ids):
    Comment_data = []
    for video_id in video_ids:
        try:
            request = youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50
            )
            response = request.execute()

            for i in response['items']:
                comment_info = {
                    'Comment_Id': i['snippet']['topLevelComment']['id'],
                    'Video_Id': i['snippet']['topLevelComment']['snippet']['videoId'],
                    'Comment_Text': i['snippet']['topLevelComment']['snippet']['textDisplay'],
                    'Comment_Author': i['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                    'Comment_Published': i['snippet']['topLevelComment']['snippet']['publishedAt']
                }

                Comment_data.append(comment_info)

        except Exception as e:
            print(f"Error occurred while fetching comments for video ID {video_id}: {e}")

    return Comment_data

#To get Playlist Details
            
def get_playlist_details(channel_id):
        next_page_token=None
        Playlist_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for i in response['items']:
                        playlist_info={'Playlist_Id':i['id'],
                                'Title':i['snippet']['title'],
                                'Channel_Id':i['snippet']['channelId'],
                                'Channel_Name':i['snippet']['channelTitle'],
                                'PublishedAt':i['snippet']['publishedAt'],
                                'Video_Count':i['contentDetails']['itemCount']
                        }
                        Playlist_data.append(playlist_info)

                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return Playlist_data

#Upload the document
def all_channel_details(channel_id):
    
    channelDetails = get_channel_info(channel_id)
    playlistDetails = get_playlist_details(channel_id)
    videoIds = get_channel_videos(channel_id)
    videoDetails = get_video_info(videoIds)
    commentDetails = get_comment_info(videoIds)
    
    channel_collections = mydb["all_channel_details"]
    
    # Replace the document if it already exists, otherwise insert it
    channel_collections.replace_one({"channel_id": channel_id}, {
        "channel_id": channel_id,
        "channel_information": channelDetails,
        "playlist_information": playlistDetails,
        "video_information": videoDetails,
        "comment_information": commentDetails
    }, upsert=True)
    
    return "Great! uploaded"

def data_from_mongodb(channel_id):
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["youTubeDB"]
    collection = mydb["all_channel_details"]
    d = collection.find_one({'channel_information.Channel_id': channel_id})
    return d
# SQL table craetion

#To create table for Channels, Playlists, Videos, Commments
def table_sql():

    create_query= '''create table if not exists channels(Channel_id varchar(30) primary key,
                                                        Channel_name varchar(100),
                                                        Playlist_id varchar(80),
                                                        Subscribers bigint,
                                                        Views bigint,
                                                        Total_videos int,
                                                        Description text)'''

    mycursor.execute(create_query)
    db.commit()
    #To create playlists
    create_query= '''create table if not exists playlists(Playlist_Id varchar(50) primary key,
                                                        Title varchar(100),
                                                        Channel_Id varchar(30),
                                                        Channel_Name varchar(100),
                                                        PublishedAt timestamp,
                                                        Video_Count int
                                                        )'''
    mycursor.execute(create_query)
    db.commit()
    #To create videos
    create_query='''create table if not exists videos(Channel_Name varchar(100),
                                                    Channel_Id varchar(30),
                                                    Video_Id varchar(30) primary key,
                                                    Title varchar(150),
                                                    Tags text,
                                                    Thumbnail varchar(200),
                                                    Description text,
                                                    Published_Date timestamp,
                                                    Duration time,
                                                    Views bigint,
                                                    Likes bigint,
                                                    Comments int,
                                                    Favorite_Count int,
                                                    Definition varchar(10),
                                                    Caption_Status varchar(50)
                                                    )'''
    mycursor.execute(create_query)
    db.commit()
    #To create playlistscomments
    create_query='''create table if not exists comments(Comment_Id varchar(100) primary key,
                                                        Video_Id varchar(100),
                                                        Comment_Text text,
                                                        Comment_Author varchar(100),
                                                        Comment_Published timestamp
                                                        )'''
    mycursor.execute(create_query)
    db.commit()

# SETTING PAGE CONFIGURATIONS
icon =Image.open("D:\YTLogo.png")

st.set_page_config(page_title="Youtube Data Harvesting and Warehousing | By BHUVANA",
                   page_icon=icon,
                   layout="wide",
                   initial_sidebar_state="auto",
                   menu_items={'About': """# This app is created by *BHUVANA!*"""})

#Initialize channel_id as empty string-Global variable
channel_id = ""

# Sidebar with options
with st.sidebar:
    select_box = st.selectbox("Select the below options", ("Home", "Channel Data Scraping", "Migrate to SQL", "Queries"))
    st.write('You selected:', select_box)
    
    if select_box == "Channel Data Scraping":
        channel_id = st.text_input("Enter the Channel ID")

# Main content based on selected option
if select_box == "Home":
    with st.container():
        st.image(Image.open(r"D:\YTLogo.png"))
        st.title(":rainbow[YouTube Data Harvesting and Warehousing using SQL and Streamlit]")
        st.header(":red[**Skills Take Away**]", divider='rainbow')
        st.caption(":blue[**_Python Scripting_**]")
        st.caption(":blue[**_Data Collection_**]")
        st.caption(":blue[**_Streamlit_**]")
        st.caption(":blue[**_MongoDB_**]")
        st.caption(":blue[**_API Integration_**]")
        st.caption(":blue[**_Data Management using MongoDB and SQL_**]")

# To scrap channel details based on Channel ID
if select_box =="Channel Data Scraping":
     if st.button(":black[**Upload Channel Details**]"):
        if channel_id:
            result = all_channel_details(channel_id)
            st.success(result)
        else:
            st.warning("Please enter a valid Channel ID")

#  Collect all document names
all_channels_mongo = []

for document in collection.find():
    channel_info = document.get("channel_information", [{}])[0]
    channel_id = channel_info.get("Channel_id")
    channel_name = channel_info.get("Channel_name")
    if channel_id and channel_name:
        all_channels_mongo.append((channel_id, channel_name))

# To migrate the selected channel to sql

if select_box =="Migrate to SQL":
    channel_id, channel_namelist = st.selectbox(":violet[**Select the channel to migrate**]", all_channels_mongo)
    if st.button(":black[**Migrate to SQL**]"):
        if channel_namelist:
            data = data_from_mongodb(channel_id)

            # Create DataFrames for different types of information
            channel_df = pd.DataFrame(data['channel_information'], index=[0])
            playlist_df = pd.DataFrame(data['playlist_information'])
            video_df = pd.DataFrame(data['video_information'])
            comment_df = pd.DataFrame(data['comment_information'])
            
            try:
                table_sql()
                # Insert data into 'channels' table
                channel_df.to_sql('channels', con=engine, if_exists='append', index=False, method='multi', chunksize=1000)

                # Insert data into 'playlists' table
                playlist_df.to_sql('playlists', con=engine, if_exists='append', index=False, chunksize=1000)

                # Assuming 'Tags' is a list of strings or None
                video_df['Tags'] = video_df['Tags'].apply(lambda x: ', '.join(x) if isinstance(x, list) else '')

                # Now, execute the to_sql function
                video_df.to_sql('videos', con=engine, if_exists='append', index=False)

                # Insert data into 'comments' table
                comment_df.to_sql('comments', con=engine, if_exists='append', index=False)

            except Exception as e:
                print("Error occurred:", e)

            finally:
                # Close the connection
                engine.dispose()   
        else:
            st.warning("Please select a channel before migrating to SQL.")

#To select the quereis

if select_box =="Queries":
    # if st.button(":black[**Queries**]"):
        questions = st.selectbox(":violet[**Select questions**]",
                                ['1. What are the names of all the videos and their corresponding channels?',
                                '2. Which channels have the most number of videos, and how many videos do they have?',
                                '3. What are the top 10 most viewed videos and their respective channels?',
                                '4. How many comments were made on each video, and what are their corresponding video names?',
                                '5. Which videos have the highest number of likes, and what are their corresponding channel names?',
                                '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?',
                                '7. What is the total number of views for each channel, and what are their corresponding channel names?',
                                '8. What are the names of all the channels that have published videos in the year 2022?',
                                '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?',
                                '10. Which videos have the highest number of comments, and what are their corresponding channel names?'])

        if questions == '1. What are the names of all the videos and their corresponding channels?':
            mycursor.execute("""SELECT title as Videos,channel_name as Channel_Name from videos""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write("### :green[Name of videos in each channel :]")
            st.write(df)

        elif questions == '2. Which channels have the most number of videos, and how many videos do they have?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, total_videos AS Total_Videos
                                FROM channels
                                ORDER BY total_videos DESC""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write("### :green[Number of videos in each channel :]")
            st.write(df)            
                        
        elif questions == '3. What are the top 10 most viewed videos and their respective channels?':
            mycursor.execute("""SELECT channel_name AS Channel_Name, Title AS Video_Title, Views 
                                FROM videos
                                ORDER BY views DESC
                                LIMIT 10""")
            df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
            st.write("### :green[Top 10 most viewed videos :]")
            st.write(df)            
            
        elif questions == '4. How many comments were made on each video, and what are their corresponding video names?':
                mycursor.execute("""SELECT a.video_id AS Video_id,Title, b.Total_Comments
                                    FROM videos AS a
                                    LEFT JOIN (SELECT video_id,COUNT(comment_id) AS Total_Comments
                                    FROM comments GROUP BY video_id) AS b
                                    ON a.video_id = b.video_id
                                    ORDER BY b.Total_Comments DESC""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                st.write("### :green[Comments made on each Video & corresponding Video names :]")
                st.write(df)

        elif questions == '5. Which videos have the highest number of likes, and what are their corresponding channel names?':
                mycursor.execute("""SELECT channel_name AS Channel_Name,Title,Likes 
                                    FROM videos
                                    ORDER BY Likes DESC
                                    LIMIT 10""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)                
                st.write("### :green[Top 10 most liked videos :]")
                st.write(df)
                
        elif questions == '6. What is the total number of likes and dislikes for each video, and what are their corresponding video names?':
                mycursor.execute("""SELECT Title, Likes
                                    FROM videos
                                    ORDER BY Likes DESC""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                st.write("### :green[Total likes, dislikes for each Video & corresponding Video names :]")
                st.write(df)

        elif questions == '7. What is the total number of views for each channel, and what are their corresponding channel names?':
                mycursor.execute("""SELECT channel_name AS Channel_Name, Views
                                    FROM channels
                                    ORDER BY views DESC""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                st.write("### :green[Channels vs Views :]")
                st.write(df)

        elif questions == '8. What are the names of all the channels that have published videos in the year 2022?':
                mycursor.execute("""SELECT channel_name AS Channel_Name
                                    FROM videos
                                    WHERE published_date LIKE '2022%'
                                    GROUP BY channel_name
                                    ORDER BY channel_name""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                st.write("### :green[Channels published in the year 2022 :]")
                st.write(df)

        elif questions == '9. What is the average duration of all videos in each channel, and what are their corresponding channel names?':
                mycursor.execute("""SELECT channel_name AS Channel_Name, floor(AVG(duration)) AS Average_Video_Duration
                                    FROM videos
                                    GROUP BY channel_name
                                    ORDER BY (duration) DESC""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                df["Average_Video_Duration"]=pd.to_datetime(df["Average_Video_Duration"], unit='s').dt.time
                st.write("### :green[Average video duration for channels :]")
                st.write(df)         

        elif questions == '10. Which videos have the highest number of comments, and what are their corresponding channel names?':
                mycursor.execute("""SELECT channel_name AS Channel_Name,Video_id AS Video_ID,Comments
                                    FROM videos
                                    ORDER BY comments DESC
                                    LIMIT 10""")
                df = pd.DataFrame(mycursor.fetchall(), columns=mycursor.column_names)
                st.write("### :green[Videos with most comments :]")
                st.write(df)
