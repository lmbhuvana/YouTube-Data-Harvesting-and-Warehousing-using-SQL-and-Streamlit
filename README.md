## _YouTube-Data-Harvesting-and-Warehousing-using-SQL-and-Streamlit_
## _Domain : Social Media_
## Problem Statement:
The problem statement is to create a Streamlit application that allows users to access and analyze data from multiple YouTube channels.
### The application will have the following features:
<ul>
<li>Ability to input a YouTube channel ID and retrieve all the relevant data (Channel name, subscribers, total video count, playlist ID, video ID, likes, dislikes, comments of each video) using Google API.
 <li>Ability to collect data for up to 10 different YouTube channels and store them in the data lake by clicking a button.
 <li>Option to store the data in MYSQL.
<li>Ability to search and retrieve data from the SQL database using different search options, including joining tables to get channel details.</p>
</ul>

## Technologies used :
<li>YouTube Data API</li>
<li>Python</li>
<li>MongoDB</li>
<li>MySQL</li>
<li>Pandas</li>
<li>Streamlit</li>

## Approach:
#### Set up a Streamlit app:</br>
Streamlit is used to create a simple UI where users can enter a YouTube channel ID, view the channel details, and select channels to migrate to the data warehouse.<br>

#### Connect to the YouTube API:</br>
YouTube API is used to retrieve channel and video data. The Google API client library for Python to make requests to the API.<br>

#### Migrate data to a SQL data warehouse: </br>
After collected data for multiple channels, migrate it to a SQL data warehouse. MySQL database is used for this.<br>

#### Query the SQL data warehouse: </br>
SQL queries is used to join the tables in the MySQL and retrieve data for specific channels based on user input. Python SQL library such as SQLAlchemy is used to interact with the SQL database.<br>

#### <p>Display data in the Streamlit app: </br>
Finally, The retrieved data is displayed in the Streamlit app.<br>

#### _Overall, this approach involves building a simple UI with Streamlit, retrieving data from the YouTube API, storing the data SQL as a warehouse, querying the data warehouse with SQL, and displaying the data in the Streamlit app_<br>
