# Importing Necessary Libraries
import googleapiclient.discovery
import pandas as pd 
import pymongo
import pymysql
import streamlit as st

# Google API Connection
apikey = "AIza****Il1_****-u3Fx****VKNt****FOKys"
youtube = googleapiclient.discovery.build("youtube", "v3", developerKey = apikey)

# Establishing Python-MongoDB Connection
client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["YouTube_00"]
collection = db.youtube_data

# Establishing Python-MySQL Connection
# MySQL Database and tables were created already
mydb = pymysql.connect(host="127.0.0.1", user="****", password="********",database="YouTube00")
sql = mydb.cursor()

# To Get YouTube Channel Details 
def channel_details(channel_id):
    request = youtube.channels().list(
        part = "snippet,contentDetails,statistics",
        id = channel_id)   
    response = request.execute()    
    details=[dict(channel_name = response["items"][0]["snippet"]["title"],
                channel_id = response["items"][0]['id'],
                channel_views = response["items"][0]["statistics"]["viewCount"],
                channel_video_count = response["items"][0]["statistics"]["videoCount"],
                overall_playlists_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"])]    
    return details

# To Get Playlist Details of YouTube Channels
def playlist_details(channel_id):
    playlists = []
    next_page_token = None
    while True:
        request = youtube.playlists().list(
            part="snippet",
            channelId=channel_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()
        for play in response.get('items', []):
            playlist = {
                "playlist_id": play["id"],
                "channel_id": play["snippet"]["channelId"],
                "playlist_name": play["snippet"]["title"]}
            playlists.append(playlist)
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return playlists

# To Get Video ids of all Videos 
def video_ids(playlist_id):
    video_ids = []
    next_page_token = None
    while True:
        request = youtube.playlistItems().list(
            part="contentDetails",
            playlistId=playlist_id,
            maxResults=50,
            pageToken=next_page_token)
        response = request.execute()
        for item in response.get("items", []):
            video_id = item["contentDetails"]["videoId"]
            video_ids.append(video_id)        
        next_page_token = response.get("nextPageToken")
        if not next_page_token:
            break
    return video_ids

# To Get Video Details 
def video_details(video_ids):
    video_details = []    
    for i in range(0, len(video_ids), 50):
        vid_ids = video_ids[i:i+50]
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=",".join(vid_ids))
        response = request.execute()
        for video in response.get("items", []):
            video_det = {
                "video_id": video["id"],
                "channel_id": video["snippet"]["channelId"],
                "video_name": video["snippet"]["title"],
                "published_date": video["snippet"]["publishedAt"],
                "views_count": video["statistics"]["viewCount"],
                "like_count": video["statistics"].get("likeCount", 0),
                "dislike_count": video["statistics"].get("dislikeCount", 0),
                "comments_count": video["statistics"].get("commentCount", 0),
                "duration": video["contentDetails"]["duration"]}
            video_details.append(video_det)
    return video_details

# To Get Comment Details
def comment_details(video_ids):
    comments = []
    for video_id in video_ids:        
            try:
                request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=10)
                response = request.execute()
                for com in response.get("items", []):
                    com_s = com["snippet"]["topLevelComment"]["snippet"]
                    comm = {
                        "video_id": video_id,
                        "comment_id": com["id"],
                        "comment_date": com_s["publishedAt"]}
                    comments.append(comm)      
            except:
                pass            
    return comments

# To Get All Details of YouTube Channel
def youtube_data (channel_id):   
    channel = channel_details(channel_id)
    playlist = playlist_details(channel_id)
    video_id = video_ids(channel[0]["overall_playlists_id"])
    video_d = video_details(video_id)
    comments = comment_details(video_id)    
    all_details={"Channel Details":channel,
         "Playlist Details":playlist,
         "Video Details":video_d,
         "Comment Details":comments}    
    return all_details

# Data harvest to streamlit
def data_harvest():
    c_id = st.text_input('Enter the Channel id')        
    if c_id and st.sidebar.button("Scrap"):
        if len(c_id)==24:
            info1 = collection.find_one({"Channel Details.channel_id":c_id}) 
            if not info1:
                try:        
                    y = youtube_data(c_id)
                    st.write(y)
                    document = [y]
                    d = collection.insert_many(document)
                    if d:
                        st.sidebar.success("Done !")
                except:
                    pass
            else:
                st.sidebar.warning("Channel Information already exist !")
        else:
            st.sidebar.error("Invalid Channel Id !") 

#  Data warehouse to Streamlit
def data_warehouse():
    cnames=[]
    doc = collection.find({}, {"Channel Details.channel_name": 1, "_id": 0})
    for i in doc:
        cname = i["Channel Details"][0]["channel_name"]
        cnames.append(cname)
    select = st.sidebar.selectbox("Select a Channel name", cnames)
    if select and st.sidebar.button("Migrate"):
        y=[]
        for i in collection.find({"Channel Details.channel_name":select}):
            y.append(i)
        c=pd.DataFrame(y[0]["Channel Details"])
        p=pd.DataFrame(y[0]["Playlist Details"])
        v=pd.DataFrame(y[0]["Video Details"])
        v['published_date'] = pd.to_datetime(v["published_date"], format='%Y-%m-%dT%H:%M:%SZ')
        v["duration"]=pd.to_timedelta(v['duration'])
        v["duration"]=v["duration"].dt.seconds
        cm=pd.DataFrame(y[0]["Comment Details"])
        cm['comment_date'] = pd.to_datetime(cm["comment_date"], format='%Y-%m-%dT%H:%M:%SZ')
        st.subheader("Channel Details")
        st.dataframe(c)
        st.subheader("Playlist Details")
        st.dataframe(p)
        st.subheader("Video Details")
        st.dataframe(v)
        st.subheader("Comment Details")
        st.dataframe(cm)        
        info2 = pd.read_sql_query("SELECT channel_name FROM Channel_Details",mydb)
        if select not in info2["channel_name"].values:
            insert1 = "INSERT INTO Channel_Details (channel_name, channel_id, channel_views, channel_video_count, overall_playlists_id) VALUES (%s,%s,%s,%s,%s)"
            for i in range(len(c)):
                sql.execute(insert1,tuple(c.iloc[i]))
            mydb.commit()
            insert2 = "INSERT INTO Playlist_Details(playlist_id, channel_id, playlist_name) VALUES (%s,%s,%s)"
            for i in range(len(p)):
                sql.execute(insert2,tuple(p.iloc[i]))
            mydb.commit()        
            insert3 = "INSERT INTO Video_Details(video_id, channel_id, video_name,published_date,views_count,like_count,dislike_count,comments_count,duration) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            for i in range(len(v)):
                sql.execute(insert3,tuple(v.iloc[i]))
            mydb.commit()        
            insert4 = "INSERT INTO Comment_Details(video_id, comment_id, comment_date) VALUES (%s,%s,%s)"
            for i in range(len(cm)):
                sql.execute(insert4,tuple(cm.iloc[i]))
            mydb.commit()
        else:
            st.sidebar.warning("Channel Information already migrated to Database !")      
 
# Data Quering through streamlit
def data_query():
    query_q = [ " ", "What are the names of all the videos and their corresponding channels?",
               "Which channels have the most number of videos, and how many videos do they have?",
               "What are the top 10 most viewed videos and their respective channels?",
               "How many comments were made on each video, and what are their corresponding video names?",
               "Which videos have the highest number of likes, and what are their corresponding channel names?",
               "What is the total number of likes and dislikes for each video, and what are their corresponding video names?",
               "What is the total number of views for each channel, and what are their corresponding channel names?",
               "What are the names of all the channels that have published videos in the year 2022?",
               "What is the average duration of all videos in each channel, and what are their corresponding channel names?",
               "Which videos have the highest number of comments, and what are their corresponding channel names?"]
    query = st.selectbox("Select the Query", query_q)
    if query == " " :
        pass
    elif query == "What are the names of all the videos and their corresponding channels?" and st.button("Query"):
        st.subheader("Names of all videos and their respective channel name")
        query_a = pd.read_sql_query("SELECT v.video_name,c.channel_name FROM Video_Details AS v JOIN Channel_Details AS c ON c.channel_id=v.channel_id", mydb)
        st.dataframe(query_a)
    elif query == "Which channels have the most number of videos, and how many videos do they have?" and st.button("Query"):
        st.subheader("Channel with most number of videos and its count")
        query_a = pd.read_sql_query("SELECT channel_name, channel_video_count FROM Channel_Details ORDER BY channel_video_count DESC", mydb)
        st.dataframe(query_a)
    elif query == "What are the top 10 most viewed videos and their respective channels?" and st.button("Query"):
        st.subheader("Top 10 most viewed video and its respective channel name")
        query_a = pd.read_sql_query("SELECT v.video_name, v.views_count, c.channel_name FROM Channel_Details c JOIN Video_Details v ON c.channel_id=v.channel_id ORDER BY v.views_count DESC LIMIT 10", mydb)
        st.dataframe(query_a)
    elif query == "How many comments were made on each video, and what are their corresponding video names?" and st.button("Query"):
        st.subheader("Number of comments on each video and their respective names")
        query_a = pd.read_sql_query("SELECT video_name, comments_count FROM Video_Details", mydb)
        st.dataframe(query_a)
    elif query == "Which videos have the highest number of likes, and what are their corresponding channel names?" and st.button("Query"):
        st.subheader("Videos with highest number of likes and its corresponding channel name")
        query_a = pd.read_sql_query("SELECT c.channel_name, v.video_name, v.like_count FROM Channel_Details c JOIN Video_Details v ON c.channel_id=v.channel_id ORDER BY v.like_count DESC" , mydb)
        st.dataframe(query_a)
    elif query == "What is the total number of likes and dislikes for each video, and what are their corresponding video names?" and st.button("Query"):
        st.subheader("Total number of likes and dislikes of each video and their respective names")
        query_a = pd.read_sql_query("SELECT video_name, like_count, dislike_count FROM Video_Details", mydb)
        st.dataframe(query_a)
    elif query == "What is the total number of views for each channel, and what are their corresponding channel names?" and st.button("Query"):
        st.subheader("Total number of views of channel and their respective names")
        query_a = pd.read_sql_query("SELECT channel_name, channel_views FROM Channel_Details ORDER BY channel_views DESC", mydb)
        st.dataframe(query_a)
    elif query == "What are the names of all the channels that have published videos in the year 2022?" and st.button("Query"):
        st.subheader("Names of all channels that have published videos in year 2022")
        query_a = pd.read_sql_query("SELECT DISTINCT c.channel_name FROM Channel_Details c JOIN Video_Details v ON c.channel_id=v.channel_id WHERE YEAR(published_date)=2022", mydb)
        st.dataframe(query_a)
    elif query == "What is the average duration of all videos in each channel, and what are their corresponding channel names?" and st.button("Query"):
        st.subheader("Average duration of all videos in a channel and their respective channel names")
        query_a = pd.read_sql_query("SELECT c.channel_name, AVG(v.duration) FROM Channel_Details c JOIN Video_Details v ON c.channel_id=v.channel_id GROUP BY c.channel_name ORDER BY AVG(v.duration) DESC", mydb)
        st.dataframe(query_a)
    elif query == "Which videos have the highest number of comments, and what are their corresponding channel names?" and st.button("Query"):
        st.subheader("Videos with highest number of Comments and their Channel names")
        query_a = pd.read_sql_query("SELECT c.channel_name, v.video_name, v.comments_count FROM Channel_Details c JOIN Video_Details v ON c.channel_id=v.channel_id ORDER BY v.comments_count DESC", mydb)
        st.dataframe(query_a)

# Projest Titile
st.sidebar.header("YouTube Data Harvesting and Warehousing using SQL MongoDB and Streamlit", divider="gray")

# Stages of Project
option = st.sidebar.selectbox('Three Stages of Project ', ('Data Scrap', 'Data Migrate', 'Data Query'))

# Condition for Scrap, Migrate, Query 
if option=="Data Scrap":   
    st.title("YouTube Data Harvesting")                    
    data_harvest()  
elif option=="Data Migrate":
    st.title("YouTube Data Ware Housing")
    data_warehouse()
elif option=="Data Query":
    st.title("YouTube Data SQL Quries")
    data_query()
