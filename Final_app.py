from googleapiclient.discovery import build
import pymongo
import mysql.connector as sql
import pandas as pd
import dateutil

import streamlit as st

mydb = sql.connect(host="localhost",
                       user="root",
                       password="root",
                       database= "youtube_data",
                       port = "3306")

###############  API KEY CONNECTION  ###########################
def Apikeyconnection():
    api_key = "AIzaSyD3V7V_54w68eF67wkZQjQ4I_r2UVj6Feo"
    api_servicename = "youtube"
    api_version = "v3"

    youtube = build(api_servicename, api_version, developerKey=api_key)
    return youtube

print("Making API connection1")
youtube = Apikeyconnection()

############# To GET CHANNEL INFORMATION  #########################

def get_channel_information(channel_id):
  request = youtube.channels().list(
                  part="snippet,ContentDetails,statistics",
                  id=channel_id
  )
  response = request.execute()
  #response['items']
  for i in response["items"]:
    data = dict(channel_name=i["snippet"]["title"],
                channel_id = i["id"],
                subscription_count = i["statistics"]["subscriberCount"],
                channel_views = i["statistics"]["viewCount"],
                total_videos = i["statistics"]["videoCount"],
                channel_description = i["snippet"]["description"],
                playlist_id =i["contentDetails"]["relatedPlaylists"]["uploads"])
    return data

############## TO GET VIDEO IDs  ################3

def get_video_ids(channel_id):
    video_ids = []
    response = youtube.channels().list(id=channel_id,
                                       part="ContentDetails").execute()
    playlist_id = response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    next_page_token = None

    while True:

        response1 = youtube.playlistItems().list(
            part="snippet",
            playlistId=playlist_id, maxResults=50,
            pageToken=next_page_token).execute()
        # response1["items"][0]["snippet"]["resourceId"]["videoId"]
        for i in range(len(response1["items"])):
            video_ids.append(
                response1["items"][i]["snippet"]["resourceId"]["videoId"])  # create video_ids list refer video3
        next_page_token = response1.get("nextPageToken")

        if next_page_token is None:
            break
    return video_ids

################# To GET VIDEO INFORMATION ########################
def get_video_information(video_ids):
    video_info = []
    for vid_id in video_ids:
        request = youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=vid_id)
        response = request.execute()

        for item in response["items"]:
            v_data = dict(channel_name=item["snippet"]["channelTitle"],
                          channel_id=item["snippet"]["channelId"],
                          video_id=item["id"],
                          video_name=item["snippet"]["title"],
                          description=item["snippet"]["description"],
                          tags=item["snippet"].get("tags"),
                          published=item["snippet"]["publishedAt"],
                          view_count=item["statistics"].get("viewCount"),
                          Like_count=item["statistics"].get("likeCount"),
                          Favorite_count=item["statistics"]["favoriteCount"],
                          Comment_count=item["statistics"].get("commentCount"),
                          duration=item["contentDetails"]["duration"],
                          Thumbnail=item["snippet"]["thumbnails"]["default"]["url"],
                          definition=item["contentDetails"]["definition"],
                          caption_status=item["contentDetails"]["caption"]

                          )
            video_info.append(v_data)
    return video_info

########### TO GET COMMENT DETAILS #######################

def get_comment_details(video_ids):
    comment_data = []
    try:
        for vidID in video_ids:
            response = youtube.commentThreads().list(
                part="snippet",
                videoId=vidID,
                maxResults=50).execute()
            for item in response["items"]:
                data = dict(comment_id=item["id"],
                            video_id=item["snippet"]["videoId"],
                            comment_text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                            comment_author=item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                            comment_published_at=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
                            )
                comment_data.append(data)
    except:
        pass
    return comment_data

######### TO UPLOAD TO MONGODB  ##############

client=pymongo.MongoClient("mongodb://localhost:27017")
db=client["youtube_data"]


def channel_details(channel_id):
    ch_details = get_channel_information(channel_id)
    vi_ids = get_video_ids(channel_id)
    vi_details = get_video_information(vi_ids)
    comm_details = get_comment_details(vi_ids)

    collection1 = db["channel_details"]
    collection1.insert_one({"channel_info": ch_details,
                            "video_info": vi_details,
                            "comment_info": comm_details})
    return "upload Successful"

#______________________CHANNEL TABLE CREATION USING SQL ____________________________________
def channel_table():
    mycursor = mydb.cursor()

    drop_query = ''' drop table if exists channels'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists channels(channel_name varchar(100),
                                                              channel_id varchar(255) primary key,
                                                              channel_views bigint,
                                                              total_videos int,
                                                              subscription_count bigint,
                                                              channel_description text)'''
        mycursor.execute(create_query)
        mydb.commit()
    except:
        print("Table already created")

    ########### channel data extracted from MongoDB to DATAFRAME  #########

    ch_list = []
    db = client["youtube_data"]
    collection1=db["channel_details"]
    for c_data in collection1.find({},{"_id":0,"channel_info":1}):
        print("c_data count1:", len(c_data))
        ch_list.append(c_data["channel_info"])

    ch_df=pd.DataFrame(ch_list)
    print(ch_df)

    ######### CHANNEL_INSERT ROWS IN TABLES  ###########

    for index,row in ch_df.iterrows():
        insert_query = '''insert into channels(channel_name,
                                                channel_id,
                                                channel_views,
                                                total_videos,
                                                subscription_count,
                                                channel_description)
                                                values(%s,%s,%s,%s,%s,%s)'''
        values=(row["channel_name"],
                row["channel_id"],
                row["channel_views"],
                row["total_videos"],
                row["subscription_count"],
                row["channel_description"])

        try:
            mycursor.execute(insert_query,values)
            mydb.commit()

        except Exception as e:
            print(e)


#_____________ VIDEO TABLE CREATION IN SQL  ________________________________________________________

def video_table():
    mycursor = mydb.cursor()

    drop_query = ''' drop table if exists videos'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists videos(channel_name varchar(100),
                                                          channel_id varchar(100),
                                                          video_id varchar(255) primary key,
                                                          video_name varchar(255),
                                                          description text,
                                                          tags text,
                                                          published timestamp,
                                                          view_count int,
                                                          Like_count int,
                                                          Favorite_count int,
                                                          Comment_count int,
                                                          duration time,
                                                          Thumbnail varchar(255),
                                                          definition varchar(10),
                                                          caption_status varchar(255))'''
        mycursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(e)

    ########### Video data extracted from MongoDB to DATAFRAME  #########
    v_list = []
    db = client["youtube_data"]
    collection1=db["channel_details"]
    for v_data in collection1.find({},{"_id":0,"video_info":1}):
        for i in range(len(v_data["video_info"])):
            v_list.append(v_data["video_info"][i])

    v_df=pd.DataFrame(v_list)
    print(v_df)

    ######### VIDEO_INSERT_ROWS IN TABLES  #########################

    # convert PT15M33S to 00:15:33 format using Timedelta function in pandas
    def time_duration(t):
        a = pd.Timedelta(t)
        b = str(a).split()[-1]
        return b

    for index,row in v_df.iterrows():
        print("processing index : ", index)
        insert_query = '''insert into videos(channel_name,
                                              channel_id,
                                              video_id,
                                              video_name,
                                              description,
                                              tags,
                                              published,
                                              view_count,
                                              Like_count,
                                              Favorite_count,
                                              Comment_count,
                                              duration,
                                              Thumbnail,
                                              definition,
                                              caption_status)
                                              values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''

        tagsNameString = ""
        if row["tags"] is not None:
            tagsNameString = ",".join(row["tags"])

        values=(row["channel_name"],
                row["channel_id"],
                row["video_id"],
                row["video_name"],
                row["description"],
                tagsNameString,
                dateutil.parser.parse(row["published"]),
                row["view_count"],
                row["Like_count"],
                row["Favorite_count"],
                row["Comment_count"],
                time_duration(row["duration"]),
                row["Thumbnail"],
                row["definition"],
                row["caption_status"])
        print("Current values to be inserted : ", values)

        try:
            mycursor.execute(insert_query,values)
            mydb.commit()

        except Exception as e:
            print(e)

        print("------------------Moving to next Row---------------------------")

#______________ Comment TABLE CREATION IN SQL  ___________________________________________

def comment_table():

    mycursor = mydb.cursor()

    drop_query = ''' drop table if exists comment'''
    mycursor.execute(drop_query)
    mydb.commit()

    try:
        create_query = '''create table if not exists comment(comment_id varchar(255) primary key,
                                                                video_id varchar(255),
                                                                comment_text text,
                                                                comment_author varchar(255),
                                                                comment_published_at timestamp)'''
        mycursor.execute(create_query)
        mydb.commit()

    except Exception as e:
        print(e)

    ########### COMMENT data extracted from MongoDB to DATAFRAME  #########

    com_list = []
    db = client["youtube_data"]
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(com_data["comment_info"])):
            com_list.append(com_data["comment_info"][i])

    com_df=pd.DataFrame(com_list)

    ######### COMMENT_INSERT_ROWS IN TABLES  #########################

    for index,row in com_df.iterrows():
        print("processing index : ", index)
        insert_query = '''insert into comment (comment_id,
                                                video_id,
                                                comment_text,
                                                comment_author,
                                                comment_published_at) 
                                              values(%s,%s,%s,%s,%s)'''

        values=(row["comment_id"],
                 row["video_id"],
                 row["comment_text"],
                 row["comment_author"],
                 dateutil.parser.parse(row["comment_published_at"]))

        try:
            mycursor.execute(insert_query,values)
            mydb.commit()

        except Exception as e:
            print(e)

#________________________TO RUN ALL THE TABLES ____________________________

def tables():
    channel_table()
    video_table()
    comment_table()
    return "Tables created successfully"

# ___________________________ STREAMLIT_DATAFRAME _______________________________

def show_channel_table():
    ch_list = []
    db = client["youtube_data"]
    collection1=db["channel_details"]
    for c_data in collection1.find({},{"_id":0,"channel_info":1}):
        print("c_data count2:", len(c_data))
        ch_list.append(c_data["channel_info"])

    ch_df=st.dataframe(ch_list)
    return ch_df

def show_video_table():
    v_list = []
    db = client["youtube_data"]
    collection1=db["channel_details"]
    for v_data in collection1.find({},{"_id":0,"video_info":1}):
        for i in range(len(v_data["video_info"])):
            v_list.append(v_data["video_info"][i])

    v_df=st.dataframe(v_list)
    return v_df

def show_comment_table():
    com_list = []
    db = client["youtube_data"]
    collection1=db["channel_details"]
    for com_data in collection1.find({},{"_id":0,"comment_info":1}):
        for i in range(len(com_data["comment_info"])):
            com_list.append(com_data["comment_info"][i])

    com_df=st.dataframe(com_list)
    return com_df

#_____________________ STREAMLIT_CODE ___________________________________________________

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.caption("Retrieving youtube channel data from Google API")
    st.caption("Storing it in MONGODB as data lake")
    st.caption("Transforming data into SQL database")
    st.caption("Querying the data")
    st.caption("Displaying in streamlit app")
#______________________________________________________________________________
channel_id = st.text_input("Enter channel_id")

if st.button("collect and store data"):  #stores at MONGODB
    chan_id=[]
    db=client["youtube_data"]
    collection1=db["channel_details"]

    for ch_data in collection1.find({},{"_id":0,"channel_info":1}):
        chan_id.append(ch_data["channel_info"]["channel_id"])
    if channel_id in chan_id:
        st.success("Channel Already Exist")
    else:
        insert=channel_details(channel_id)
        st.success(insert)
#_____________________________________________________________________________________

if st.button("migrate to sql"):
    Table=tables()
    st.success(Table)

show_table = st.radio("SELECT TABLE to VIEW",("CHANNELS","VIDEOS","COMMENTS"))

if show_table =="CHANNELS":
    show_channel_table()

elif show_table =="VIDEOS":
    show_video_table()

elif show_table =="COMMENTS":
    show_comment_table()

########## SQL CONNECTION #####################

cursor = mydb.cursor()

question = st.selectbox("*****    SELECT QUESTION TO VIEW    ******",
                        ("1.All the videos and their corresponding channels?",
                         "2.Channels with most number of videos, and how many?",
                         "3.Top 10 most viewed videos and their channels?",
                         "4.Comments in each video, and their corresponding video names?",
                         "5.Videos with highest likes, and their channel names?",
                         "6.Number of likes for each video, and their video names?",
                         "7.Number of views for each channel, and their channel names?",
                         "8.Channels that have published videos in the year 2022",
                         "9.Average duration of all videos in each channel, and their channel names?",
                         "10.Videos with highest comments, and their channel names?"))

#_______________________ QUESTIONS  ______________________________

if question =='1.All the videos and their corresponding channels?':
    cursor.execute('''SELECT video_name, channel_name from videos''')
    t1=cursor.fetchall()
    df=pd.DataFrame(t1,columns=["VIDEO_TITLE","CHANNEL_NAME"])
    st.write(df)

elif question == '2.Channels with most number of videos, and how many?':
    cursor.execute('''SELECT channel_name,total_videos
                       FROM channels
                       ORDER BY total_videos desc''')
    t2 = cursor.fetchall()
    df2 = pd.DataFrame(t2, columns=["CHANNEL_NAME", "NUM OF VIDEOS"])
    st.write(df2)

elif question == '3.Top 10 most viewed videos and their channels?':
    cursor.execute('''SELECT channel_name,video_name, view_count FROM videos
                       where view_count is not null
                       ORDER BY view_count DESC
                       LIMIT 10''')

    t3 = cursor.fetchall()
    df3 = pd.DataFrame(t3, columns=["CHANNEL_NAME", "VIDEO_TITLE","VIEWS"])
    st.write(df3)

elif question == '4.Comments in each video, and their corresponding video names?':
    cursor.execute('''SELECT Comment_count ,video_name
                       FROM videos where Comment_count is not null ''')

    t4 = cursor.fetchall()
    df4 = pd.DataFrame(t4, columns=["NO OF COMMENTS", "VIDEO_TITLE"])
    st.write(df4)

elif question == '5.Videos with highest likes, and their channel names?':
    cursor.execute('''SELECT video_name ,channel_name ,Like_count
                       FROM videos where Like_count is not null
                       ORDER BY Like_count desc''')
    t5 = cursor.fetchall()
    df5 = pd.DataFrame(t5, columns=["VIDEO_NAME", "CHANNEL_NAME", "LIKE_COUNT"])
    st.write(df5)

elif question == '6.Number of likes for each video, and their video names?':
    cursor.execute('''SELECT Like_count, video_name
                       FROM videos
                       ORDER BY Like_count desc''')
    t6 = cursor.fetchall()
    df6 = pd.DataFrame(t6, columns=["LIKE_COUNT", "VIDEO_NAME"])
    st.write(df6)

elif question == '7.Number of views for each channel, and their channel names?':
    cursor.execute('''SELECT channel_name ,channel_views
                       FROM channels''')

    t7 = cursor.fetchall()
    df7 = pd.DataFrame(t7, columns=["CHANNEL_NAME", "TOTAL_VIEWS"])
    st.write(df7)

elif question == '8.Channels that have published videos in the year 2022':
    cursor.execute('''SELECT channel_name ,published,video_name
                       FROM videos
                       where extract(year from published)=2022''')

    t8 = cursor.fetchall()
    df8 = pd.DataFrame(t8, columns=["CHANNEL_NAME", "PUBLISHED DATE", "VIDEO_NAME"])
    st.write(df8)

elif question == '9.Average duration of all videos in each channel, and their channel names?':
    cursor.execute('''SELECT channel_name,AVG(duration)
                       FROM videos GROUP BY channel_name''')

    t9 = cursor.fetchall()
    df9 = pd.DataFrame(t9, columns=["CHANNEL_NAME", "AVG_DURATION"])

    duration = []
    for index, row in df9.iterrows():
        channel_nam = row["CHANNEL_NAME"]
        avg_duration = row["AVG_DURATION"]
        avg_duration_str = str(avg_duration)
        duration.append(dict(ch_name=channel_nam, aver_duration=avg_duration_str))
    duration_df = pd.DataFrame(duration)

    st.write(duration_df)

elif question == '10.Videos with highest comments, and their channel names?':
    cursor.execute('''SELECT video_name,Comment_count , channel_name
                       FROM videos where Comment_count is not null
                        ORDER BY Comment_count desc''')

    t10 = cursor.fetchall()
    df10 = pd.DataFrame(t10, columns=["VIDEO_NAME", "COMMENT_COUNT", "CHANNEL_NAME", ])
    st.write(df10)

# python -m streamlit run C:\Users\user\PycharmProjects\pythonProject\Final.py
