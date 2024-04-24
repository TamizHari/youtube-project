from googleapiclient.discovery import build
import pymongo
import mysql.connector
import pandas as pd
import streamlit as st

#API KEY CONNECTION

def Api_connect():
    Api_ID="...API KEY.?.."
    api_service_name="youtube"
    api_version="v3"

    youtube=build(api_service_name,api_version,developerKey=Api_ID)
    
    return youtube

youtube=Api_connect()


# get channels information
def get_Channel_info(channel_id):
    request=youtube.channels().list(
                    part="snippet,contentDetails,statistics",
                    id=channel_id
    )
    response=request.execute()

    for i in response['items']:
        data=dict(Channel_Name=i["snippet"]["title"],
                Channel_ID=i["id"],
                Subscribers=i['statistics']['subscriberCount'],
                views=i["statistics"]["viewCount"],
                Total_Videos=i["statistics"]["videoCount"],
                Channel_Description=i["snippet"]["description"],
                Playlist_ID=i["contentDetails"]["relatedPlaylists"]["uploads"]
                )
    return data


#get video ID's
def get_videos_ids(channel_id):
    video_ids=[]
    response=youtube.channels().list(id=channel_id,
                                    part='contentDetails').execute()
    Playlist_ID=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']

    next_page_token=None
    while True:
        response1=youtube.playlistItems().list(
                                            part='snippet',
                                            playlistId=Playlist_ID,
                                            maxResults=197,
                                            pageToken=next_page_token).execute()
        for i in range(len(response1['items'])):
            video_ids.append(response1['items'][i]['snippet']['resourceId']['videoId'])
        next_page_token=response1.get('nextPageToken')

        if next_page_token is None:
            break
    return video_ids


#get video information
def get_video_info(video_ids):
    video_data=[]
    for video_id in video_ids:
        request=youtube.videos().list(
            part="snippet,contentDetails,statistics",
            id=video_id
        )
        response=request.execute()
        for item in response["items"]:
            data=dict(Channel_name=item['snippet']['channelTitle'],
                    Channel_Id=item['snippet']['channelId'],
                    Video_Id=item['id'],
                    Title=item['snippet']['title'],
                    Tags=item['snippet'].get('tags'),
                    Thumbnail=item['snippet']['thumbnails']['default']['url'],
                    Description=item['snippet'].get('description'),
                    Published_Dates=item['snippet']['publishedAt'],
                    Duration=item['contentDetails'].get('duration'),
                    Likes=item['statistics'].get('likeCount'),
                    Views=item['statistics'].get('viewCount'),
                    comments=item['statistics'].get('commentCount'),
                    favorite_count=item['statistics']['favoriteCount'],
                    Definition=item['contentDetails']['definition'],
                    Caption_status=item['contentDetails']['caption']
                    )
            video_data.append(data)
    return video_data


#get comment Information
def get_comment_info(video_ids):
    Comment_data=[]
    try:
        for video_id in video_ids:
            request=youtube.commentThreads().list(
                part="snippet",
                videoId=video_id,
                maxResults=50,
            )
            response=request.execute()

            for item in response['items']:
                data=dict(Comment_Id=item['snippet']['topLevelComment']['id'],
                        Video_Id=item['snippet']['topLevelComment']['snippet']['videoId'],
                        Comment_Text=item['snippet']['topLevelComment']['snippet']['textDisplay'],
                        Comment_Author=item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                        Comment_Published=item['snippet']['topLevelComment']['snippet']['publishedAt']
                        )
                Comment_data.append(data)
    except:
        pass
    return Comment_data


#get_playlist_details
def get_playlist_details(channel_id):
        next_page_token=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part='snippet,contentDetails',
                        channelId=channel_id,
                        maxResults=50,
                        pageToken=next_page_token
                )
                response=request.execute()

                for item in response['items']:
                        data=dict(playlist_Id=item['id'],
                                Title=item['snippet']['title'],
                                Channel_Id=item['snippet']['channelId'],
                                Channel_Name=item['snippet']['channelTitle'],
                                PublishedAt=item['snippet']['publishedAt'],
                                Videos_Count=item['contentDetails']['itemCount'])
                        All_data.append(data)
                next_page_token=response.get('nextPageToken')
                if next_page_token is None:
                        break
        return All_data


#upload to mongoDB

client=pymongo.MongoClient("--mongoDB link--")
db=client["Youtube_data"]


def channel_details(channel_id):
    ch_details=get_Channel_info(channel_id)
    pl_details=get_playlist_details(channel_id)
    vi_ids=get_videos_ids(channel_id)
    vi_details=get_video_info(vi_ids)
    com_deatils=get_comment_info(vi_ids)
    
    coll1=db["channel_details"]
    coll1.insert_one({"channel_information":ch_details,
                      "playlist_information":pl_details,
                      "video_information":vi_details,
                      "comment_information":com_deatils})
    return "upload completed Successfully" 


#--------------Table Creation for  channels,playlist,videos,comments-------------->>>>>>>>>

def channels_table(channel_name_s):
    import mysql.connector

    mydb = mysql.connector.connect(host="localhost",user="root",password="")

    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute('show databases')
    for i in mycursor:
        print(i)


    create_query="""create table if not exists youtube_data_harvest.channels (Channel_Name varchar(100),
                                                    Channel_Id varchar(80) primary key,
                                                    Subscribers int,
                                                    views int,
                                                    Total_videos int,
                                                    Channel_Description Varchar(20),
                                                    Playlist_Id Varchar(80))"""
    mycursor.execute(create_query)
    mydb.commit()
                
    single_channel_detail=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name": channel_name_s},{"_id":0}):
        single_channel_detail.append(ch_data["channel_information"])

    df_single_channel_detail=pd.DataFrame(single_channel_detail)

    for index,row in df_single_channel_detail.iterrows():
        insert_query='''insert into youtube_data_harvest.channels(Channel_Name ,
                                            Channel_ID,
                                            Subscribers,
                                            views,
                                            Total_Videos,
                                            Channel_Description,
                                            Playlist_ID)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s)'''
        values=(row['Channel_Name'],
                row['Channel_ID'],
                row['Subscribers'],
                row['views'],
                row['Total_Videos'],
                row['Channel_Description'],
                row['Playlist_ID'])
        
        try:
            mycursor.execute(insert_query,values)
            mydb.commit()
        except:
            Note=f"Your provided channel Name {channel_name_s} is already exists"
            return Note

#-------------------create playlists table------------->>>>>>>>>>>>>

def playlist_table(channel_name_s):
    import mysql.connector

    mydb = mysql.connector.connect(host="localhost",user="root",password="")

    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute('show databases')
    for i in mycursor:
        print(i)
    

 
    create_query="""create table if not exists youtube_data_harvest.playlists (playlist_Id varchar(100) primary key,
                                                    Title varchar(100),
                                                    Channel_Id varchar(100),
                                                    Channel_Name varchar(100),
                                                    PublishedAt timestamp,
                                                    Videos_Count int
                                                    )"""

    mycursor.execute(create_query)
    mydb.commit()
       
    single_playlist_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_playlist_details.append(ch_data["playlist_information"])

    df_single_playlist_details=pd.DataFrame(single_playlist_details[0])
        
    for index,row in df_single_playlist_details.iterrows():
        insert_query='''insert into youtube_data_harvest.playlists(playlist_Id,
                                            Title,
                                            Channel_Id,
                                            Channel_Name,
                                            PublishedAt,
                                            Videos_Count)
                                            
                                            values(%s,%s,%s,%s,%s,%s)'''
     
        values=(row['playlist_Id'],
                row['Title'],
                row['Channel_Id'],
                row['Channel_Name'],
                row['PublishedAt'],
                row['Videos_Count'])
                
      
        mycursor.execute(insert_query,values)
        mydb.commit()  


#-------------------create videos table----------->>>>>>>>>>>>>

def videos_table(channel_name_s):

    import mysql.connector

    mydb = mysql.connector.connect(host="localhost",user="root",password="")

    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute('show databases')
    for i in mycursor:
        print(i)
 

    create_query="""create table if not exists youtube_data_harvest.Videos(Channel_name varchar(100),
                                                            Channel_Id varchar(100),
                                                            Video_Id varchar(60) primary key,
                                                            Title varchar(80),
                                                            Tags varchar(50),
                                                            Thumbnail varchar(200),
                                                            Description varchar(50),
                                                            Published_Dates timestamp,
                                                            duration int,
                                                            Likes int,
                                                            Views int,
                                                            comments int,
                                                            favorite_count int,
                                                            Definition varchar(10),
                                                            Caption_status varchar(50)
                                                            )"""

    mycursor.execute(create_query)
    mydb.commit()
        
    
    single_video_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_video_details.append(ch_data["video_information"])

    df_single_video_details=pd.DataFrame(single_video_details[0])
    
    for index,row in df_single_video_details.iterrows():
        insert_query='''insert into youtube_data_harvest.Videos(Channel_name,
                                                                Channel_Id,
                                                                Video_Id,
                                                                Title,
                                                                Tags,
                                                                Thumbnail,
                                                                Description,
                                                                Published_Dates,
                                                                duration,
                                                                Likes,
                                                                Views,
                                                                comments,
                                                                favorite_count,
                                                                Definition,
                                                                Caption_status)
                                            
                                            values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)'''
        
        values=(row['Channel_name'],row['Channel_Id'],row['Video_Id'],row['Title'],row['Tags'],row['Thumbnail'],
                row['Description'],row['Published_Dates'],row['Duration'],row['Likes'],row['Views'],row['comments'],
                row['favorite_count'],row['Definition'],row['Caption_status'])
                
        
        mycursor.execute(insert_query,values)
        mydb.commit()
    
#----------------------create comment table-------------------->>>>>>>>>>>>>>>>>

def comments_table(channel_name_s):
    mydb = mysql.connector.connect(host="localhost",user="root",password="")

    print(mydb)
    mycursor = mydb.cursor(buffered=True)
    mycursor.execute('show databases')
    for i in mycursor:
        print(i)


    create_query="""create table if not exists youtube_data_harvest.Comments(Comment_Id varchar(100) primary key,
                                                                            Video_Id varchar(50),
                                                                            Comment_Text text,
                                                                            Comment_Author varchar(150),
                                                                            Comment_Published timestamp
                                                                            )"""

    mycursor.execute(create_query)
    mydb.commit()

    single_comments_details=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({"channel_information.Channel_Name":channel_name_s},{"_id":0}):
        single_comments_details.append(ch_data["comment_information"])

    df_single_comments_details=pd.DataFrame(single_comments_details[0])
                
    for index,row in df_single_comments_details.iterrows():
        insert_query='''insert into youtube_data_harvest.Comments(Comment_Id,
                                                                Video_Id,
                                                                Comment_Text,
                                                                Comment_Author,
                                                                Comment_Published)
                                            
                                                    values(%s,%s,%s,%s,%s)'''

        values=(row['Comment_Id'],
                row['Video_Id'],
                row['Comment_Text'],
                row['Comment_Author'],
                row['Comment_Published'])
                

        mycursor.execute(insert_query,values)
        mydb.commit() 

#-------------------------Execute All Functions------------------>>>>>>>>>>>>>>>

def tables(singe_channel):
    Note=channels_table(singe_channel)
    if Note:
        return Note
    else:
        playlist_table(singe_channel)
        videos_table(singe_channel)
        comments_table(singe_channel)

    return "Tables Created Successfully"


#-----------------------Streamlit Functions------------>>>>>>>>>>>

def show_channels_table():
    ch_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_list.append(ch_data["channel_information"])
    df=st.dataframe(ch_list)

    return df

def show_playlists_table():    
    ply_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ply_data in coll1.find({},{"_id":0,"playlist_information":1}):
        for i in range(len(ply_data["playlist_information"])):
            ply_list.append(ply_data["playlist_information"][i])
        df1=st.dataframe(ply_list)

        return df1

def show_videos_table():    
    vi_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for vi_data in coll1.find({},{"_id":0,"video_information":1}):
        for i in range(len(vi_data["video_information"])):
            vi_list.append(vi_data["video_information"][i])
        df2=st.dataframe(vi_list)

        return df2
    
def show_comments_table():
    com_list=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for com_data in coll1.find({},{"_id":0,"comment_information":1}):
        for i in range(len(com_data["comment_information"])):
            com_list.append(com_data["comment_information"][i])
        df3=st.dataframe(com_list)

    return df3 


#-------------------------Streamlit CODE---------------->>>>>>>>>>>>>>>>>>>

with st.sidebar:
    st.title(":red[YOUTUBE DATA HARVESTING AND WAREHOUSING]")
    st.header("Skill Take Away")
    st.caption("Python Scripting")
    st.caption("MongoDB")
    st.caption("API Integration")
    st.caption("Data Management using MongoDB and SQL")

channel_id=st.text_input("Enter the channel ID")

if st.button("collect and store data"):
    ch_ids=[]
    db=client["Youtube_data"]
    coll1=db["channel_details"]
    for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
        ch_ids.append(ch_data["channel_information"]["Channel_ID"])

    if channel_id in ch_ids:
        st.success("Channel Details of the given channel id already exists")

    else:
        insert=channel_details(channel_id)
        st.success(insert)

all_channels=[]
db=client["Youtube_data"]
coll1=db["channel_details"]
for ch_data in coll1.find({},{"_id":0,"channel_information":1}):
    all_channels.append(ch_data['channel_information']['Channel_Name'])

unique_channel=st.selectbox("Select the channel",all_channels)        

if st.button("Migrate to Sql"):
    Table=tables(unique_channel)
    st.success(Table)

show_table=st.radio("SELECT THE TABLE FOR VIEW",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if show_table=="CHANNELS":
    show_channels_table()

elif show_table=="PLAYLISTS":
    show_playlists_table()

elif show_table=="VIDEOS":
    show_videos_table()

elif show_table=="COMMENTS":
    show_comments_table()


#---------------------SQL CONNECTION WITH QUESTIONS--------------->>>>>>>>>>>>>

mydb = mysql.connector.connect(host="localhost",user="root",password="")

print(mydb)
mycursor = mydb.cursor(buffered=True)
mycursor.execute('show databases')
for i in mycursor:
    print(i)

question=st.selectbox("Select your question",("1. ALL th videos and the channel name",
                                              "2. channels with most number of videos",
                                              "3. 10 most viewed videos",
                                              "4. comments in each videos",
                                              "5. Videos with highest likes",
                                              "6. likes of all videos",
                                              "7. views of each channels",
                                              "8. videos published in the year of 2023",
                                              "9. average duration of all videos in each channel",
                                              "10. videos with highest number of comments"))    

if question=="1. ALL th videos and the channel name":
    query1='''select title as videos,channel_name as channelname from youtube_data_harvest.videos '''
    mycursor.execute(query1)
    mydb.commit()
    t1=mycursor.fetchall()
    df=pd.DataFrame(t1,columns=["video title","channel name"])
    st.write(df)

elif question=="2. channels with most number of videos":
    query2='''select channel_name as channelname,total_videos as no_videos from youtube_data_harvest.channels 
                order by total_videos desc'''
    mycursor.execute(query2)
    mydb.commit()
    t2=mycursor.fetchall()
    df2=pd.DataFrame(t2,columns=["channel name","No of videos"])
    st.write(df2)

elif question=="3. 10 most viewed videos":
    query3='''select views as views,channel_name as channelname,title as videotitle from youtube_data_harvest.videos
            where views is not null order by views desc limit 10 '''
    mycursor.execute(query3)
    mydb.commit()
    t3=mycursor.fetchall()
    df3=pd.DataFrame(t3,columns=["views","channel name","videotitle"])
    st.write(df3)

elif question=="4. comments in each videos":
    query4='''select comments as no_comments,title as videotitle from youtube_data_harvest.videos where comments is not null '''
    mycursor.execute(query4)
    mydb.commit()
    t4=mycursor.fetchall()
    df4=pd.DataFrame(t4,columns=["no of comments","videotitle"])
    st.write(df4)

elif question=="5. Videos with highest likes":
    query5='''select title as videotitle,channel_name as channelname,likes as likecount from youtube_data_harvest.videos where likes is not null order by likes desc '''
    mycursor.execute(query5)
    mydb.commit()
    t5=mycursor.fetchall()
    df5=pd.DataFrame(t5,columns=["videotitle","channelname","likecount"])
    st.write(df5)

elif question=="6. likes of all videos":
    query6='''select likes as likecount,title as videostitle from youtube_data_harvest.videos'''
    mycursor.execute(query6)
    mydb.commit()
    t6=mycursor.fetchall()
    df6=pd.DataFrame(t6,columns=["likecount","videotitle"])
    st.write(df6)

elif question=="7. views of each channels":
    query7='''select channel_name as channelname,views as totalviews from youtube_data_harvest.videos'''
    mycursor.execute(query7)
    mydb.commit()
    t7=mycursor.fetchall()
    df7=pd.DataFrame(t7,columns=["channelname","totalviews"])
    st.write(df7)
    
elif question=="8. videos published in the year of 2023":
    query8='''select title as video_title, Published_dates as videorelease,channel_name as channelname from youtube_data_harvest.videos
                where extract(year from published_dates)=2023'''
    mycursor.execute(query8)
    mydb.commit()
    t8=mycursor.fetchall()
    df8=pd.DataFrame(t8,columns=["videotitle","videorelease","channelname"])
    st.write(df8)
elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname, AVG(duration) as averageduration from youtube_data_harvest.videos
                group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    st.write(df9)
elif question=="9. average duration of all videos in each channel":
    query9='''select channel_name as channelname, AVG(duration) as averageduration from youtube_data_harvest.videos
                group by channel_name'''
    mycursor.execute(query9)
    mydb.commit()
    t9=mycursor.fetchall()
    df9=pd.DataFrame(t9,columns=["channelname","averageduration"])
    T9=[]
    for index,row in df9.iterrows():
        channel_title=row["channelname"]
        average_duration=row["averageduration"]
        average_duration_str=str(average_duration)
        T9.append(dict(channeltitle=channel_title,avgduration=average_duration_str))
        df1=pd.DataFrame(T9)
        st.write(df1)
elif question=="10. videos with highest number of comments":
    query10='''select title as videotitle,channel_name as channelname,comments as comments from youtube_data_harvest.videos
                where comments is not null order by comments desc'''
    mycursor.execute(query10)
    mydb.commit()
    t10=mycursor.fetchall()
    df10=pd.DataFrame(t10,columns=["videotitle","channelname","comments"])
    st.write(df10)