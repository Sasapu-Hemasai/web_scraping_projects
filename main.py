import pandas as pd
import mysql.connector as connection
import requests
from bs4 import BeautifulSoup
import pymongo
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from flask import Flask,render_template,request

app = Flask(__name__)

#this checks and returns the playlist id of the channel
def playlistid(channel_id, api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    channel_id = channel_id
    channels_response = youtube.channels().list(
        part="contentDetails",
        id=channel_id
    ).execute()
    playlist_id = channels_response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
    return playlist_id

#this checks and returns the video ids of the channel
def videosids(channel_id, api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    max_results = 50
    videos = youtube.playlistItems().list(
        part="snippet",
        playlistId=playlistid(channel_id, api_key),
        maxResults=max_results,
    ).execute()
    video_ids = []
    for i in range(len(videos['items'])):
        video_id = videos['items'][i]['snippet']['resourceId']['videoId']
        video_ids.append(video_id)
    return video_ids

#this returns the detaisls of video ids of the channel
def details(channel_id, api_key):
    youtube = build("youtube", "v3", developerKey=api_key)
    vids = videosids(channel_id, api_key)
    video_id_string = ','.join(vids)
    video_response = youtube.videos().list(
        part="snippet,statistics",
        id=video_id_string
    ).execute()
    details = []
    for i in range(len(video_response['items'])):
        title = video_response['items'][i]['snippet']['title']
        vid = video_response['items'][i]['id']
        video_url = f"https://www.youtube.com/watch?v={vid}"
        likes = video_response['items'][i]['statistics']['likeCount']
        no_of_comments = video_response['items'][i]['statistics'].get('commentCount', 0)
        thumbnail = video_response['items'][i]['snippet']['thumbnails']['default']['url']
        details.append(
            {"title": title, "video_id": vid, "video_url": video_url, "likes": likes, "no_of_comments": no_of_comments,
             "thumbnail": thumbnail})

    return details

# creating a df and storing it in sql
def storingtosql(channel_id, api_key):
    video_details = details(channel_id, api_key)
    df = pd.DataFrame(video_details)
    df['no_of_comments'] = df['no_of_comments'].astype(int)
    l = list(df.columns)
    l1 = []
    for i in l:
        i1 = i.replace(" ", "_")
        l1.append(i1)
    df.columns = l1
    dict1 = {}
    for i in range(len(df.columns)):
        dict1[df.dtypes.index[i]] = str(df.dtypes.values[i])
    string = ""
    for k, v in dict1.items():
        string = string + f"{k} {v},"
    replace = {
        'object': 'VARCHAR(200)',
        'float64': 'FLOAT',
        'int64': 'INT',
        'int32': 'INT'
    }
    newstr = string
    for k, v in replace.items():
        newstr = newstr.replace(k, v)
    newstr = newstr.strip(",")

    return newstr, df

def get_channel_id(usernamee):
    url = f"https://www.youtube.com/{usernamee}"
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    canonical_tag = soup.find("link", rel="canonical")
    if canonical_tag:
        canonical_url = canonical_tag.get("href")
        if canonical_url:
            canonical_url = str(canonical_url)
            channel_Id = canonical_url.strip("https://www.youtube.com/channel/")
            return channel_Id
    else:
        return None

#gets all the comments for a single video
def get_comments_for_video(api_key, video_id):
    youtube = build("youtube", "v3", developerKey=api_key)
    comments = []

    next_page_token = None
    while True:
        # Fetch comment threads for the video
        comments_response = youtube.commentThreads().list(
            part="snippet",
            videoId=video_id,
            maxResults=100,
            pageToken=next_page_token
        ).execute()

        # Extract comments and commenter names from the response
        for comment in comments_response.get("items", []):
            comment_text = comment["snippet"]["topLevelComment"]["snippet"]["textDisplay"]
            commenter_name = comment["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"]
            comments.append({"videoId": video_id, "name": commenter_name, "comment": comment_text})

        # Check if there are more comments in the next page
        next_page_token = comments_response.get("nextPageToken")
        if not next_page_token:
            break

    return comments

#gets comments for all the videos which calls the get_comments_for_video function
def get_comments_for_videos(api_key, video_ids):
    all_comments = []

    for video_id in video_ids:
        try:
            video_comments = get_comments_for_video(api_key, video_id)
            all_comments.extend(video_comments)
        except HttpError as e:
            error_message = e._get_reason()
            continue

    return all_comments


def commentsfornosql(api_key, video_ids):
    video_comments = get_comments_for_videos(api_key, video_ids)
    comments = {}

    for i in video_comments:
        videoid = i['videoId']
        name = i['name']
        comment = i['comment']

        if videoid not in comments:
            comments[videoid] = {}
        if name not in comments[videoid]:  # Fix the condition here
            comments[videoid][name] = []

        comments[videoid][name].append(comment)
    return comments
#editing the comments in a list to push to nosql
def results_lists(api_key, video_ids):
    result_list = []
    comments_dict = commentsfornosql(api_key, video_ids)
    for video_id, details in comments_dict.items():
        video_dict = {'video_id': video_id}
        video_dict.update(details)
        result_list.append(video_dict)
    return result_list

#inserting the results list to nosql
def insertingtonosql(usernamee, api_key, video_ids):
    rs = results_lists(api_key, video_ids)
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db1 = client['hemasai']
    user = usernamee.strip("@")
    luser = user.lower()
    collection_name = f"collection_{luser}"
    collection = db1[collection_name]
    collection.insert_many(rs)

    return collection_name

def datafetch(usernamee, channel_id, api_key):
    details_result, df = storingtosql(channel_id, api_key)
    username = "root"
    password = "W@2915djkq#"
    hostt = "localhost"
    mysql = connection.connect(host=hostt, user=username, passwd=password, use_pure=True)
    cur = mysql.cursor()
    cur.execute("use mysql")
    user = usernamee.strip("@")
    luser = user.lower()
    tablename = f"youtube_video_{luser}"
    create_query = f"""CREATE TABLE IF NOT EXISTS {tablename}({details_result})"""
    cur.execute(create_query)
    mysql.commit()
    rows = [tuple(row) for row in df.values]
    columns = ", ".join(df.columns)
    stre = ""
    for i in range(len(df.columns)):
        stre = stre + "%s, "
    stre = stre.strip(", ")
    insert_query = f"""INSERT INTO {tablename} ({columns}) VALUES ({stre})"""
    cur.executemany(insert_query, rows)
    mysql.commit()
    select_query = f"SELECT * FROM {tablename};"
    cur.execute(select_query)

    return tablename, select_query
def fetch(usernamee, channel_id, api_key, video_ids):
    tablename, select_query = datafetch(usernamee, channel_id, api_key)
    collection_name = insertingtonosql(usernamee, api_key, video_ids)
    username = "root"
    password = "W@2915djkq#"
    hostt = "localhost"
    mysql = connection.connect(host=hostt, user=username, passwd=password, use_pure=True)
    cur = mysql.cursor()
    cur.execute("use mysql")
    select_query = f"SELECT * FROM {tablename};"
    cur.execute(select_query)
    sql_data = cur.fetchall()

    client = pymongo.MongoClient("mongodb://localhost:27017")
    db1 = client['hemasai']
    collection = db1[collection_name]
    nsdata = list(collection.find())
    nosql_data = nosqldatafromatter(nsdata)
    return sql_data, nosql_data

@app.route('/')
def homepage():
    return render_template('home.html')

def checkusernamesql(usernamee):
    user = usernamee.strip("@")
    luser = user.lower()
    tablename = f"youtube_video_{luser}"
    username = "root"
    password = "W@2915djkq#"
    hostt = "localhost"
    mysql = connection.connect(host=hostt, user=username, passwd=password, use_pure=True)
    cur = mysql.cursor()
    cur.execute("use mysql")
    check_query = f" SHOW TABLEs LIKE '{tablename}'"
    cur.execute(check_query)
    result = cur.fetchone()
    if result:
        count_rows_query = f"SELECT COUNT(*) FROM {tablename}"
        cur.execute(count_rows_query)
        row_count = cur.fetchone()[0]
        if row_count > 0:
            mysql.cursor()
            select_query = f"SELECT * FROM {tablename}"
            cur.execute(select_query)
            sql_data = cur.fetchall()
            cur.close()
            mysql.close()
            return sql_data
    else:
        sql_data = "no data"
        return sql_data
def checkusernamenosql(usernamee):
    user = usernamee.strip("@")
    luser = user.lower()
    client = pymongo.MongoClient("mongodb://localhost:27017")
    db1 = client['hemasai']
    collection_name = f"collection_{luser}"
    collection = db1[collection_name]
    data = list(collection.find())
    if data:
        nosql_data = data
        return nosql_data
    else:
        nosql_data = "no data"
        return nosql_data

def nosqldatafromatter(nsdata):
    datalis = []
    for i in nsdata:
        video_id = i['video_id']
        for k, v in i.items():
            datalis.append([video_id, k, v])
    for i in datalis:
        if '_id' in i:
            datalis.remove(i)
    for i in datalis:
        if 'video_id' in i:
            datalis.remove(i)
    finallis = []
    for i in datalis:
        idd = i[0]
        name = i[1]
        comment = i[2][0]
        finallis.append([idd, name, comment])
    return finallis


@app.route('/search', methods=['POST'])
def get_final_data():
    if request.method == 'POST':
        usernamee = request.form['username']
        sdata = checkusernamesql(usernamee)
        nsdata = checkusernamenosql(usernamee)
        if sdata != "no data" and nsdata != "no data":
            sqldata = sdata
            nosqldata = nosqldatafromatter(nsdata)
            return render_template('results.html', sql_data=sqldata, nosql_data=nosqldata)
        else:
            api_key = "AIzaSyDBKfHSCoJO-IiPlU0PZrHSIttvSizfgxU"
            channel_id = get_channel_id(usernamee)
            video_ids = videosids(channel_id, api_key)
            sqldata,nosqldata = fetch(usernamee, channel_id, api_key, video_ids)
            return render_template('results.html', sql_data=sqldata, nosql_data=nosqldata)

if __name__ == "__main__":
    app.run(debug=True)