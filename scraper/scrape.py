import googleapiclient.discovery
import os
import time
from datetime import datetime
from upload_to_s3 import create_bucket_if_not_exists, upload_file_to_s3
import google.generativeai as genai

def suggest_queries(api_key):
    
    genai.configure(api_key=api_key)

    model = genai.GenerativeModel('gemini-pro')

    prompt = """Act as data analyst and suggest exactly 40 topics that can be searched using youtube api that will give distinct content creator for each query.
    the result should be comma seperated, without any bullets or numbers and within 5 words"""
    response = model.generate_content(prompt)
    return response.text


def search_channels(youtube, query, max_results=50):
    request = youtube.search().list(
        part="snippet",
        q=query,
        type="channel",
        maxResults=max_results
    )
    response = request.execute()
    return response['items']

def get_channel_details(youtube,channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()
    return response['items'][0]

def is_monetized(channel_data):
    try:
        subscriber_count = int(channel_data['statistics']['subscriberCount'])
        view_count = int(channel_data['statistics']['viewCount'])
        video_count = int(channel_data['statistics']['videoCount'])
        if subscriber_count > 1000 and view_count > 10000 and video_count > 10:
            return True
        else:
            return False
    except KeyError:
        return False

def scrape_data(youtube, query):

    monetized_channels = []
    channels = search_channels(youtube, query)
    for channel in channels:
        channel_id = channel['id']['channelId']
        channel_data = get_channel_details(youtube,channel_id)
        channel_data['query'] = query
        if is_monetized(channel_data):
            monetized_channels.append(channel_data)
        time.sleep(1)  # Respect API rate limits

    return monetized_channels
        