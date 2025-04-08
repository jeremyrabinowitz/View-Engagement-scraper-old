import os
import time
import requests
from urllib.parse import urlparse, parse_qs
from dotenv import load_dotenv
from pathlib import Path

# 🔐 Load secrets from .env file (used in Render secret file)
load_dotenv(dotenv_path=Path(".env"))

# 📦 Environment variables for Airtable and YouTube
AIRTABLE_API_KEY = os.environ['AIRTABLE_API_KEY']
AIRTABLE_BASE_ID = os.environ['AIRTABLE_BASE_ID']
AIRTABLE_TABLE_NAME = os.environ['AIRTABLE_TABLE_NAME']
AIRTABLE_VIEW_NAME = os.environ['AIRTABLE_VIEW_NAME']
YOUTUBE_API_KEY = os.environ['YOUTUBE_API_KEY']


# 🎯 Extract YouTube video ID from different possible URL formats
def extract_video_id(url):
    if not url:
        return None
    parsed_url = urlparse(url)
    if 'youtu.be' in parsed_url.netloc:
        return parsed_url.path.strip("/")
    if 'youtube.com' in parsed_url.netloc and parsed_url.path.startswith('/live/'):
        return parsed_url.path.split('/live/')[-1].split('/')[0]
    if 'youtube.com' in parsed_url.netloc:
        query = parse_qs(parsed_url.query)
        return query.get('v', [None])[0]
    return None


# 📊 Use the YouTube Data API to fetch view/like/comment stats
def get_youtube_stats(video_id):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=statistics&id={video_id}&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    if response.status_code != 200:
        return None
    data = response.json()
    if "items" in data and data["items"]:
        stats = data["items"][0]["statistics"]
        return {
            "views": int(stats.get("viewCount", 0)),
            "likes": int(stats.get("likeCount", 0)),
            "comments": int(stats.get("commentCount", 0)),
        }
    return None


# 📥 Fetch all Airtable records from the specified view (handles pagination)
def get_airtable_records():
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {"Authorization": f"Bearer {AIRTABLE_API_KEY}"}
    all_records = []
    offset = None

    while True:
        params = {
            "pageSize": 100,
            "view": AIRTABLE_VIEW_NAME  # ⬅️ Filters to only the relevant view
        }
        if offset:
            params["offset"] = offset

        response = requests.get(url, headers=headers, params=params)
        data = response.json()
        batch = data.get("records", [])
        all_records.extend(batch)

        offset = data.get("offset")
        if not offset:
            break  # No more pages to fetch

    return all_records


# 🔁 Push engagement stat updates back to Airtable in batches of 10
def batch_update_airtable(records_to_update):
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{AIRTABLE_TABLE_NAME}"
    headers = {
        "Authorization": f"Bearer {AIRTABLE_API_KEY}",
        "Content-Type": "application/json"
    }

    for i in range(0, len(records_to_update), 10):
        batch = {"records": records_to_update[i:i+10]}
        requests.patch(url, headers=headers, json=batch)
        time.sleep(0.25)  # ⏱ Prevent hitting Airtable rate limits


# 🚀 Main process: fetch Airtable records, get YouTube stats, update Airtable
def main():
    records = get_airtable_records()
    print(f"🧾 Pulled {len(records)} records from Airtable.")

    updates = []

    for record in records:
        fields = record.get("fields", {})
        url = fields.get("Asset Link")

        if not url:
            continue  # Skip records with no video URL

        video_id = extract_video_id(url)
        if not video_id:
            continue  # Skip if video ID couldn't be parsed

        stats = get_youtube_stats(video_id)
        if not stats:
            continue  # Skip if stats couldn't be retrieved

        updates.append({
            "id": record["id"],
            "fields": {
                "Views": stats["views"],
                "Likes": stats["likes"],
                "Comments": stats["comments"]
            }
        })

    if updates:
        batch_update_airtable(updates)
        print(f"✅ Successfully updated {len(updates)} records.")
    else:
        print("⚠️ No updates were needed.")


# 🏁 Kick off the main process
if __name__ == "__main__":
    main()
