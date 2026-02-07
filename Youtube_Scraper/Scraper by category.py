#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import requests
import sys
import time
import os
import argparse
from typing import List, Dict, Optional

# List of simple to collect features
SNIPPET_FEATURES = [
    "title",
    "publishedAt",
    "channelId",
    "channelTitle",
    "categoryId"
]

# Any characters to exclude, generally these are things that become problematic in CSV files
UNSAFE_CHARACTERS = ['\n', '"']

# Used to identify columns
HEADER = [
    "video_id"] + SNIPPET_FEATURES + [
    "trending_date", "tags", "view_count", "likes", "dislikes",
    "comment_count", "thumbnail_link", "comments_disabled",
    "ratings_disabled", "description", "duration"
]

# YouTube video categories
CATEGORIES = {
    "1": "Film & Animation",
    "2": "Autos & Vehicles",
    "10": "Music",
    "15": "Pets & Animals",
    "17": "Sports",
    "20": "Gaming",
    "22": "People & Blogs",
    "23": "Comedy",
    "24": "Entertainment",
    "25": "News & Politics",
    "26": "Howto & Style",
    "27": "Education",
    "28": "Science & Technology"
}


def setup(api_path: str, code_path: str) -> tuple:
    """Read API key and country codes from files."""
    try:
        with open(api_path, 'r') as file:
            api_key = file.readline().strip()
        
        if not api_key:
            raise ValueError("API key is empty")
        
        with open(code_path) as file:
            country_codes = [x.rstrip() for x in file if x.strip()]
        
        if not country_codes:
            raise ValueError("No country codes found")
        
        return api_key, country_codes
    
    except FileNotFoundError as e:
        print(f"Error: Could not find file - {e}")
        sys.exit(1)
    except Exception as e:
        print(f"Error during setup: {e}")
        sys.exit(1)


def prepare_feature(feature) -> str:
    """Removes unsafe characters and surrounds the item in quotes."""
    for ch in UNSAFE_CHARACTERS:
        feature = str(feature).replace(ch, "")
    return f'"{feature}"'


def api_request(page_token: str, country_code: str, api_key: str, category_id: str = None) -> Optional[Dict]:
    """Makes API request to YouTube and returns JSON response."""
    # Build URL with optional category filter
    category_param = f'&videoCategoryId={category_id}' if category_id else ''
    
    request_url = (
        f'https://www.googleapis.com/youtube/v3/videos?'
        f'part=id,statistics,snippet,contentDetails&'
        f'{page_token}'
        f'chart=mostPopular&'
        f'regionCode={country_code}&'
        f'maxResults=50'
        f'{category_param}&'
        f'key={api_key}'
    )
    
    try:
        request = requests.get(request_url, timeout=10)
        
        if request.status_code == 429:
            print("Temp-Banned due to excess requests, please wait and continue later")
            sys.exit()
        elif request.status_code == 403:
            print("Access forbidden. Check your API key and quota.")
            sys.exit(1)
        elif request.status_code != 200:
            print(f"API request failed with status code: {request.status_code}")
            print(f"Response: {request.text}")
            return None
        
        return request.json()
    
    except requests.exceptions.Timeout:
        print("Request timed out")
        return None
    except requests.exceptions.RequestException as e:
        print(f"Request error: {e}")
        return None


def get_tags(tags_list: List[str]) -> str:
    """Takes a list of tags, prepares each tag and joins them with pipe character."""
    if not tags_list:
        tags_list = ["[none]"]
    return prepare_feature("|".join(tags_list))


def get_videos(items: List[Dict]) -> List[str]:
    """Extract video data from API response items."""
    lines = []
    
    for video in items:
        comments_disabled = False
        ratings_disabled = False
        
        # Skip videos without statistics (often deleted videos)
        if "statistics" not in video:
            continue
        
        video_id = prepare_feature(video['id'])
        
        # Get duration from contentDetails
        duration = video.get('contentDetails', {}).get('duration', '')
        
        snippet = video['snippet']
        statistics = video['statistics']
        
        # Extract snippet features
        features = [prepare_feature(snippet.get(feature, "")) for feature in SNIPPET_FEATURES]
        
        # Special case features
        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", {}).get("default", {}).get("url", "")
        trending_date = time.strftime("%Y-%m-%d")
        tags = get_tags(snippet.get("tags", []))
        view_count = statistics.get("viewCount", 0)
        
        # Check for likes/dislikes
        if 'likeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics.get('dislikeCount', 0)
        else:
            ratings_disabled = True
            likes = 0
            dislikes = 0
        
        # Check for comment count
        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
        else:
            comments_disabled = True
            comment_count = 0
        
        # Compile all data into one line
        line = [video_id] + features + [
            prepare_feature(x) for x in [
                trending_date, tags, view_count, likes, dislikes,
                comment_count, thumbnail_link, comments_disabled,
                ratings_disabled, description, duration
            ]
        ]
        lines.append(",".join(line))
    
    return lines


def get_pages(country_code: str, api_key: str, category_id: str = None) -> List[str]:
    """Iterate through all pages of results for a country/category."""
    country_data = []
    next_page_token = '&'
    
    while next_page_token is not None:
        # Get page of video data
        video_data_page = api_request(next_page_token, country_code, api_key, category_id)
        
        if video_data_page is None:
            print(f"Failed to get data for {country_code}" + 
                  (f" category {category_id}" if category_id else "") + 
                  ", skipping...")
            break
        
        # Get next page token
        next_page_token = video_data_page.get("nextPageToken", None)
        next_page_token = f"pageToken={next_page_token}&" if next_page_token is not None else None
        
        # Extract video data
        items = video_data_page.get('items', [])
        country_data += get_videos(items)
    
    return country_data


def write_to_file(country_code: str, country_data: List[str], output_dir: str, category_id: str = None, category_name: str = None):
    """Write collected data to CSV file."""
    category_suffix = f"_{category_name.replace(' ', '_').replace('&', 'and')}" if category_name else ""
    
    print(f"Writing {country_code}" + 
          (f" {category_name}" if category_name else "") + 
          " data to file...")
    
    try:
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        filename = f"{output_dir}/{time.strftime('%Y-%m-%d')}_{country_code}{category_suffix}_videos.csv"
        
        with open(filename, "w+", encoding='utf-8') as file:
            for row in country_data:
                file.write(f"{row}\n")
        
        print(f"Successfully wrote {len(country_data) - 1} videos to {filename}")
    
    except Exception as e:
        print(f"Error writing file for {country_code}" + 
              (f" {category_name}" if category_name else "") + 
              f": {e}")


def get_data(country_codes: List[str], api_key: str, output_dir: str, collect_by_category: bool):
    """Main function to collect data for all country codes."""
    for country_code in country_codes:
        if collect_by_category:
            # Collect data for each category separately
            for category_id, category_name in CATEGORIES.items():
                print(f"\nCollecting {category_name} data for {country_code}...")
                country_data = [",".join(HEADER)] + get_pages(country_code, api_key, category_id)
                
                # Only write file if we got videos (more than just header)
                if len(country_data) > 1:
                    write_to_file(country_code, country_data, output_dir, category_id, category_name)
                else:
                    print(f"No videos found for {country_code} - {category_name}")
                
                # Small delay to avoid rate limiting
                time.sleep(0.5)
        else:
            # Collect all mostPopular videos without category filter
            print(f"\nCollecting data for {country_code}...")
            country_data = [",".join(HEADER)] + get_pages(country_code, api_key)
            write_to_file(country_code, country_data, output_dir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape YouTube trending video metrics')
    parser.add_argument(
        '--key_path',
        help='Path to the file containing the api key',
        default='api_key.txt'
    )
    parser.add_argument(
        '--country_code_path',
        help='Path to the file containing the list of country codes to scrape',
        default='country_codes.txt'
    )
    parser.add_argument(
        '--output_dir',
        help='Path to save the outputted files in',
        default='output/'
    )
    parser.add_argument(
        '--by_category',
        help='Collect videos separated by category',
        action='store_true',
        default=True
    )
    
    args = parser.parse_args()
    
    # Setup and run
    api_key, country_codes = setup(args.key_path, args.country_code_path)
    get_data(country_codes, api_key, args.output_dir, args.by_category)
    
    print("\nData collection complete!")
