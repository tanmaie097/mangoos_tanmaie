import requests
import json
import os
from dotenv import load_dotenv
from datetime import datetime
load_dotenv()

API_KEY = os.getenv("news_API_key")   # <-- Replace this with your NewsAPI key
KEYWORD = "AI"  

                  
URL = f"https://newsapi.org/v2/everything?q={KEYWORD}&apiKey={API_KEY}"
response = requests.get(URL)
data = response.json()


SAVE_FOLDER = "/Users/tanmaie/Desktop/magnoos/exercises/04_ETL/news_etl/raw"
#os.makedirs(SAVE_FOLDER, exist_ok=True)
today = datetime.now().strftime("%Y-%m-%d")
file_path = f"{SAVE_FOLDER}/raw_news_{today}.json"

with open(file_path, "w") as file:
    json.dump(data, file, indent=2)

print(f"Raw data saved to: {file_path}")

