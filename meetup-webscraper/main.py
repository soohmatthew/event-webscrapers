from datetime import datetime
from tempfile import mkdtemp
import time
import boto3
import json
import os.path
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
import os
import sys
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from bs4 import BeautifulSoup
import pandas as pd
from pandas import json_normalize


def identify_category(text, keywords):
    for category, words in keywords.items():
        if text.lower() in [word.lower() for word in words]:
            return category
    return None


def handler(event=None, context=None):
    options = webdriver.ChromeOptions()
    service = webdriver.ChromeService("/opt/chromedriver")

    options.binary_location = '/opt/chrome/chrome'
    options.add_argument("--headless=new")
    options.add_argument('--no-sandbox')
    options.add_argument("--disable-gpu")
    options.add_argument("--window-size=1280x1696")
    options.add_argument("--single-process")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--disable-dev-tools")
    options.add_argument("--no-zygote")
    options.add_argument(f"--user-data-dir={mkdtemp()}")
    options.add_argument(f"--data-path={mkdtemp()}")
    options.add_argument(f"--disk-cache-dir={mkdtemp()}")
    options.add_argument("--remote-debugging-port=9222")

    driver = webdriver.Chrome(options=options, service=service)
    keywords = {'visual_arts': ['paint', 'curator', 'sculpture',
                                'ceramic', 'printmaking', 'silk%20screen',
                                'calligraphy', 'digital%20art', 'fine%20arts',
                                'art%20auction', '3d%20art', 'new%20media',
                                'graffiti', 'mural', 'photograph'],
                'literary_arts': ['poems', 'poetry', 'literature', 'fiction', 'anthology',
                                  'novel', 'storytelling'],
                'craft': ['flower%20arrangement', 'needle-craft', 'pottery',
                          'terrarium', 'weaving', 'handmade', 'embroidery'],
                'film': ['films', 'movie', 'screening', 'art%20house%20film',
                         'film%20festival'],
                'heritage': ['cultural%20tour', 'cultural%20trial', 'historic%20district',
                             'museum%20tour']}

    keyword = event["art_form"]
    art_form = identify_category(keyword, keywords)

    url = f"https://www.meetup.com/find/?keywords={keyword}&source=EVENTS&location=sg--Singapore"

    # Open the URL
    driver.get(url)

    # Wait for the page to load (you can adjust the time based on your needs)
    time.sleep(2)

    # Get initial page height
    last_height = driver.execute_script(
        "return document.body.scrollHeight")

    while True:
        # Scroll down to the bottom
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")

        # Wait for a short moment to allow the content to load
        time.sleep(5)

        # Calculate new page height and compare with the last height
        new_height = driver.execute_script(
            "return document.body.scrollHeight")
        if new_height == last_height:
            # If the page height remains the same, we've reached the bottom
            break

        # Update the last height for the next iteration
        last_height = new_height

    # Now you can start scraping the content using Selenium or any other scraping library

    # For example, let's print the titles of the events
    soup = BeautifulSoup(driver.page_source, 'html.parser')
    try:
        web_info = json.loads(soup.find_all(
            'script', type='application/ld+json')[1].text)
        df = json_normalize(web_info)
        if df.shape[0] > 0:
            df['art_form'] = art_form
            df['art_form_category'] = keyword.replace('%20', '_')
            s3 = boto3.client('s3')
            file_path = f"""meetup_{keyword}_{art_form}_{datetime.today().strftime('%Y-%m-%d')}.json"""
            result = df.to_json(orient="records")
            s3 = boto3.client('s3')
            bucket_name = "meetup-data-scraped"
            s3.put_object(Bucket=bucket_name,
                          Key=file_path, Body=json.dumps(result))
            print(f"Saved to {file_path}")
    except Exception as e:
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": str(e),
                }
            ),
        }

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": df.shape,
            }
        ),
    }
