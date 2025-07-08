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


def identify_category(text, keywords):
    for category, words in keywords.items():
        if text.lower() in [word.lower() for word in words]:
            return category
    return None


def handler(event=None, context=None):
    options = webdriver.ChromeOptions()
    service = webdriver.ChromeService("/opt/chromedriver")

    options.binary_location = '/opt/chrome/chrome'
    user_agent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_1) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.1 Safari/605.1.15"
    options.add_argument(f"--user-agent={user_agent}")
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

    # Open the URL
    try:
        page_num = 1
        event_list = []
        url = f"https://peatix.com/search?q={keyword}&country=SG&l.ll=1.3352%2C103.8529&l.text=Singapore&p={page_num}"
        driver.get(url)
        time.sleep(5)
        soup = BeautifulSoup(driver.page_source, "html5lib")
        web_elements = soup.find_all('li', class_="event-thumb")
        not_sponsored_content = []
        for i in range(len(web_elements)):
            if web_elements[i].find('a', class_='event-thumb_link top-cover') is None:
                not_sponsored_content.append(web_elements[i])
        print(len(not_sponsored_content))
        while len(not_sponsored_content) > 0:
            for web_element in not_sponsored_content:
                event_data = {
                    "Event Link": "None" if web_element.select_one('.event-thumb_link') is None else web_element.select_one('.event-thumb_link')['href'],
                    "Event Date": "None" if web_element.select_one('.event-cal') is None else web_element.select_one('.event-cal').text + f'-{str(datetime.now().year)}',
                    "Event Time": "None" if web_element.select_one('.datetime') is None else web_element.select_one('.datetime').text,
                    "Event Location": "None" if web_element.select_one('.event-thumb_location') is None else web_element.select_one('.event-thumb_location').text,
                    "Event Name": "None" if web_element.select_one('.event-thumb_name') is None else web_element.select_one('.event-thumb_name').text,
                    "Event Organizer": "None" if web_element.select_one('.event-thumb_organizer') is None else web_element.select_one('.event-thumb_organizer').text,
                    "Event Image URL": "None" if web_element.select_one('.event-thumb_cover') is None else web_element.select_one('.event-thumb_cover')['style'].split('"')[1],
                    "art_form": art_form,
                    "art_form_category": keyword.replace('%20', '_')
                }
                event_list.append(event_data)
            page_num += 1
            url = f"https://peatix.com/search?q={keyword}&country=SG&l.ll=1.3352%2C103.8529&l.text=Singapore&p={page_num}"
            driver.get(url)
            time.sleep(5)
            soup = BeautifulSoup(driver.page_source, "html5lib")
            web_elements = soup.find_all('li', class_="event-thumb")
            not_sponsored_content = []
            for i in range(len(web_elements)):
                if web_elements[i].find('a', class_='event-thumb_link top-cover') is None:
                    not_sponsored_content.append(web_elements[i])
        if len(event_list) > 0:
            s3 = boto3.client('s3')
            file_path = f"""peatix_{keyword}_{art_form}_{datetime.today().strftime('%Y-%m-%d')}.json"""
            bucket_name = "meetup-data-scraped"
            s3.put_object(Bucket=bucket_name,
                          Key=file_path, Body=json.dumps(event_list))
            print(f"Saved to {file_path}")
        driver.close()
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": len(event_list),
                }
            ),
        }
    except Exception as e:
        return {
            "statusCode": 200,
            "body": json.dumps(
                {
                    "message": str(e),
                }
            ),
        }
