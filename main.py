import requests
import bs4
import pandas as pd
import time
import random
import boto3
#import os
import io
from concurrent.futures import ThreadPoolExecutor, as_completed

#from dotenv import find_dotenv, load_dotenv
#load_dotenv(find_dotenv())


# -------------------------------
# CONFIGURATION
# -------------------------------

URL = "https://www.topjobs.lk/do.landing?CO=FA&FA=SDQ&SV=y&jst=OPEN&SV=y"

HEADERS = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) '
                  'AppleWebKit/537.36 (KHTML, like Gecko) '
                  'Chrome/140.0.0.0 Safari/537.36',
    'Accept-Language': 'en-US,en;q=0.5'
}

MAX_WORKERS = 10  # Number of threads for parallel scraping
BUCKET_NAME = "webscraping-s3-7483-8939-6719"  # <-- your S3 bucket name

#Create S3 client
# s3 = boto3.client(
#     "s3",
#     aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
#     aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
# )

s3 = boto3.client("s3")

# -------------------------------
# HELPER FUNCTIONS
# -------------------------------

def get_job_title(soup):
    """Extract job title."""
    job_title = soup.select_one("#position")
    return job_title.text.strip() if job_title else ""


def get_employer(soup):
    """Extract employer name."""
    employer = soup.select_one("#employer")
    return employer.text.strip() if employer else ""


def get_closing_date(soup):
    """Extract closing date."""
    closing = soup.select_one("span#endDate")
    return closing.text.strip() if closing else ""


def fetch_job_details(job_url):
    """Fetch and parse a single job page."""
    try:
        # Optional: small random delay to avoid hitting site too fast
        time.sleep(random.uniform(0.5, 2.0))

        resp = requests.get(job_url, headers=HEADERS, timeout=10)
        resp.raise_for_status()
        soup = bs4.BeautifulSoup(resp.content, "html.parser")

        return {
            "job_title": get_job_title(soup),
            "employer": get_employer(soup),
            "closing_date": get_closing_date(soup),
            "web_link": job_url
        }

    except requests.RequestException as e:
        print(f"⚠️ Failed to fetch {job_url} — {e}")
        return {
            "job_title": "",
            "employer": "",
            "closing_date": "",
            "web_link": job_url
        }


# -------------------------------
# MAIN SCRAPER LOGIC
# -------------------------------

def data_extraction():
    print("🔍 Fetching job list...")
    main_page = requests.get(URL, headers=HEADERS)
    main_page.raise_for_status()

    soup = bs4.BeautifulSoup(main_page.content, "html.parser")
    links = soup.find_all("a", attrs={'class': 'openAd job-link job-title l-split'})

    joblist = ["https://www.topjobs.lk" + link.get('href') for link in links]

    print(f"✅ Found {len(joblist)} job links. Starting scrape...")

    job_details = []

    # Parallel fetching for speed
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(fetch_job_details, url) for url in joblist]
        for future in as_completed(futures):
            job_details.append(future.result())

    return job_details


def data_transformation(job_data):
    
    df = pd.DataFrame(job_data)
    df['scraped_date'] = pd.Timestamp.now()

    # Remove empty entries if any
    transformed_df = df[df['job_title'] != ""]

    return transformed_df

def data_loading(df):
    # Write to in-memory CSV (Lambda doesn’t allow local writes)
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    filename = f"topjobs_data_software_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv"

    

      # Upload to S3
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=filename,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )

    print(f"✅ Uploaded {len(df)} records to s3://{BUCKET_NAME}/{filename}")

def job_handler():
    scraped_data = data_extraction()
    transformed_data = data_transformation(scraped_data)
    data_loading(transformed_data)
    


def lambda_handler(event=None, context=None):
    print("🚀 Lambda execution started...")
    try:
        job_handler()
        print("🎯 Scraping job completed successfully.")
        return {
            "statusCode": 200,
            "body": "Scraping completed successfully."
        }
    except Exception as e:
        print(f"❌ Error occurred: {str(e)}")
        return {
            "statusCode": 500,
            "body": f"Error: {str(e)}"
        }

# if __name__ == "__main__":
#   lambda_handler()