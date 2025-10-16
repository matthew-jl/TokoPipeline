import json
import re
import time
import sys
from playwright.sync_api import sync_playwright, TimeoutError
from typing import List, Dict
from datetime import datetime
import boto3

def upload_to_s3(bucket_name: str, data: List[Dict], product_name: str ) -> None:
    s3_client = boto3.client('s3')
    json_data = json.dumps(data, indent=4, ensure_ascii=False)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_name = f"raw-reviews/{product_name}.json"
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=file_name,
            Body=json_data.encode('utf-8')
        )
        print(f"\nSuccessfully uploaded data to s3://{bucket_name}/{file_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

def scrape_tokopedia_reviews(url: str) -> List[Dict]:
    all_reviews_data = []
    scraped_review_texts = set()

    # sync_playwright() -> context manager
    with sync_playwright() as p:
        # headless=False -> see the browser UI
        # headless=True -> automation
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        
        try:
            print(f"Navigating to {url}...")
            page.goto(url, wait_until="domcontentloaded", timeout=60000) # Adjust timeout if connection is slow
            print("Initial page loaded.")

            print("Checking for initial page overlay/modal...")
            try:
                close_button_selector = "div.css-11hzwo5 button"
                
                page.wait_for_selector(close_button_selector, timeout=5000)
                
                print("Overlay found. Clicking close button...")
                page.click(close_button_selector)
                
                page.wait_for_selector(close_button_selector, state="detached", timeout=5000)
                print("Overlay closed successfully.")
            except TimeoutError:
                print("No overlay found. Continuing to scrape.")
            except Exception as e:
                print(f"An error occurred while trying to close the overlay: {e}")

            page_number = 1

            while True:
                print(f"\n--- Scraping Page {page_number} ---")
                
                # Adjust css selector based on website
                page.wait_for_selector('article.css-15m2bcr', timeout=30000)
                review_containers = page.query_selector_all('article.css-15m2bcr')
                print(f"Found {len(review_containers)} review containers on this page.")
                
                # Expand reviews by clicking on "Selengkapnya" button
                show_more_buttons = page.get_by_text("Selengkapnya").all() # Use this because "Tutup Ulasan" has the same selector
                # show_more_buttons = page.get_by_role("button", name="Selengkapnya")

                if len(show_more_buttons) > 0:
                    for button in show_more_buttons:
                        try:
                            button.click(timeout=1000)
                        except Exception:
                            pass 
                    
                    print(f"Clicked {len(show_more_buttons)} 'Selengkapnya' buttons. Waiting for content to load...")
                    time.sleep(1) 
                else:
                    print("No 'Selengkapnya' buttons found on this page.")

                new_reviews_found = 0
                for container in review_containers:
                    review_text_element = container.query_selector('p[data-unify="Typography"] span[data-testid="lblItemUlasan"]')
                    star_rating_element = container.query_selector('div[data-testid="icnStarRating"]')
                    
                    if star_rating_element and review_text_element:
                        review_text = review_text_element.inner_text().strip()
                        
                        # Only process if the review is new
                        if review_text not in scraped_review_texts:
                            scraped_review_texts.add(review_text)
                            
                            # Example aria-label: "bintang 5"
                            aria_label = star_rating_element.get_attribute("aria-label") or ""
                            # get "5" from "bintang 5"
                            star_rating_match = re.search(r'\d+', aria_label)
                            star_rating = int(star_rating_match.group(0)) if star_rating_match else None
                            
                            all_reviews_data.append({
                                "star_rating": star_rating,
                                "review_text": review_text
                            })
                            new_reviews_found += 1
                
                print(f"Scraped {new_reviews_found} new reviews. Total unique reviews: {len(all_reviews_data)}")

                # 'Next Page' button
                next_button = page.query_selector('button[aria-label="Laman berikutnya"]')
                
                if not next_button or next_button.is_disabled():
                    print("\n'Next Page' button is disabled or not found. Scraping complete.")
                    break
                
                print("Clicking 'Next Page' button...")
                next_button.click()
                
                print("Waiting for new content to load...")
                # page.wait_for_load_state('networkidle', timeout=20000)
                
                page_number += 1
                time.sleep(2) # to avoid overwhelming server

        except TimeoutError:
            print("A timeout occurred while waiting for new content. Assuming end of pages.")

        except Exception as e:
            print(f"An error occurred: {e}")
        
        finally:
            browser.close()
            
    return all_reviews_data

def extract_product_name(url: str) -> str:
    try:
        # Example: .../product-name-sku/review -> product-name-sku
        path_segments = url.strip('/').split('/')
        product_segment = path_segments[-2]
        return product_segment
    except IndexError:
        print("Warning: Could not extract product name from URL. Using a generic name.")
        return f"unknown-product-{datetime.now().strftime('%Y%m%d%H%M%S')}"

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Error: No URL provided.", file=sys.stderr)
        print("Usage: python scraper.py <tokopedia_review_url>", file=sys.stderr)
        sys.exit(1)
    target_url = sys.argv[1]

    S3_BUCKET_NAME = "tokopedia-reviews-matthewjl"

    product_name = extract_product_name(target_url)
    print(f"Extracted product name: {product_name}")
    
    scraped_reviews = scrape_tokopedia_reviews(target_url)
    
    if scraped_reviews:
        print(f"\nSuccessfully scraped a total of {len(scraped_reviews)} unique reviews.")

        # Save to local file (comment out)
        # output_filename = f"reviews-{product_name}.json"
        # with open(output_filename, 'w', encoding='utf-8') as f:
        #     json.dump(scraped_reviews, f, indent=4, ensure_ascii=False)
        # print(f"Data saved to {output_filename}")

        # Upload to S3
        upload_to_s3(S3_BUCKET_NAME, scraped_reviews, product_name)
    else:
        print("\nNo reviews were scraped. An empty file was not created.")