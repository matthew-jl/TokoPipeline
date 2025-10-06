import json
import re
import time
from playwright.sync_api import sync_playwright, TimeoutError
from typing import List, Dict

def scrape_tokopedia_reviews(url: str) -> List[Dict]:
    """
    Scrapes the first page of product reviews from a Tokopedia review URL.

    Args:
        url: The full URL of the Tokopedia product review page.

    Returns:
        A list of dictionaries, where each dictionary represents a review.
    """
    
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

            page_number = 1

            while True:
                print(f"\n--- Scraping Page {page_number} ---")
                
                # Adjust css selector based on website
                page.wait_for_selector('article.css-15m2bcr', timeout=30000)
                review_containers = page.query_selector_all('article.css-15m2bcr')
                print(f"Found {len(review_containers)} review containers on this page.")
                
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
                page.wait_for_load_state('networkidle', timeout=10000)
                
                page_number += 1
                time.sleep(1) # to avoid overwhelming server

        except TimeoutError:
            print("A timeout occurred while waiting for new content. Assuming end of pages.")

        except Exception as e:
            print(f"An error occurred: {e}")
        
        finally:
            browser.close()
            
    return all_reviews_data

if __name__ == "__main__":
    # (REPLACE THIS WITH DESIRED PRODUCT REVIEW PAGE. Make sure it ends with "/review" for Tokopedia)
    target_url = "https://www.tokopedia.com/project1945/project-1945-x-cj-petruk-perfume-edp-parfum-unisex-100ml-1730927312240412298/review"
    
    scraped_reviews = scrape_tokopedia_reviews(target_url)
    
    if scraped_reviews:
        output_filename = "reviews.json"
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(scraped_reviews, f, indent=4, ensure_ascii=False)
        
        print(f"\nSuccessfully scraped a total of {len(scraped_reviews)} unique reviews.")
        print(f"Data saved to {output_filename}")
    else:
        print("\nNo reviews were scraped. An empty file was not created.")