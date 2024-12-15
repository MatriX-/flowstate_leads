from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from typing import List
from playwright.sync_api import sync_playwright, TimeoutError
import time
import random
from datetime import datetime
import json
from rich.console import Console
import urllib.parse
from maps_logger import setup_logger

logger = setup_logger()
console = Console()
app = FastAPI(title="Maps Scraper API", version="1.0.0")

class SearchRequest(BaseModel):
    city: str
    state: str
    num_leads: int
    search_terms: List[str] = ["smoke shop", "vape shop", "tobacco shop"]

def get_search_url(term, city, state):
    """Generate direct Google Maps search URL for specific city/state"""
    # Encode the exact location we want to search in
    location = urllib.parse.quote(f"{city}, {state}")
    # Encode the search term
    query = urllib.parse.quote(f"{term}")
    # Use 'in' syntax to force Google to search within the specified city
    return f"https://www.google.com/maps/search/{query}+in+{location}"

def extract_business_info(page, listing):
    """Extract business information from a listing element"""
    try:
        name = ""
        name_el = listing.query_selector('.qBF1Pd')
        if name_el:
            name = name_el.inner_text()
            
            # Skip if business name contains "cigar" (case insensitive)
            if 'cigar' in name.lower():
                return None
        
        address = ""
        phone = ""
        
        # Click the listing to open details panel
        try:
            listing.click()
            time.sleep(random.uniform(0.5, 1))
            
            # Look for the address in the details panel
            address_button = page.query_selector('button[data-item-id="address"]')
            if address_button:
                address = address_button.inner_text().strip()
            
            # Look for phone in details panel
            phone_button = page.query_selector('button[data-item-id*="phone"]')
            if phone_button:
                phone = phone_button.inner_text().strip()
                
        except Exception as e:
            logger.warning(f"Error clicking listing or extracting details: {str(e)}")
            # Fallback to original method if clicking fails
            info_containers = listing.query_selector_all('.W4Efsd')
            for container in info_containers:
                container_text = container.inner_text()
                
                if not address and ('Tobacco shop' in container_text or 'Smoke shop' in container_text or 'Vaporizer store' in container_text):
                    spans = container.query_selector_all('span')
                    for span in spans:
                        span_text = span.inner_text().strip()
                        if any(char.isdigit() for char in span_text) and not span_text.startswith('('):
                            address = span_text.replace('·', '').strip()
                            break
                
                if not phone and ('Open' in container_text or 'Closes' in container_text):
                    phone_el = container.query_selector('span.UsdlK')
                    if phone_el:
                        phone = phone_el.inner_text().strip()

        if name:
            result = {
                'name': name,
                'address': address,
                'phone': phone,
                'website': ''
            }
            return result
            
        return None

    except Exception as e:
        logger.error(f"Error extracting business info: {str(e)}")
        return None

def load_more_results(page):
    """Attempt to load more results by scrolling the results panel"""
    try:
        initial_results = len(page.query_selector_all('.Nv2PK'))
        
        # Make sure the panel is focused
        panel = page.query_selector('.DxyBCb')
        if panel:
            panel.click()
        
        # Scroll in larger increments
        last_results_count = initial_results
        max_attempts = 10
        
        for _ in range(max_attempts):
            # Scroll multiple times quickly
            for _ in range(5):
                page.keyboard.press('PageDown')
                time.sleep(random.uniform(0.2, 0.3))
            
            new_results = len(page.query_selector_all('.Nv2PK'))
            if new_results <= last_results_count:
                page.keyboard.press('End')
                time.sleep(random.uniform(0.3, 0.5))
                final_check = len(page.query_selector_all('.Nv2PK'))
                return final_check > initial_results
            
            last_results_count = new_results
        
        return True
        
    except Exception as e:
        logger.error(f"Error while scrolling: {str(e)}")
        return False

@app.post("/scrape")
def scrape_locations(request: SearchRequest):
    """
    API endpoint to scrape business locations based on search criteria
    """
    logger.info(f"Received scraping request for {request.city}, {request.state}")
    logger.info(f"Search terms: {request.search_terms}")
    logger.info(f"Requested leads: {request.num_leads}")
    
    all_results = []
    processed_addresses = set()

    try:
        with sync_playwright() as p:
            logger.info("Launching browser")
            browser = p.chromium.launch(headless=True)
            context = browser.new_context(viewport={'width': 1920, 'height': 1080})
            page = context.new_page()
            
            for term in request.search_terms:
                if len(all_results) >= request.num_leads:
                    logger.info("Reached desired number of leads")
                    break
                    
                logger.info(f"Searching for term: {term}")
                search_url = get_search_url(term, request.city, request.state)
                logger.debug(f"Search URL: {search_url}")
                
                page.goto(search_url)
                time.sleep(random.uniform(1, 1.5))

                while len(all_results) < request.num_leads:
                    try:
                        page.wait_for_selector('.Nv2PK', timeout=15000)
                        result_items = page.query_selector_all('.Nv2PK')
                        
                        if not result_items:
                            logger.warning("No results found for current search")
                            break

                        logger.info(f"Found {len(result_items)} results on current page")
                        for item in result_items:
                            if len(all_results) >= request.num_leads:
                                break
                                
                            info = extract_business_info(page, item)
                            if info and info['address'] and info['address'] not in processed_addresses:
                                processed_addresses.add(info['address'])
                                info['search_term'] = term
                                all_results.append(info)
                                logger.info(f"Added business: {info['name']}")
                            
                            time.sleep(random.uniform(0.2, 0.3))

                        if len(all_results) < request.num_leads:
                            if not load_more_results(page):
                                logger.info("No more results to load")
                                break

                    except TimeoutError:
                        logger.warning(f"Timeout while loading results for term: {term}")
                        break

            logger.info("Closing browser")
            browser.close()

    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    if not all_results:
        logger.warning("No results found for the search criteria")
        raise HTTPException(status_code=404, detail="No results found")

    logger.info(f"Successfully found {len(all_results)} leads")
    return {
        "status": "success",
        "location": {
            "city": request.city,
            "state": request.state
        },
        "total_leads": len(all_results),
        "results": all_results
    }

@app.get("/health")
def health_check():
    logger.debug("Health check requested")
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "maps_api:app",
        host="0.0.0.0",
        port=8000,
        reload=False,
        log_level="info",
        workers=1
    ) 
