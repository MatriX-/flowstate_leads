from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from typing import List, Optional, Set
from playwright.sync_api import sync_playwright, TimeoutError
import time
import random
from datetime import datetime
import json
from rich.console import Console
import urllib.parse
from maps_logger import setup_logger
from concurrent.futures import ThreadPoolExecutor
from itertools import cycle
import uuid

logger = setup_logger()
console = Console()
app = FastAPI(title="Maps Scraper API", version="1.0.0")

class SearchRequest(BaseModel):
    city: str
    state: str
    num_leads: int
    search_terms: List[str] = ["smoke shop", "vape shop", "tobacco shop"]
    existing_names: Optional[List[str]] = None
    existing_addresses: Optional[List[str]] = None

def get_search_url(term, city, state):
    """Generate direct Google Maps search URL for specific city/state"""
    # Encode the search term and location separately
    location = urllib.parse.quote(f"{city}, {state}")
    query = urllib.parse.quote(f"{term}")
    # Use a simpler URL format that's more reliable
    return f"https://www.google.com/maps/search/{query}+near+{location}"

def extract_business_info(page, listing):
    """Extract business information from a listing element"""
    try:
        # Add initial wait for stability
        time.sleep(0.5)
        
        name = ""
        name_el = listing.query_selector('.qBF1Pd')
        if name_el:
            name = name_el.inner_text()
            
            if 'cigar' in name.lower():
                return None
        
        address = ""
        phone = ""
        
        try:
            # Add more robust clicking
            listing.click(timeout=2000)
            time.sleep(1)  # Increased wait time
            
            # Try multiple selectors for address
            address_selectors = [
                'button[data-item-id="address"]',
                '[data-item-id*="address"]',
                '.rogA2c'
            ]
            
            for selector in address_selectors:
                try:
                    address_el = page.wait_for_selector(selector, timeout=2000, state='visible')
                    if address_el:
                        address = address_el.inner_text().strip()
                        break
                except:
                    continue
            
            # Try multiple selectors for phone
            phone_selectors = [
                'button[data-item-id*="phone"]',
                '[data-item-id*="phone"]',
                '.rogA2c span.UsdlK'
            ]
            
            for selector in phone_selectors:
                try:
                    phone_el = page.wait_for_selector(selector, timeout=2000, state='visible')
                    if phone_el:
                        phone = phone_el.inner_text().strip()
                        break
                except:
                    continue

        except Exception as e:
            logger.warning(f"Error clicking listing or extracting details: {str(e)}")
            # Fallback method remains the same...
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

        # Only return results if we have both a name and a phone number
        if name and phone:
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
    """Optimized version of loading more results"""
    try:
        initial_results = len(page.query_selector_all('.Nv2PK'))
        
        # Scroll the results panel
        page.evaluate("""
            const panel = document.querySelector('.DxyBCb');
            if (panel) {
                panel.scrollTo({
                    top: panel.scrollHeight,
                    behavior: 'smooth'
                });
            }
        """)
        
        # Wait briefly for new results to load
        time.sleep(0.3)
        
        new_results = len(page.query_selector_all('.Nv2PK'))
        return new_results > initial_results
        
    except Exception as e:
        logger.error(f"Error while scrolling: {str(e)}")
        return False

@app.post("/scrape")
def scrape_locations(request: SearchRequest):
    """Modified scraping endpoint with concurrent browser contexts"""
    logger.info(f"Starting scrape for {request.city}, {request.state}")
    
    all_results = []
    processed_addresses = set(request.existing_addresses or [])
    existing_names = set(request.existing_names or [])

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            logger.info("Browser launched successfully")
            
            # Create multiple browser contexts
            contexts = []
            pages = []
            
            # Only create contexts/pages until we have enough results
            for i, term in enumerate(request.search_terms):
                if len(all_results) >= request.num_leads:
                    break
                    
                context = browser.new_context(
                    viewport={'width': 1920, 'height': 1080},
                    user_agent=f'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/96.0.{i}.0'
                )
                contexts.append(context)
                
                page = context.new_page()
                pages.append(page)
                
                search_url = get_search_url(term, request.city, request.state)
                logger.info(f"Searching for {term} in {request.city}, {request.state}")
                page.goto(search_url, wait_until='networkidle')
                time.sleep(2)
                
                # Process results from this page immediately
                result_items = page.query_selector_all('.Nv2PK')
                if not result_items:
                    logger.warning(f"No results found for term: {term}")
                    continue

                for item in result_items:
                    if len(all_results) >= request.num_leads:
                        break
                        
                    info = extract_business_info(page, item)
                    if (info and info['address'] and 
                        info['address'] not in processed_addresses and 
                        info['name'] not in existing_names):
                        
                        processed_addresses.add(info['address'])
                        existing_names.add(info['name'])
                        info['search_term'] = term
                        all_results.append(info)
                        logger.info(f"Found business: {info['name']}")
                    
                    time.sleep(0.1)

                # Only try to load more if we still need results
                while len(all_results) < request.num_leads:
                    if not load_more_results(page):
                        break
                    
                    new_items = page.query_selector_all('.Nv2PK')
                    for item in new_items:
                        if len(all_results) >= request.num_leads:
                            break
                            
                        info = extract_business_info(page, item)
                        if (info and info['address'] and 
                            info['address'] not in processed_addresses and 
                            info['name'] not in existing_names):
                            
                            processed_addresses.add(info['address'])
                            existing_names.add(info['name'])
                            info['search_term'] = term
                            all_results.append(info)
                            logger.info(f"Found business: {info['name']}")
                        
                        time.sleep(0.1)

            # Cleanup
            for page in pages:
                page.close()
            for context in contexts:
                context.close()
            browser.close()

    except Exception as e:
        logger.error(f"Error during scraping: {str(e)}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))

    if not all_results:
        logger.warning("No results found for the search criteria")
        raise HTTPException(status_code=404, detail="No results found")

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

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = str(uuid.uuid4())
    logger.info(f"Request {request_id} started - {request.method} {request.url.path}")
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    logger.info(
        f"Request {request_id} completed - Duration: {duration:.2f}s, "
        f"Client: {request.client.host}"
    )
    return response

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
