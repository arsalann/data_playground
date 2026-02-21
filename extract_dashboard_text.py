"""Extract all text content from the dashboard for analysis."""
import time
from pathlib import Path
from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=True)
    page = browser.new_page(viewport={"width": 1920, "height": 3000})
    
    print("Loading dashboard...")
    page.goto('http://localhost:8501', wait_until='networkidle')
    page.wait_for_timeout(8000)
    
    # Get all text
    body_text = page.evaluate("document.body.innerText")
    
    # Save it
    Path('dashboard_screenshots').mkdir(exist_ok=True)
    with open('dashboard_screenshots/full_text.txt', 'w') as f:
        f.write(body_text)
    
    # Print it
    print("="*80)
    print("DASHBOARD TEXT CONTENT")
    print("="*80)
    print(body_text)
    print("="*80)
    
    # Get scroll height
    scroll_info = page.evaluate("""() => {
        return {
            scrollHeight: document.body.scrollHeight,
            clientHeight: document.documentElement.clientHeight
        }
    }""")
    
    print(f"\nScroll height: {scroll_info['scrollHeight']}px")
    print(f"Client height: {scroll_info['clientHeight']}px")
    
    # Take full page screenshot with larger viewport
    page.screenshot(path='dashboard_screenshots/full_page_tall.png', full_page=True)
    print("\nFull page screenshot saved to dashboard_screenshots/full_page_tall.png")
    
    browser.close()
