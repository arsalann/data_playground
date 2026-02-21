#!/usr/bin/env python3
"""
Verify Stack Overflow dashboard by taking screenshots of all sections.
"""

import time
from pathlib import Path
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

def main():
    print("Starting dashboard verification...")
    
    # Setup Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = None
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.get("http://localhost:8501")
        
        # Wait for Streamlit to load
        print("Waiting for page to load...")
        time.sleep(5)
        
        # Take initial screenshot
        screenshots_dir = Path("dashboard_screenshots")
        screenshots_dir.mkdir(exist_ok=True)
        
        driver.save_screenshot(str(screenshots_dir / "01_top_of_page.png"))
        print("Screenshot 1: Top of page")
        
        # Check page title
        title = driver.title
        print(f"Page title: {title}")
        
        # Try to find key elements
        try:
            # Wait for Streamlit app to be ready
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "body"))
            )
            
            # Get page height for scrolling
            total_height = driver.execute_script("return document.body.scrollHeight")
            viewport_height = driver.execute_script("return window.innerHeight")
            
            print(f"Total page height: {total_height}px")
            print(f"Viewport height: {viewport_height}px")
            
            # Scroll and capture screenshots
            scroll_position = 0
            screenshot_num = 2
            
            while scroll_position < total_height:
                scroll_position += viewport_height - 100
                driver.execute_script(f"window.scrollTo(0, {scroll_position});")
                time.sleep(1)
                
                driver.save_screenshot(str(screenshots_dir / f"{screenshot_num:02d}_scroll_{scroll_position}.png"))
                print(f"Screenshot {screenshot_num}: Scrolled to {scroll_position}px")
                screenshot_num += 1
                
                if scroll_position >= total_height:
                    break
            
            # Scroll back to top
            driver.execute_script("window.scrollTo(0, 0);")
            time.sleep(1)
            
            # Try to extract text content
            body_text = driver.find_element(By.TAG_NAME, "body").text
            
            # Check for key elements
            checks = {
                "Title 'The State of Stack Overflow'": "The State of Stack Overflow" in body_text,
                "Contains 'Peak'": "Peak" in body_text,
                "Contains 'Latest'": "Latest" in body_text,
                "Contains 'Monthly Questions Asked'": "Monthly Questions Asked" in body_text,
                "Contains 'Answer Desert'": "Answer Desert" in body_text,
                "Contains 'Acceleration'": "Acceleration" in body_text,
                "Contains 'Bruin'": "Bruin" in body_text,
                "Contains 'BigQuery'": "BigQuery" in body_text,
                "Contains 'Streamlit'": "Streamlit" in body_text,
                "Contains 'Altair'": "Altair" in body_text,
            }
            
            print("\n" + "="*60)
            print("VERIFICATION RESULTS")
            print("="*60)
            
            for check_name, result in checks.items():
                status = "PASS" if result else "FAIL"
                print(f"{status} {check_name}: {result}")
            
            # Look for error messages
            if "error" in body_text.lower() or "exception" in body_text.lower():
                print("\nWARNING: Possible errors found in page content")
            
            # Save full page text
            with open(screenshots_dir / "page_content.txt", "w") as f:
                f.write(body_text)
            print(f"\nFull page text saved to {screenshots_dir / 'page_content.txt'}")
            
            # Try to find metric card values
            print("\n" + "="*60)
            print("METRIC CARD VALUES")
            print("="*60)
            
            # Look for numbers in the text
            lines = body_text.split('\n')
            for i, line in enumerate(lines):
                if 'Peak' in line or 'Latest' in line:
                    print(f"{line}")
                    if i + 1 < len(lines):
                        print(f"  {lines[i+1]}")
            
            print(f"\nAll screenshots saved to {screenshots_dir}/")
            
        except Exception as e:
            print(f"Error during verification: {e}")
            driver.save_screenshot(str(screenshots_dir / "error_state.png"))
        
    except Exception as e:
        print(f"Fatal error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if driver:
            driver.quit()
    
    print("\nVerification complete!")

if __name__ == "__main__":
    main()
