"""
Quick script to verify the Stack Overflow dashboard renders correctly.
Takes screenshots and checks for key elements.
"""
import time
from pathlib import Path

try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.chrome.options import Options
    DRIVER = "selenium"
except ImportError:
    print("Selenium not available, trying playwright...")
    try:
        from playwright.sync_api import sync_playwright
        DRIVER = "playwright"
    except ImportError:
        print("Neither selenium nor playwright available. Install one:")
        print("  pip install selenium")
        print("  pip install playwright && playwright install chromium")
        exit(1)


def verify_with_selenium():
    """Verify dashboard using Selenium"""
    options = Options()
    options.add_argument('--headless=new')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-gpu')
    
    driver = webdriver.Chrome(options=options)
    
    try:
        print("📱 Navigating to http://localhost:8501...")
        driver.get('http://localhost:8501')
        
        # Wait for Streamlit to load
        time.sleep(5)
        
        # Take initial screenshot
        screenshots_dir = Path('dashboard_screenshots')
        screenshots_dir.mkdir(exist_ok=True)
        
        driver.save_screenshot(str(screenshots_dir / '01_top.png'))
        print("✅ Screenshot 1: Top of page")
        
        # Check title
        title = driver.title
        print(f"📄 Page title: {title}")
        
        # Check for key elements
        checks = {
            "Title contains 'Stack Overflow'": "Stack Overflow" in driver.page_source,
            "Metric cards present": "peak" in driver.page_source.lower(),
            "Monthly Questions chart": "Monthly Questions" in driver.page_source,
            "Tag trends": "tag" in driver.page_source.lower(),
            "Answer Desert": "Answer Desert" in driver.page_source or "answer" in driver.page_source.lower(),
            "Acceleration": "Acceleration" in driver.page_source or "acceleration" in driver.page_source.lower(),
            "Footer mentions": "Bruin" in driver.page_source or "BigQuery" in driver.page_source,
        }
        
        print("\n🔍 Element checks:")
        for check, result in checks.items():
            status = "✅" if result else "❌"
            print(f"{status} {check}")
        
        # Scroll and take more screenshots
        total_height = driver.execute_script("return document.body.scrollHeight")
        viewport_height = driver.execute_script("return window.innerHeight")
        
        scroll_positions = [0]
        current_pos = viewport_height
        while current_pos < total_height:
            scroll_positions.append(current_pos)
            current_pos += viewport_height
        
        for i, pos in enumerate(scroll_positions[1:], start=2):
            driver.execute_script(f"window.scrollTo(0, {pos});")
            time.sleep(1)
            driver.save_screenshot(str(screenshots_dir / f'{i:02d}_scroll_{pos}.png'))
            print(f"✅ Screenshot {i}: Scrolled to {pos}px")
        
        # Scroll to bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(1)
        driver.save_screenshot(str(screenshots_dir / f'{len(scroll_positions)+1:02d}_bottom.png'))
        print(f"✅ Screenshot {len(scroll_positions)+1}: Bottom of page")
        
        print(f"\n📸 All screenshots saved to {screenshots_dir}/")
        
        # Check for any visible error messages
        error_indicators = ["error", "exception", "traceback", "failed"]
        errors_found = []
        page_text = driver.page_source.lower()
        for indicator in error_indicators:
            if indicator in page_text and "error" not in driver.title.lower():
                errors_found.append(indicator)
        
        if errors_found:
            print(f"\n⚠️  Possible errors detected: {', '.join(errors_found)}")
        else:
            print("\n✅ No obvious errors detected")
        
    finally:
        driver.quit()


def verify_with_playwright():
    """Verify dashboard using Playwright"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1920, "height": 1080})
        
        try:
            print("📱 Navigating to http://localhost:8501...")
            page.goto('http://localhost:8501', wait_until='networkidle')
            
            # Wait a bit for Streamlit to fully render
            page.wait_for_timeout(8000)
            
            # Create screenshots directory
            screenshots_dir = Path('dashboard_screenshots')
            screenshots_dir.mkdir(exist_ok=True)
            
            # Take initial screenshot
            page.screenshot(path=str(screenshots_dir / '01_top.png'), full_page=False)
            print("✅ Screenshot 1: Top of page")
            
            # Check title
            title = page.title()
            print(f"📄 Page title: {title}")
            
            # Get page content
            content = page.content()
            body_text = page.evaluate("document.body.innerText")
            
            # Check for key elements
            checks = {
                "Title contains 'Stack Overflow'": "Stack Overflow" in content,
                "Metric cards present": "peak" in content.lower(),
                "Monthly Questions chart": "Monthly Questions" in content,
                "Tag trends": "tag" in content.lower(),
                "Answer Desert": "Answer Desert" in content or "answer" in content.lower(),
                "Acceleration": "Acceleration" in content or "acceleration" in content.lower(),
                "Footer mentions": "Bruin" in content or "BigQuery" in content,
            }
            
            print("\n🔍 Element checks:")
            for check, result in checks.items():
                status = "✅" if result else "❌"
                print(f"{status} {check}")
            
            # Get actual scroll dimensions
            scroll_info = page.evaluate("""() => {
                return {
                    scrollHeight: document.body.scrollHeight,
                    clientHeight: document.documentElement.clientHeight,
                    offsetHeight: document.body.offsetHeight
                }
            }""")
            
            print(f"\n📏 Page dimensions:")
            print(f"   scrollHeight: {scroll_info['scrollHeight']}px")
            print(f"   clientHeight: {scroll_info['clientHeight']}px")
            print(f"   offsetHeight: {scroll_info['offsetHeight']}px")
            
            # Scroll incrementally and take screenshots
            scroll_height = max(scroll_info['scrollHeight'], scroll_info['offsetHeight'])
            viewport_height = scroll_info['clientHeight']
            
            current_pos = 0
            screenshot_num = 2
            step = viewport_height - 100  # Overlap slightly
            
            print(f"\n📸 Taking scrolling screenshots (step={step}px):")
            
            while current_pos < scroll_height - viewport_height:
                current_pos += step
                page.evaluate(f"window.scrollTo(0, {current_pos})")
                page.wait_for_timeout(1500)
                page.screenshot(path=str(screenshots_dir / f'{screenshot_num:02d}_scroll_{current_pos}.png'), full_page=False)
                print(f"✅ Screenshot {screenshot_num}: Scrolled to {current_pos}px")
                screenshot_num += 1
            
            # Final bottom screenshot
            page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            page.wait_for_timeout(1500)
            page.screenshot(path=str(screenshots_dir / f'{screenshot_num:02d}_bottom.png'), full_page=False)
            print(f"✅ Screenshot {screenshot_num}: Bottom of page")
            
            # Take a full page screenshot
            page.screenshot(path=str(screenshots_dir / 'full_page.png'), full_page=True)
            print(f"✅ Full page screenshot saved")
            
            print(f"\n📸 All screenshots saved to {screenshots_dir}/")
            
            # Extract metric card values
            print("\n" + "="*60)
            print("METRIC CARD VALUES")
            print("="*60)
            lines = body_text.split('\n')
            for i, line in enumerate(lines[:50]):  # Check first 50 lines
                print(f"{i}: {line}")
            
            # Save full text
            with open(screenshots_dir / 'page_text.txt', 'w') as f:
                f.write(body_text)
            print(f"\n✅ Full page text saved to {screenshots_dir}/page_text.txt")
            
            # Check for errors
            error_indicators = ["error", "exception", "traceback", "failed"]
            errors_found = []
            page_text = content.lower()
            for indicator in error_indicators:
                if indicator in page_text and "error" not in title.lower():
                    errors_found.append(indicator)
            
            if errors_found:
                print(f"\n⚠️  Possible errors detected: {', '.join(errors_found)}")
            else:
                print("\n✅ No obvious errors detected")
            
        finally:
            browser.close()


if __name__ == "__main__":
    print(f"🚀 Using {DRIVER} to verify dashboard\n")
    if DRIVER == "selenium":
        verify_with_selenium()
    else:
        verify_with_playwright()
