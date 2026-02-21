"""
Simple verification script to check if the Streamlit dashboard is accessible
and report what should be displayed based on the code.
"""
import requests
import json
from datetime import datetime

def check_streamlit_health():
    """Check if Streamlit is running and accessible"""
    try:
        response = requests.get("http://localhost:8501", timeout=5)
        if response.status_code == 200:
            print("✅ Streamlit app is running and accessible")
            print(f"   Status Code: {response.status_code}")
            print(f"   Page Title: {response.text.split('<title>')[1].split('</title>')[0] if '<title>' in response.text else 'Not found'}")
            return True
        else:
            print(f"⚠️  Streamlit returned status code: {response.status_code}")
            return False
    except requests.exceptions.RequestException as e:
        print(f"❌ Could not connect to Streamlit: {e}")
        return False

def check_healthz():
    """Check Streamlit's health endpoint"""
    try:
        response = requests.get("http://localhost:8501/_stcore/health", timeout=5)
        if response.status_code == 200:
            print("✅ Streamlit health endpoint OK")
            return True
    except:
        print("⚠️  Streamlit health endpoint not accessible")
        return False

def expected_dashboard_elements():
    """Report what should be on the dashboard based on the code"""
    print("\n" + "="*70)
    print("📋 EXPECTED DASHBOARD ELEMENTS")
    print("="*70)
    
    elements = {
        "1. Page Title": "The State of Stack Overflow",
        "2. Caption": "Monthly activity data from 2008 to present · Built with Bruin + BigQuery + Streamlit",
        "3. Info Banner": "Data current through [latest month]. Sources: BigQuery Public Datasets + Stack Exchange API",
        "4. Metric Cards (4 columns)": [
            "Peak Month - [number] questions, [date]",
            "Latest Month - [number] questions, [date]",
            "Decline from Peak - [percentage]%",
            "Post-ChatGPT Avg - [number]/mo, [percentage]% vs pre-ChatGPT"
        ],
        "5. Monthly Questions Chart": {
            "Title": "Monthly Questions Asked",
            "Type": "Bar chart with rounded corners",
            "Features": [
                "Colored by era (Growth/Plateau/Post-ChatGPT)",
                "Vertical line at Nov 2022 (ChatGPT launch)",
                "Horizontal average line (pre-ChatGPT)",
                "Tooltip with month, questions, era"
            ],
            "Height": "380px"
        },
        "6. Tag Trends Chart": {
            "Title": "Which Communities Collapsed First?",
            "Type": "Multi-line chart (top 8 tags)",
            "Features": [
                "Normalized to peak (100%)",
                "Smoothed to quarterly averages",
                "Interactive legend selection",
                "8 different colors for tags",
                "ChatGPT reference line"
            ],
            "Height": "380px",
            "Data": "Through Sep 2022"
        },
        "7. Answer Desert Section": {
            "Title": "The Answer Desert",
            "Layout": "Two side-by-side charts",
            "Left": "Answer Rate (% answered)",
            "Right": "Answers per Question",
            "Type": "Line + circle markers",
            "Features": [
                "Quarterly averages",
                "Post-ChatGPT points highlighted in orange",
                "ChatGPT reference line"
            ],
            "Height": "340px each"
        },
        "8. Acceleration Chart": {
            "Title": "The Acceleration",
            "Type": "Bar chart (year-over-year change)",
            "Features": [
                "Shows only complete calendar years",
                "Post-ChatGPT bars highlighted in orange",
                "Zero reference line",
                "Rounded top corners"
            ],
            "Height": "340px"
        },
        "9. Footer": "Data: Google BigQuery Public Datasets + Stack Exchange API · Pipeline: Bruin · Database: BigQuery · Visualization: Streamlit + Altair"
    }
    
    for key, value in elements.items():
        print(f"\n{key}:")
        if isinstance(value, dict):
            for k, v in value.items():
                if isinstance(v, list):
                    print(f"  {k}:")
                    for item in v:
                        print(f"    - {item}")
                else:
                    print(f"  {k}: {v}")
        elif isinstance(value, list):
            for item in value:
                print(f"  - {item}")
        else:
            print(f"  {value}")

def color_scheme():
    """Report the color scheme"""
    print("\n" + "="*70)
    print("🎨 COLOR SCHEME")
    print("="*70)
    print("Highlight (Post-ChatGPT):  #D55E00 (orange)")
    print("Default (Pre-ChatGPT):     #56B4E9 (blue)")
    print("Secondary (Plateau era):   #E69F00 (amber)")
    print("Muted (reference lines):   #999999 (gray)")
    print("\nEra Colors:")
    print("  Growth (2008-2014):      Blue (#56B4E9)")
    print("  Plateau (2015-2022):     Amber (#E69F00)")
    print("  Post-ChatGPT (2023+):    Orange (#D55E00)")

def data_sources():
    """Report data sources"""
    print("\n" + "="*70)
    print("📊 DATA SOURCES")
    print("="*70)
    print("BigQuery Tables:")
    print("  1. bruin-playground-arsalan.staging.stackoverflow_monthly")
    print("     - Monthly aggregated question counts")
    print("     - Includes era classification")
    print("     - Answer rate and avg answer count")
    print("  2. bruin-playground-arsalan.staging.stackoverflow_tag_trends")
    print("     - Tag-level monthly data")
    print("     - Normalized to peak for each tag")
    print("     - Available through Sep 2022")

if __name__ == "__main__":
    print("🔍 Stack Overflow Dashboard Verification\n")
    print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Check if the app is running
    is_running = check_streamlit_health()
    check_healthz()
    
    # Show what should be on the dashboard
    expected_dashboard_elements()
    color_scheme()
    data_sources()
    
    print("\n" + "="*70)
    print("✨ NEXT STEPS")
    print("="*70)
    if is_running:
        print("The Streamlit app is running. Open http://localhost:8501 in your browser")
        print("and verify the following:")
        print("\n1. All 4 metric cards show actual numbers (not loading spinners)")
        print("2. The main bar chart renders with colored bars")
        print("3. The tag trends chart shows 8 different colored lines")
        print("4. The Answer Desert section shows 2 charts side-by-side")
        print("5. The Acceleration section shows a bar chart")
        print("6. No error messages or red text appear")
        print("7. All tooltips work when hovering over charts")
    else:
        print("⚠️  The Streamlit app is not accessible.")
        print("Make sure it's running with:")
        print("  cd stackoverflow-trends/assets/reports")
        print("  streamlit run streamlit_app.py")
    
    print("\n" + "="*70)
