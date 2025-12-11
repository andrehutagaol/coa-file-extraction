"""
COA File Extraction - Automation Module
Automates downloading COA files from Smartsheet and uploading data to Deacom ERP.
Uses Selenium for browser automation.
"""

import os
import time
import math
import pandas as pd

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select

# =============================================================================
# CONFIGURATION - Replace with your own values
# =============================================================================
# Smartsheet credentials
SMARTSHEET_EMAIL = "EMAIL"
SMARTSHEET_PASSWORD = "PASSWORD"
SMARTSHEET_URL = "https://app.smartsheet.com/sheets/SHEET_ID"

# Deacom credentials
DEACOM_USERNAME = "USERNAME"
DEACOM_PASSWORD = "PASSWORD"
DEACOM_URL = "URL/"

# Chrome driver path (optional - leave empty to use default)
CHROME_DRIVER_PATH = ""  # e.g., r"C:\Users\user\chromedriver.exe"

# File paths
DOWNLOAD_DIR = r"Path\Downloads"
UPLOAD_FILE = r"Path\rm_clean_filtered.xlsx"


# =============================================================================
# BROWSER UTILITIES
# =============================================================================
def get_chrome_driver(download_dir: str = None, driver_path: str = None) -> webdriver.Chrome:
    """
    Initialize Chrome WebDriver with optional download directory.

    Args:
        download_dir: Directory for downloads
        driver_path: Path to chromedriver executable

    Returns:
        Chrome WebDriver instance
    """
    options = Options()
    options.add_argument("--start-maximized")

    if download_dir:
        os.makedirs(download_dir, exist_ok=True)
        options.add_experimental_option("prefs", {
            "download.default_directory": download_dir,
            "download.prompt_for_download": False,
            "plugins.always_open_pdf_externally": True,
            "download.directory_upgrade": True
        })

    if driver_path:
        service = Service(driver_path)
        return webdriver.Chrome(service=service, options=options)
    else:
        return webdriver.Chrome(options=options)


# =============================================================================
# SMARTSHEET AUTOMATION
# =============================================================================
class SmartsheetAutomation:
    """
    Automates downloading COA files from Smartsheet.
    """

    def __init__(self, email: str, password: str, sheet_url: str):
        """
        Initialize Smartsheet automation.

        Args:
            email: Smartsheet login email
            password: Smartsheet login password
            sheet_url: URL of the Smartsheet to access
        """
        self.email = email
        self.password = password
        self.sheet_url = sheet_url
        self.driver = None
        self.wait = None

    def login(self):
        """Login to Smartsheet."""
        self.driver = get_chrome_driver()
        self.wait = WebDriverWait(self.driver, 20)

        # Navigate to login page
        self.driver.get("https://app.smartsheet.com/b/login")

        # Enter email
        email_input = self.wait.until(
            EC.presence_of_element_located((By.ID, "loginEmail"))
        )
        email_input.send_keys(self.email)

        # Click Continue
        continue_button = self.wait.until(
            EC.element_to_be_clickable((By.ID, "formControl"))
        )
        continue_button.click()

        # Click "Sign in with email and password"
        sign_in_button = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable((
                By.XPATH, "//button[contains(text(), 'Sign in with email and password')]"
            ))
        )
        sign_in_button.click()

        # Enter password
        password_input = WebDriverWait(self.driver, 15).until(
            EC.presence_of_element_located((By.ID, "loginPassword"))
        )
        password_input.clear()
        password_input.send_keys(self.password)

        # Submit login
        sign_in_submit = WebDriverWait(self.driver, 15).until(
            EC.element_to_be_clickable((By.ID, "formControl"))
        )
        sign_in_submit.click()

        time.sleep(5)  # Wait for login to complete
        print("Logged in to Smartsheet")

    def navigate_to_sheet(self):
        """Navigate to the target sheet."""
        self.driver.get(self.sheet_url)
        time.sleep(3)

    def apply_filter(self, filter_name: str = "L3M Data"):
        """
        Apply a saved filter to the sheet.

        Args:
            filter_name: Name of the filter to apply
        """
        # Click filter dropdown
        arrow_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, 'button[data-client-id="tlb-12"]'))
        )
        self.driver.execute_script("arguments[0].click();", arrow_button)

        # Select filter
        filter_li = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((
                By.XPATH,
                f'//li[@data-client-id="fmn-2" and .//div[@class="label" and text()="{filter_name}"]]'
            ))
        )
        self.driver.execute_script("arguments[0].click();", filter_li)
        time.sleep(2)
        print(f"Applied filter: {filter_name}")

    def open_attachments_panel(self):
        """Open the attachments panel."""
        attachment_button = self.driver.find_element(By.ID, "rtr-2")
        if "active" not in attachment_button.get_attribute("class"):
            attachment_button.click()
        time.sleep(2)

    def download_all_attachments(self, filename: str = "COA files.zip"):
        """
        Download all attachments as a ZIP file.

        Args:
            filename: Name for the downloaded ZIP file
        """
        # Select all attachments
        checkbox = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[aria-label="Select All"]'))
        )
        self.driver.execute_script("arguments[0].click();", checkbox)
        time.sleep(2)

        # Click Actions button
        actions_btn = self.wait.until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR, 'button[data-client-id="atp-20"][aria-disabled="false"]'
            ))
        )
        self.driver.execute_script("arguments[0].click();", actions_btn)

        # Click Download
        download_button = WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((
                By.XPATH, '//div[@class="css-5vtc5c" and contains(text(), "Download")]'
            ))
        )
        self.driver.execute_script("arguments[0].scrollIntoView(true);", download_button)
        self.driver.execute_script("arguments[0].click();", download_button)

        # Set filename
        filename_input = self.wait.until(
            EC.presence_of_element_located((By.CSS_SELECTOR, 'input[type="text"][value$=".zip"]'))
        )
        filename_input.send_keys(Keys.CONTROL + "a")
        filename_input.send_keys(Keys.BACKSPACE)
        filename_input.send_keys(filename)

        # Click OK
        ok_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((
                By.XPATH, '//button[@type="submit" and .//span[text()="OK"]]'
            ))
        )
        ok_button.click()

        print(f"Download started: {filename}")
        time.sleep(10)  # Wait for download

    def close(self):
        """Close the browser."""
        if self.driver:
            self.driver.quit()

    def download_coa_files(self, filter_name: str = "L3M Data", filename: str = "COA files.zip"):
        """
        Full workflow to download COA files.

        Args:
            filter_name: Name of the filter to apply
            filename: Name for the downloaded ZIP file
        """
        try:
            self.login()
            self.navigate_to_sheet()
            self.apply_filter(filter_name)
            self.open_attachments_panel()
            self.download_all_attachments(filename)
            print("COA files downloaded successfully!")
        finally:
            self.close()


# =============================================================================
# DEACOM AUTOMATION
# =============================================================================
class DeacomAutomation:
    """
    Automates interactions with Deacom ERP system.
    """

    def __init__(self, username: str, password: str, url: str, driver_path: str = None):
        """
        Initialize Deacom automation.

        Args:
            username: Deacom login username
            password: Deacom login password
            url: Deacom URL
            driver_path: Path to chromedriver executable
        """
        self.username = username
        self.password = password
        self.url = url
        self.driver_path = driver_path
        self.driver = None
        self.wait = None

    def login(self):
        """Login to Deacom."""
        if self.driver_path:
            service = Service(self.driver_path)
            self.driver = webdriver.Chrome(service=service)
        else:
            self.driver = webdriver.Chrome()

        self.wait = WebDriverWait(self.driver, 20)
        self.driver.get(self.url)

        # Enter username
        username_input = self.wait.until(
            EC.element_to_be_clickable((
                By.XPATH, "//label[contains(text(), 'Username')]/following-sibling::input"
            ))
        )
        username_input.click()
        username_input.send_keys(self.username)

        # Enter password
        password_input = self.wait.until(
            EC.element_to_be_clickable((
                By.XPATH, "//label[contains(text(), 'Password')]/following-sibling::input"
            ))
        )
        password_input.click()
        password_input.send_keys(self.password + Keys.RETURN)

        time.sleep(5)

        # Handle "Yes" dialog if appears
        try:
            yes_buttons = self.driver.find_elements(
                By.XPATH, '//span[text()="Yes"]/parent::button'
            )
            if yes_buttons:
                yes_buttons[0].click()
                print("Clicked 'Yes' on dialog")
        except Exception:
            pass

        print("Logged in to Deacom")

    def navigate_to_purchasing_report(self):
        """Navigate to Purchasing > Order Reporting."""
        # Click Purchasing menu
        purchasing_button = self.wait.until(
            EC.element_to_be_clickable((By.ID, "Menu_Purchasing"))
        )
        self.driver.execute_script("arguments[0].click();", purchasing_button)

        # Click Order Reporting
        order_reporting = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//ul[contains(@id, 'Menu_Links_Purchasing')]//span[normalize-space()='Order Reporting']"
            ))
        )
        self.driver.execute_script("arguments[0].click();", order_reporting)
        time.sleep(2)

    def download_lots_received_report(self):
        """Download Lots Received report as Excel."""
        # Select report type
        report_dropdown = self.wait.until(
            EC.presence_of_element_located((
                By.XPATH, "//label[normalize-space()='Report Type']/following-sibling::select"
            ))
        )
        self.driver.execute_script("arguments[0].scrollIntoView(true);", report_dropdown)
        Select(report_dropdown).select_by_visible_text("Lots Received")

        # Select status
        status_dropdown = self.wait.until(
            EC.presence_of_element_located((
                By.XPATH, "//label[normalize-space()='Status']/following-sibling::select"
            ))
        )
        all_option = status_dropdown.find_element(
            By.XPATH, ".//option[normalize-space()='All Orders']"
        )
        all_option.click()

        # Click View
        view_button = WebDriverWait(self.driver, 20).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//div[contains(@class,'ToolBarRow')]"
                "//a[contains(@class,'ViewButton') and not(ancestor::div[contains(@style,'display: none')])]"
            ))
        )
        self.driver.execute_script("arguments[0].click();", view_button)
        time.sleep(5)

        # Click Excel export
        self._wait_for_overlays()
        excel_button = self.wait.until(
            EC.presence_of_element_located((
                By.XPATH,
                "//a[contains(@class,'ExcelButton') and @title='Excel' "
                "and not(ancestor::div[contains(@style,'display: none')])]"
            ))
        )
        self.driver.execute_script("arguments[0].scrollIntoView(true);", excel_button)
        time.sleep(0.5)

        try:
            excel_button.click()
        except Exception:
            self.driver.execute_script("arguments[0].click();", excel_button)

        print("Excel export initiated!")

    def navigate_to_price_updates(self):
        """Navigate to Inventory > Price Updates."""
        # Click Inventory menu
        inventory_button = self.wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "span.MenuInventory"))
        )
        inventory_button.click()

        # Click Price Updates
        price_updates = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//*[contains(text(), 'Price Updates')]"))
        )
        price_updates.click()

        # Set Change From to Spreadsheet
        change_from_dropdown = self.wait.until(
            EC.presence_of_element_located((
                By.XPATH, "//label[text()='Change From']/following-sibling::select"
            ))
        )
        Select(change_from_dropdown).select_by_visible_text("Spreadsheet")
        time.sleep(2)

    def upload_spreadsheet(self, file_path: str):
        """
        Upload a spreadsheet file.

        Args:
            file_path: Path to the Excel file to upload
        """
        # Find file input
        file_input = self.wait.until(
            EC.presence_of_element_located((
                By.XPATH, "//label[text()='Spreadsheet']/following-sibling::input[@type='file']"
            ))
        )
        file_input.send_keys(file_path)

        # Click Continue
        continue_button = self.wait.until(
            EC.element_to_be_clickable((
                By.XPATH, "//div[contains(@class,'ToolBarButton')]//div[normalize-space()='Continue']"
            ))
        )
        continue_button.click()

        # Click Yes confirmation
        yes_button = self.wait.until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Yes']"))
        )
        yes_button.click()

        # Wait for processing
        self.wait.until(
            EC.invisibility_of_element_located((
                By.XPATH, "//div[contains(text(),'Rollup costs')]"
            ))
        )

        # Click OK
        self.wait.until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//div[contains(@class,'DialogVisible')]//button[.//span[normalize-space()='Ok']]"
            ))
        ).click()

        print(f"Uploaded: {file_path}")

    def upload_spreadsheet_in_batches(self, file_path: str, batch_size: int = 100):
        """
        Upload a spreadsheet in batches to avoid timeouts.

        Args:
            file_path: Path to the Excel file
            batch_size: Number of rows per batch
        """
        df = pd.read_excel(file_path)
        num_batches = math.ceil(len(df) / batch_size)
        print(f"Total rows: {len(df)}, Total batches: {num_batches}")

        base_path = os.path.dirname(file_path)
        base_name = os.path.splitext(os.path.basename(file_path))[0]

        for batch_idx in range(num_batches):
            start = batch_idx * batch_size
            end = start + batch_size
            batch_df = df.iloc[start:end]

            batch_file = os.path.join(base_path, f"{base_name}_batch_{batch_idx + 1}.xlsx")
            batch_df.to_excel(batch_file, index=False)
            print(f"[INFO] Batch {batch_idx + 1}/{num_batches} saved: {batch_file}")

            try:
                self._upload_single_batch(batch_file)
                print(f"[INFO] Batch {batch_idx + 1} uploaded successfully.")
            except Exception as e:
                print(f"[ERROR] Batch {batch_idx + 1} failed: {e}")
                continue

            time.sleep(5)

    def _upload_single_batch(self, file_path: str):
        """Upload a single batch file."""
        file_input = self.driver.find_element(By.XPATH, "//input[@type='file']")
        file_input.send_keys(file_path)

        continue_button = WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((
                By.XPATH, "//div[contains(@class,'ToolBarButton')]//div[normalize-space()='Continue']"
            ))
        )
        continue_button.click()

        yes_button = WebDriverWait(self.driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, "//button[normalize-space()='Yes']"))
        )
        yes_button.click()

        WebDriverWait(self.driver, 300).until(
            EC.invisibility_of_element_located((
                By.XPATH, "//div[contains(text(),'Rollup costs')]"
            ))
        )

        WebDriverWait(self.driver, 60).until(
            EC.element_to_be_clickable((
                By.XPATH,
                "//div[contains(@class,'DialogVisible')]//button[.//span[normalize-space()='Ok']]"
            ))
        ).click()

    def _wait_for_overlays(self):
        """Wait for loading overlays to disappear."""
        overlays = ["WaitWindow", "lui_ID34_Grid", "load_ID34_Grid"]
        for overlay_id in overlays:
            try:
                self.wait.until(EC.invisibility_of_element_located((By.ID, overlay_id)))
            except Exception:
                pass

    def close(self):
        """Close the browser."""
        if self.driver:
            self.driver.quit()


# =============================================================================
# MAIN WORKFLOW FUNCTIONS
# =============================================================================
def download_coa_files_from_smartsheet():
    """Download COA files from Smartsheet."""
    automation = SmartsheetAutomation(
        SMARTSHEET_EMAIL,
        SMARTSHEET_PASSWORD,
        SMARTSHEET_URL
    )
    automation.download_coa_files()


def download_purchasing_report_from_deacom():
    """Download Lots Received report from Deacom."""
    automation = DeacomAutomation(
        DEACOM_USERNAME,
        DEACOM_PASSWORD,
        DEACOM_URL,
        CHROME_DRIVER_PATH if CHROME_DRIVER_PATH else None
    )

    try:
        automation.login()
        automation.navigate_to_purchasing_report()
        automation.download_lots_received_report()
        print("Purchasing report downloaded!")
    finally:
        automation.close()


def upload_price_updates_to_deacom(file_path: str):
    """Upload price updates to Deacom."""
    automation = DeacomAutomation(
        DEACOM_USERNAME,
        DEACOM_PASSWORD,
        DEACOM_URL,
        CHROME_DRIVER_PATH if CHROME_DRIVER_PATH else None
    )

    try:
        automation.login()
        automation.navigate_to_price_updates()
        automation.upload_spreadsheet_in_batches(file_path)
        print("Price updates uploaded!")
    finally:
        automation.close()


# =============================================================================
# ENTRY POINT
# =============================================================================
if __name__ == "__main__":
    print("COA Automation Module")
    print("=" * 50)
    print("\nAvailable functions:")
    print("1. download_coa_files_from_smartsheet()")
    print("2. download_purchasing_report_from_deacom()")
    print("3. upload_price_updates_to_deacom(file_path)")
    print("\nImport this module and call the functions as needed.")
