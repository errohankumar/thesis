import os
import shutil
import time
import re

from selenium import webdriver
#Chrome driver is not good
#from webdriver_manager.chrome import ChromeDriverManager

# Extends Selenium WebDriver classes to include the request function from the Requests library,
# while doing all the needed cookie and request headers handling.
from seleniumrequests import Firefox

# Set the chrome executable path for ChromeDriver
#driver = webdriver.Chrome(executable_path='/Users/rohankumar/PycharmProjects/StreamingAnalytics/softwares/chromedriver ')

# this is used for locating elements withing a webpage
from selenium.webdriver.common.by import By

# gives control over the firefox options such as download folder etc..
from selenium.webdriver.firefox.options import Options

# Causes webdriver to wait for sometime
from selenium.webdriver.support.ui import WebDriverWait

from selenium.webdriver.support import expected_conditions as EC
from datetime import datetime, timedelta

class EntsoeDownloader:
    """
    A class used to automatically scrap CSV files from ENTSOE Transparecny Platform
    ...
    Attributes
    ----------
    __username : str
        a string representing username of an existing ENTSOE account
    __password : str
        a string representing password of an existing ENTSOE account
    __login_url : str
        the homepage link of ENTSOE
    __download_dir : str
        the folder path where CSV files are stored
    date: str
        string representing today's date
    __day_ahead_price_url: None
        the download url for scraping day ahead prices
    __generation_per_product_url: None
        the download url for scraping electricity generation per products
    __actual_total_load_url: None
        the donwload url for scraping actual loads
    __generation_forecasted_url
        the download url for scraping the generation forecasted
    driver: None
        contains the selenium chrome driver class
    Methods
    -------
    check_download(i=0)
        Checks whether the CSV file is downloaded within a certain time-period else it terminates
    setup(headless=False, date_input=None)
        Initializes the chrome webdriver for scraping
    set_download_url()
        Initializes the download urls
    login_and_download()
        Interacts with the firefox browser and executes a sequence of tasks for scraping
    """
    def __init__(self, last_date_fetched, username="", password=""):

        last_date_fetched = datetime.strptime(last_date_fetched, "%Y-%m-%d %H:%M:%S")
        new_date = last_date_fetched.date()
        new_date += timedelta(days=1)

        self.__username = username
        self.__password = password
        self.__login_url = "https://transparency.entsoe.eu/homepageLogin"
        self.__download_dir = os.path.join(os.getcwd(), "download")

        # choose another date if the last hour of fetched data is 23:00:00
        self.date = last_date_fetched.date() if last_date_fetched.time().hour != 23 else new_date

        self.__day_ahead_price_url = None
        self.__generation_per_product_url = None
        self.__actual_total_load_url = None
        self.__generation_forecasted_url = None
        self.driver = None

    @staticmethod
    def check_download(i=0):
        """
        Checks first whether files have been downloaded within a 20 second time interval
        Parameters
        -------------
         i:
            File counter, to check how many files have been downloaded
        """
        time_to_wait = 150
        time_counter = 0

        # Validate whether file has been downloaded
        while True:

            if time_counter > time_to_wait:
                raise FileNotFoundError("Necessary files not found in directory (20 seconds timeout reached)!")
            # List all the files in the : 'current_working_dir'/download
            file_names = os.listdir(os.getcwd() + "/download")
            if len(file_names) == i:
                return True

            time.sleep(1)
            time_counter += 1

    def setup(self, headless=False, date_input=None):

        # Validate date variable
        if date_input is not None:
            self.date = datetime.strptime(date_input, "%d.%m.%Y").date()

        # Create directory if fetched folder is not available
        if not os.path.exists(self.__download_dir):
            os.mkdir(self.__download_dir)

        # Set headless option and firefox profile
        options = Options()
        options.headless = headless

        fp = webdriver.FirefoxProfile()
        fp.set_preference("browser.download.folderList", 2)
        fp.set_preference("browser.download.manager.showWhenStarting", False)
        fp.set_preference("browser.download.dir", self.__download_dir)
        fp.set_preference("browser.helperApps.neverAsk.saveToDisk",
                          "text/plain, application/vnd.ms-excel, text/csv, text/comma-separated-values, "
                          "application/octet-stream")

        # Initialize Firefox() object to navigate
        self.driver = Firefox(firefox_profile=fp, options=options)

        return self

    def set_download_url(self):
        """
        Sets class url attributes if date is given
        """

        if self.date is None:
            raise ValueError("Attribute 'date' needs to be defined")

        self.__day_ahead_price_url = "https://transparency.entsoe.eu/transmission-domain/r2/dayAheadPrices/export?" \
                                     "name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime=" + self.date.strftime(
            "%d.%m.%Y") + "+00%3A00%7CCET%7CDAY&biddingZone.values=CTY%7C10Y1001A1001A83F!BZN%7C10Y1001A1001A82H&dateTime.timezone=CET_CEST&dateTime" \
                          ".timezone_input=CET+(UTC%2B1)+%2F+CEST+(UTC%2B2)&dataItem=ALL&timeRange=YEAR&exportType=CSV"

        self.__generation_per_product_url = "https://transparency.entsoe.eu/generation/r2/actualGenerationPerProductionType/export?" \
                                            "name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&datepicker-day-offset-select-dv-date-from_input=D&dateTime.dateTime="+self.date.strftime(
            "%d.%m.%Y")+"+00%3A00%7CCET%7CDAYTIMERANGE&dateTime.endDateTime="+self.date.strftime(
            "%d.%m.%Y")+"+00%3A00%7CCET%7CDAYTIMERANGE&area.values=CTY%7C10Y1001A1001A83F!BZN%7C10Y1001A1001A82H&productionType.values=B01&productionType." \
                        "values=B02&productionType.values=B03&productionType.values=B04&productionType.values=B05&productionType.values=B06&productionType." \
                        "values=B07&productionType.values=B08&productionType.values=B09&productionType.values=B10&productionType.values=B11&productionType." \
                        "values=B12&productionType.values=B13&productionType.values=B14&productionType.values=B20&productionType.values=B15&productionType." \
                        "values=B16&productionType.values=B17&productionType.values=B18&productionType.values=B19&dateTime.timezone=CET_CEST&dateTime." \
                        "timezone_input=CET+(UTC%2B1)+%2F+CEST+(UTC%2B2)&dataItem=ALL&timeRange=YEAR&exportType=CSV"

        self.__actual_total_load_url = "https://transparency.entsoe.eu/load-domain/r2/totalLoadR2/export?name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime" \
                                       ".dateTime="+self.date.strftime(
            "%d.%m.%Y")+"+00%3A00%7CCET%7CDAY&biddingZone.values=CTY%7C10Y1001A1001A83F!BZN%7C10Y1001A1001A82H&dateTime.timezone=CET_CEST&dateTime" \
                        ".timezone_input=CET+(UTC%2B1)+%2F+CEST+(UTC%2B2)&dataItem=ALL&timeRange=YEAR&exportType=CSV"


        self.__generation_forecasted_url = "https://transparency.entsoe.eu/generation/r2/dayAheadGenerationForecastWindAndSolar/export?" \
                                           "name=&defaultValue=false&viewType=TABLE&areaType=BZN&atch=false&dateTime.dateTime="+self.date.strftime(
            "%d.%m.%Y") +"+00%3A00%7CCET%7CDAYTIMERANGE&dateTime" \
                                           ".endDateTime="+ self.date.strftime(
            "%d.%m.%Y") + "+00%3A00%7CCET%7CDAYTIMERANGE&area.values=CTY%7C10Y1001A1001A83F!BZN%7C10Y1001A1001A82H&productionType" \
                                           ".values=B16&productionType.values=B18&productionType.values=B19&processType.values=A18&processType.values=A01&processType" \
                                           ".values=A40&dateTime.timezone=CET_CEST&dateTime.timezone_input=CET+(UTC%2B1)+%2F+CEST+(UTC%2B2)&dataItem=ALL&timeRange=YEAR&exportType=CSV"


    def login_and_download(self):
        """
        Executes a sequence of tasks to log into ENTSOE and downloads the specified files
        """

        if self.__username == "" or self.__password == "":
            raise ValueError("Pleaser set the credentials for ENTSOE to download")

        # Remove "download" directory to and create new one
        if len(os.listdir(os.getcwd() + "/download")) > 0:
            shutil.rmtree(self.__download_dir)
            os.mkdir(self.__download_dir)

        # Instantiate the download urls and request the browser for the homepage login url
        self.set_download_url()
        self.driver.get(self.__login_url)

        # Find login form elements and insert ENTSOE account credentials
        self.driver.find_element_by_id("email").send_keys(self.__username)
        self.driver.find_element_by_id("password").send_keys(self.__password)

        # Trigger click event to sign in
        self.driver.find_element_by_xpath("//div[@class='submit-line']/div/input").click()

        # Wait until login is completed and the "user-panel-drop-down" element is agreed
        WebDriverWait(self.driver, 10).until(
            EC.presence_of_element_located((By.ID, "close-button"))
        )

        # Agree to the terms and conditions
        loader_div = self.driver.find_element_by_xpath("//*[@id='loading']")
        self.driver.execute_script("arguments[0].setAttribute('style','visibility:hidden;');", loader_div)

        self.driver.find_element_by_id("close-button").click()

        # Open download url in new tab
        urls = [self.__generation_per_product_url, self.__day_ahead_price_url,
                self.__actual_total_load_url, self.__generation_forecasted_url]

        for url, index in zip(urls, range(len(urls))):
            if self.check_download(index):
                print("Opening url: ", re.search("/r2/(.*)/export", url).group(1))
                self.driver.execute_script("window.open('" + url + "');")

        # Download CSV file and exit the browser
        time_counter = 0
        time_to_wait = 150  # the last CSV file which is the one holding forecast information on solar, wind and total
        # takes at least 40 seconds to download

        while len(os.listdir(os.path.join(os.getcwd(), "download"))) is not 4:
            if time_counter > time_to_wait: raise FileNotFoundError(
                "Necessary files not found in directory"
                " (150 seconds timeout reached)!")
            time.sleep(1)
            time_counter += 1

        self.driver.quit()
