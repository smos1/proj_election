import pandas as pd
import lxml
from lxml import html
import bs4 as bs
import requests
import os
from io import StringIO, BytesIO
from PIL import Image
from pathlib import Path
from selenium import webdriver
import time
import pytesseract
from typing import BinaryIO, List, Dict
import time
from selenium import webdriver
from bs4 import BeautifulSoup

main_url = "http://www.vybory.izbirkom.ru/region/izbirkom"


def detect_captcha_text(image: BinaryIO) -> str:
    """Detects captcha text

    Args:
        image (bytes): image parsed from html

    Returns:
        string: captcha symbols
    """

    stream = BytesIO(image)
    image_rgb = Image.open(stream).convert("RGBA")
    stream.close()
    gray_image = image_rgb.convert("L")
    text_image = pytesseract.image_to_string(gray_image)
    return text_image


def replace_o_as_0(list_of_char: List) -> List:
    """Replaces O's with zeroes

    Args:
        list_of_char (List): list of chars

    Returns:
        List: list of chars with replaced symbols
    """
    for index in range(len(list_of_char)):
        if list_of_char[index] == "o":
            list_of_char[index] = "0"
        else:
            continue
    return list_of_char


def find_and_recognize_captcha(driver) -> Dict:
    """Finds element with captcha and detects captcha text

    Args:
        driver: selenium driver

    Returns:
        Dict: captcha input field and captcha text
    """
    captcha_png = driver.find_element_by_id("captchaImg").screenshot_as_png
    captcha_text = detect_captcha_text(captcha_png)
    text_list = list(captcha_text)
    text_list_prep = replace_o_as_0(text_list)
    text_prep = "".join(text_list_prep)
    captcha_input_field = driver.find_element_by_id("captcha")
    return {"captcha_input_field": captcha_input_field, "captcha_text": text_prep}


def get_through_captcha(driver, url: str) -> None:
    """Iterativly tries to get through captcha

    Args:
        driver: selenium driver
        url (str): url link

    Returns:
        None

    """

    driver.get(url)
    time.sleep(1)
    while True:
        try:
            captcha_elements = find_and_recognize_captcha(driver)
            captcha_input_field = captcha_elements["captcha_input_field"]
            captcha_text = captcha_elements["captcha_text"]
            captcha_input_field.send_keys(captcha_text)
            driver.find_element_by_id("send").click()
            html_source = driver.page_source
        except Exception:
            print(Exception)
            break
    return None


def get_html_source(driver, url: str) -> str:
    """Gets through captcha and returns html page source

    Args:
        driver: selenium driver
        url (str): url link

    Returns:
        str: page source
    """

    driver.get(url)
    time.sleep(1)
    get_through_captcha(driver, url)
    html_source = driver.page_source
    return html_source


def get_region_result_links(html_page: str) -> pd.DataFrame:
    """Parses dropdown links from html

    Args:
        html_page (str): page source

    Returns:
        pd.DataFrame: dataframe with two columns: Region, Url
    """

    soup = BeautifulSoup(html_page, "html.parser")
    regions_dict = {}
    for option in soup.find_all("option")[1:]:
        regions_dict[f"{option.text}"] = option["value"]
    regions_df = pd.DataFrame(regions_dict.items(), columns=["Region", "Url"])
    return regions_df


def get_election_result(url: str, driver) -> None:
    """Load election result data

    Args:
        url (str): url link
        driver : selenium driver

    Returns:
        None: returns nothing, it downloads data
    """

    driver.get(url)
    time.sleep(1)
    get_through_captcha(driver, url)
    vote_table = driver.find_element_by_link_text("Сводная таблица итогов голосования")
    vote_table.click()
    print_version = driver.find_element_by_link_text("Версия для печати")
    print_version.click()
    return None


def load_election_results(urls_df: pd.DataFrame, region_level: int = 0) -> None:
    """Loads election results data

    Args:
        urls_df (pd.DataFrame): all region level links
        region_level (int, optional): region level. Defaults to 0.

    Returns:
        None: returns nothing, it downloads data
    """

    # create folder for downloaded files
    saving_folder = f"/election results level_{region_level}"
    # set a chromedriver saving settings
    chrome_options = webdriver.ChromeOptions()
    prefs = {
        "profile.default_content_settings.popups": 0,
        "download.default_directory": f"{os.getcwd() + saving_folder}",  # IMPORTANT - ENDING SLASH V IMPORTANT
        "directory_upgrade": True,
    }
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome("./chromedriver", chrome_options=chrome_options)

    for url in urls_df[f"REGION_LVL_{region_level}_URL"]:
        get_election_result(url, driver)
        break
    return None


class ElectionRunner:
    """
    Downloads election results
    Preprocesses downloaded data

    """

    def __init__(self, driver_path: str, saving_path: str):
        self.driver_path = driver_path
        self.saving_path = saving_path

    def load_election_results(self, urls_df: pd.DataFrame, region_level: int) -> None:
        """Loads election results data

        Args:
            urls_df (pd.DataFrame): all region level links
            region_level (int, optional): region level. Defaults to 0.

        Returns:
            None: returns nothing, it downloads data
        """

        # create folder for downloaded files
        saving_folder = f"{self.saving_path}/election_data_lvl_{region_level}"
        # set a chromedriver saving settings
        chrome_options = webdriver.ChromeOptions()
        prefs = {
            "profile.default_content_settings.popups": 0,
            "download.default_directory": f"{os.getcwd() + saving_folder}",
            "directory_upgrade": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(self.driver_path, chrome_options=chrome_options)
        # remove break if all files needed
        for url in urls_df[f"REGION_LVL_{region_level}_URL"]:
            get_election_result(url, driver)
            break
        return None

    def preprocess_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Preprocessing raw election data

        Args:
            data (pd.DataFrame): raw election results

        Returns:
            pd.DataFrame: preprocessed data
        """

        cl_df = data.copy()
        for row in cl_df[f"{cl_df.columns[0]}"]:
            if "Дата" in str(row):
                vote_date = row.split(" ")[2]
            elif "Наименование" in str(row):
                izbirkom_name = row.split(":")[-1].strip()

        cl_df_dr = cl_df.dropna(thresh=2)
        cl_df_dr = cl_df_dr.T.reset_index(drop=True)
        cl_df_dr.columns = cl_df_dr.iloc[1]
        cl_df_dr.dropna()
        reg_name_col = cl_df_dr.iloc[:, 0].dropna()
        cl_df_dr["Избирательная комиссия"] = reg_name_col
        cl_df_dr.dropna()
        df_clean = cl_df_dr[
            [
                "Число участников голосования, включенных в список участников голосования на момент окончания голосования",
                "Число бюллетеней, выданных участникам голосования",
                "Число бюллетеней, содержащихся в ящиках для голосования",
                "Число недействительных бюллетеней",
                "ДА",
                "НЕТ",
                "Избирательная комиссия",
            ]
        ]
        df_clean = df_clean.drop(df_clean.index[:3])
        df_clean["Регион"] = izbirkom_name
        df_clean["Дата голосования"] = vote_date
        df_clean = df_clean.reset_index(drop=True)
        return df_clean


links = pd.read_csv("all_levels_links.csv", index_col="Unnamed: 0")

runner = ElectionRunner(driver_path="./chromedriver", saving_path="")

runner.load_election_results(urls_df=links, region_level=1)

time.sleep(3)

rep = pd.read_excel("election_data_lvl_1/report.xls")

prep_data = runner.preprocess_data(rep)
