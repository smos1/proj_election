from bs4 import BeautifulSoup
import pandas as pd
import os
from PIL import Image, ImageFilter
from selenium import webdriver
import time
import pytesseract
from typing import BinaryIO, List, Dict
from io import BytesIO
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.support.ui import Select
import re
import numpy as np

SLEEP_TIME = 0.5

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

    text_image = pytesseract.image_to_string(gray_image,
                                             config='--psm 7 -c tessedit_char_whitelist=0123456789')
    text_image=re.sub('[\W]', '', text_image)

    if len(text_image) !=5:
        text_image = '00000'

    return text_image



def find_and_recognize_captcha(driver) -> Dict:
    """Finds element with captcha and detects captcha text
    Args:
        driver: selenium driver
    Returns:
        Dict: captcha input field and captcha text
    """
    captcha_png = driver.find_element_by_id("captchaImg").screenshot_as_png
    captcha_text = detect_captcha_text(captcha_png).replace("o", "0")
    captcha_input_field = driver.find_element_by_id("captcha")
    return {"captcha_input_field": captcha_input_field, "captcha_text": captcha_text}


def get_through_captcha(driver, url: str):
    """Iterativly tries to get through captcha
    Args:
        driver: selenium driver
        url (str): url link
    Returns:
        None
    """

    driver.get(url)
    time.sleep(SLEEP_TIME)
    flag = 1
    while flag:
        try:
            element_dlya = driver.find_element_by_xpath("//table")
            flag = 0
        except NoSuchElementException:
            #empty_space = driver.find_element_by_xpath("//html")
            #empty_space.click()
            captcha_elements = find_and_recognize_captcha(driver)
            captcha_input_field = captcha_elements["captcha_input_field"]
            captcha_text = captcha_elements["captcha_text"]
            captcha_input_field.send_keys(captcha_text)
            driver.find_element_by_id("send").click()
            time.sleep(SLEEP_TIME)



def get_election_result(url: str, driver, level=1) -> pd.DataFrame:
    """Load election result data
    Args:
        url (str): url link
        driver : selenium driver
        level: 1/2 position of link to election table from the bottom of the page (1 for UIK data, 2 for summary data)
    Returns:
        None: returns nothing, it downloads data
    """
    driver.get(url)

    time.sleep(SLEEP_TIME)
    get_through_captcha(driver, url)
    vote_table = driver.find_elements_by_css_selector("tr>td>nobr>a")[-level]
    vote_table.click()
    table_result = str(BeautifulSoup(driver.page_source, features="lxml").select('table:nth-of-type(5)'))
    if table_result=='[]':
        return None
    else:
        return pd.read_html(table_result)[0]



def test_if_new_layers(driver, elections_url, level_name=None, inceptions_level=0, output=[], summary_found="", summary_level_name=None, path=[]) -> list:
    flag_direct_selection = 0

    if not summary_found:
        cur_url = driver.current_url
        table = get_election_result(driver.current_url, driver, level=2)
        if type(table)==pd.DataFrame:
            summary_found = cur_url
            summary_level_name = level_name
        driver.back()

    try:
        link_to_real_data = driver.find_element_by_xpath(
            "/html/body/table[2]/tbody/tr[2]/td/a")
        flag_direct_selection = 1

    except NoSuchElementException:
        form = driver.find_element_by_xpath(
            "/html/body/table[2]/tbody/tr[2]/td/form/select")
        flag_direct_selection = 2

    if flag_direct_selection == 1:
        link_to_real_data = driver.find_element_by_xpath(
            "/html/body/table[2]/tbody/tr[2]/td/a")
        link_to_real_data.click()

        path = 'direct' if inceptions_level==0 else path
        output += [{'uik':k.next,
                    'path':path,
                    'summary_found':summary_found,
                    'summary_level_name':summary_level_name,
                    'link_to_UIK':k['value'],
                    'elections_url':elections_url} for k in BeautifulSoup(
                driver.page_source, features="lxml").select('option[value]')]
                           
        return output

    elif flag_direct_selection == 2:
        # if we have a dropdown menu with OIK
        options = BeautifulSoup(driver.page_source, features="lxml").select('option[value]')
        number_of_OIK = len(options)
        options_values = [i.text for i in options]

        # to know how many times we need to walk around
        for i in range(number_of_OIK):
            form = driver.find_element_by_xpath(
                "/html/body/table[2]/tbody/tr[2]/td/form/select")
            Select(form).select_by_visible_text(f'{options_values[i]}')
            driver.find_element_by_xpath(
                "/html/body/table[2]/tbody/tr[2]/td/form/input").click()
            inceptions_level += 1
            test_if_new_layers(driver=driver,
                               elections_url=elections_url,
                               level_name=options_values[i],
                               inceptions_level=inceptions_level,
                               output = output,
                               summary_found=summary_found,
                               summary_level_name=summary_level_name,
                               path=path+[options_values[i]])
            back_steps(driver)

        return output

    else:
        return


def back_steps(driver) -> int:
    try:
        driver.back()
        driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form")
    except:
        driver.back()
        driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form")
        return None


def get_links_UIK(link: str, dct: dict, driver) -> list:
    #time.sleep()
    get_through_captcha(driver, link)

    list_of_links_to_UIK_data = test_if_new_layers(driver=driver,
                                                   elections_url=link,
                                                   level_name='direct',
                                                   inceptions_level=0,
                                                   path=[],
                                                   output=[])

    return list_of_links_to_UIK_data


