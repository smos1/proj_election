#!/usr/bin/env python
# coding: utf-8
import sys
from zipfile import ZipFile

import requests
import selenium
from bs4 import BeautifulSoup
import pandas as pd
import os
from PIL import Image
from selenium import webdriver
import time
import pytesseract
from typing import BinaryIO, List, Dict
from bs4 import BeautifulSoup
from matplotlib.pyplot import imshow
import numpy as np
from io import StringIO, BytesIO
from selenium.common.exceptions import NoSuchElementException        
from selenium.webdriver.support.ui import Select
from webdriver_manager.chrome import ChromeDriverManager

driver = webdriver.Chrome(ChromeDriverManager().install())


DRIVER_NAME = {"linux": "chromedriver",
               "darwin": "chromedriver",
               "win32": "chromedriver.exe"}

PLATFORM_DRIVER_LINKS = {'linux': "https://chromedriver.storage.googleapis.com/88.0.4324.27/chromedriver_linux64.zip",
                          'win32': "https://chromedriver.storage.googleapis.com/88.0.4324.27/chromedriver_win32.zip",
                          'darwin': "https://chromedriver.storage.googleapis.com/88.0.4324.27/chromedriver_mac64.zip"}



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
    if len(text_image)<4: text_image='00000'
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
    flag=1
    while flag:
        try:
            element_dlya=driver.find_element_by_xpath("//table")
            flag=0
        except NoSuchElementException:
            empty_space=driver.find_element_by_xpath("//html")
            empty_space.click()
            captcha_elements = find_and_recognize_captcha(driver)
            captcha_input_field = captcha_elements["captcha_input_field"]
            captcha_text = captcha_elements["captcha_text"]
            captcha_input_field.send_keys(captcha_text)
            driver.find_element_by_id("send").click()
            html_source = driver.page_source
    return None


def get_election_result(url: str, driver) -> pd.DataFrame:
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
    vote_table = driver.find_elements_by_css_selector("tr>td>nobr>a")[-1]
    vote_table.click()
    #get_through_captcha(driver, url)
    table = pd.read_html(str(BeautifulSoup(driver.page_source).select('table:nth-of-type(5)')))[0]
    return table    
    

def test_if_new_layers(driver, level_name:str, inceptions_level=0, output=dict()) -> list:
    flag_selection=0
    
    try:
        link_to_real_data=driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/a")
        flag_direct_selection=1

    except NoSuchElementException:
        form=driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form/select")
        flag_direct_selection=2
    
    if flag_direct_selection==1:
        link_to_real_data=driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/a")
        link_to_real_data.click()
        
        #print([k['value'] for k in BeautifulSoup(driver.page_source).select('option[value]')])
        if inceptions_level==0:
            output.update({'direct':[k['value'] for k in BeautifulSoup(driver.page_source).select('option[value]')]})
        else:
            output.update({level_name:[k['value'] for k in BeautifulSoup(driver.page_source).select('option[value]')]})
        return output
    
    elif flag_direct_selection==2:
        #if we have a dropdown menu with OIK
        options=BeautifulSoup(driver.page_source).select('option[value]')
        number_of_OIK=len(options)
        options_values=[i.text for i in options]
        
        
        #to know how many times we need to walk around
        print('total',number_of_OIK)
        for i in range(2, number_of_OIK+2):
            form=driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form/select")
            Select(form).select_by_visible_text(f'{options_values[i-2]}')
            driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form/input").click()
            inceptions_level+=1
            output.update({options_values[i-2]:test_if_new_layers(driver, options_values[i-2], inceptions_level,dict())})
            
            back_steps(driver)
            
        return output

    else:
        return
        
def back_steps(driver) -> int:
    counter=1
    
    try:
        driver.back()
        driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form")
    except:
        driver.back()
        driver.find_element_by_xpath("/html/body/table[2]/tbody/tr[2]/td/form")
        return None



    
def get_links_UIK(link: str, dct:dict, driver) -> list:
    driver.get(link)
    time.sleep(1)
    get_through_captcha(driver, link)
    list_of_links_to_UIK_data=dict()
    
    list_of_links_to_UIK_data=test_if_new_layers(driver, 'direct',0,dct)
    
    return list_of_links_to_UIK_data


def get_or_download_driver():
    platform_driver_name = DRIVER_NAME[sys.platform]
    platform_driver_path = "./"+ platform_driver_name
    try:
        driver = webdriver.Chrome("./" + platform_driver_path)
    except (FileNotFoundError, selenium.common.exceptions.WebDriverException):
        resp = requests.get(PLATFORM_DRIVER_LINKS[sys.platform], stream=True)
        zipfile = ZipFile(BytesIO(resp.content))
        zipfile.extract(platform_driver_name)
        os.chmod(platform_driver_path, 0o755)
        driver = webdriver.Chrome(platform_driver_path)

    return driver


driver = get_or_download_driver()

df=pd.read_csv('start_page_2010.csv')
driver_path="D:\Downloads_new\chromedriver_win32/chromedriver.exe"
driver = webdriver.Chrome(driver_path)
# get links for  UIKs
data=dict()
links_failed=[]
import pickle

os.chdir('D://Documents/GitHub/Elections')




for i,j in df.iterrows():
    try:
        data.update({j.link:get_links_UIK(j.link, dct={})})
    except:
        links_failed.append(j.link)
    if i%20==0:
        with open(f'./dicts/data_{i}.pickle', 'wb') as foo:
            pickle.dump(data, foo, protocol=pickle.HIGHEST_PROTOCOL)
        data=dict()


# save failed data
print(len(links_failed))
with open('./dicts/failed_links.pickle', 'wb') as foo:
    pickle.dump(links_failed, foo, protocol=pickle.HIGHEST_PROTOCOL)


# get data
def runner(urls:str) -> pd.DataFrame:
    driver_path="D:\Downloads_new\chromedriver_win32/chromedriver.exe"
    driver = webdriver.Chrome(driver_path)
    return {i:get_election_result(i, driver) for i in urls}
    

#testdrive
#Надо определиться, какие данные мы собираем. Все возможные или только результаты. http://www.orenburg.vybory.izbirkom.ru/region/orenburg?action=show&root=564042018&tvd=456404290626&vrn=456404290592&prver=0&pronetvd=null&region=56&sub_region=56&type=0&vibid=456404290626
kkkkkk=runner(df.link_to_UIK.sample(10).to_list())