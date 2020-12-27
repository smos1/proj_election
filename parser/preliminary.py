#!/usr/bin/env python
# coding: utf-8
import re

import numpy as np
from datetime import date

import pandas as pd
from PIL import Image, ImageFilter
from selenium import webdriver
import time
import pytesseract
from typing import BinaryIO, List, Dict
from bs4 import BeautifulSoup

from io import BytesIO
from selenium.common.exceptions import NoSuchElementException        
from selenium.webdriver.support.ui import Select
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

from UpperLevelLinkCollector import get_upper_level_links
from selenium.webdriver.chrome.options import Options

from helper import get_links_UIK, get_election_result

chrome_options = Options()
#chrome_options.add_argument("--headless")


def parse_elections_main(start_date:date, end_date:date):
    driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)

    df_with_links = get_upper_level_links(start_date, end_date)
    # get links for  UIKs
    data=dict()

    for i,j in tqdm(df_with_links.iterrows(), desc="Collecting links_to UIKs"):
        data.update({j.link:get_links_UIK(j.link, dct={}, driver=driver)})

    processed_dicts = [[a[0][0], a[0][1:], a[1]] for a in process(data, [])]

    df = pd.DataFrame(processed_dicts).explode(2, ignore_index=True)

    result_data = {i: get_election_result(j, driver) for i, j in df.items()}

    return result_data


def process(dct, path=[]):
    list_of_tuples = []
    for i, j in dct.items():
        if isinstance(j, list):
            list_of_tuples.append((path + [i], j))
        elif isinstance(j, dict):
            list_of_tuples += process(j, path + [i])
    return list_of_tuples








if __name__ == '__main__':
    parse_elections_main(date(2020,1,1), date(2020,1,27))