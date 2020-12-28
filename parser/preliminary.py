#!/usr/bin/env python
# coding: utf-8
import multiprocessing
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
import numpy as np


CHUNK_SIZE = 10

def parse_elections_main(start_date:date, end_date:date, debug=True):

    def get_results_from_upper_level_df(df):
        data = dict()
        for i, j in tqdm(df.iterrows(), desc="Collecting links_to UIKs"):
            data.update({j.link: get_links_UIK(j.link, dct={}, driver=driver)})

        df = pd.DataFrame([content for pack in data.values() for content in pack])
        results_data = {i: get_election_result(i, driver, level=1) for i in df.link_to_UIK}
        summary_data = {i: get_election_result(i, driver, level=1) for i in df.summary_found.unique()}

        check_sums_vs_summary_data(results_data, summary_data)
        return results_data

    df_with_links = get_upper_level_links(start_date, end_date)

    # get links for  UIKs
    if debug:
        driver = webdriver.Chrome(ChromeDriverManager().install())
        get_results_from_upper_level_df(df_with_links)
    else:
        split_df = np.array_split(df_with_links, np.ceil(df_with_links.shape[0]/CHUNK_SIZE))
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        pool = multiprocessing.Pool(processes=4)
        results = pool.map(get_results_from_upper_level_df, split_df)
        results = pd.concat(results, axis=0)

    return results


def check_sums_vs_summary_data(results_data, summary_data):
    pass






if __name__ == '__main__':
    parse_elections_main(date(2020,9,12), date(2020,9,14))