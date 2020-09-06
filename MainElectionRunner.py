import pandas as pd 
import lxml
from lxml import html
import bs4 as bs
import requests
from io import StringIO, BytesIO
from PIL import Image
from pathlib import Path
from selenium import webdriver
import time



main_url = 'http://www.vybory.izbirkom.ru/region/izbirkom'


def detect_captcha_text(image):
    '''
    Gets a byte image
    Returns text from image

    '''
    import pytesseract
    stream = BytesIO(image)
    image_rgb = Image.open(stream).convert("RGBA")
    stream.close()
    gray_image = image_rgb.convert('L')
    text_image = pytesseract.image_to_string(gray_image)
    return text_image



def replace_o_as_0(list_of_char):
    '''

    Replaces o with 0
    '''
    for index in range(len(list_of_char)):
        if list_of_char[index] == 'o':
            list_of_char[index] = '0'
        else:
            continue
    return list_of_char



def find_captcha(driver):
    '''

    Takes captcha, detects it
    Returns Captcha input field element and captcha text
    

    '''
    captcha = driver.find_element_by_id("captchaImg").screenshot_as_png
    captcha_text = detect_captcha_text(captcha)
    text_list = list(captcha_text)
    text_list_prep = replace_o_as_0(text_list)
    text_prep = ''.join(text_list_prep)
    captcha_input_field = driver.find_element_by_id('captcha')
    return captcha_input_field , text_prep



def get_html_source(url):
    '''
    Gets a source site url and gets through captcha
    Return page html

    '''
    import time
    from selenium import webdriver
    driver = webdriver.Chrome('./chromedriver')  
    driver.get(url)
    time.sleep(1)
    # find link to Общероссийское голосование по вопросу одобрения изменений в Конституцию      Российской Федерации
    # link = driver.find_elements_by_css_selector('table:nth-child(17) tbody:nth-child(1)                                             tr:nth-child(2) td:nth-child(2) > a.vibLink')[0]
    # link.click()
    time.sleep(1) # Let the user actually see something!
    
    while True:
        captcha_input_field, text_prep = find_captcha(driver)
        time.sleep(1)
    
        captcha_input_field.send_keys(text_prep)
        time.sleep(1)
        driver.find_element_by_id('send').click()
        time.sleep(1)
        html_source = driver.page_source
        if 'РЕЗУЛЬТАТЫ ОБЩЕРОССИЙСКОГО ГОЛОСОВАНИЯ' in html_source:
            break
    return html_source


def get_region_result_links(html_page):
    '''
    Parsing links and names from dropdown table


    Input: html page
    Output: pandas dataframe

    '''
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html_page, 'html.parser')
    regions_dict = {}
    for option in soup.find_all('option')[1:]:
        regions_dict[f'{option.text}'] = option['value']
    regions_df = pd.DataFrame(regions_dict.items(), columns = ['Region', 'Url'])
    return regions_df


def get_election_result(url, driver):
    '''
    Gets a source site url from regions and gets through captcha,
    downloads excel files with election results
    

    '''
    import time
    # from selenium import webdriver
    # driver = webdriver.Chrome('./chromedriver')  
    driver.get(url)
    time.sleep(1)
    while True:
        captcha_input_field, text_prep = find_captcha(driver)
        time.sleep(1)
    
        captcha_input_field.send_keys(text_prep)
        time.sleep(1)
        driver.find_element_by_id('send').click()
        time.sleep(1)
        html_source = driver.page_source
        # КОСТЫЛЬ
        if 'РЕЗУЛЬТАТЫ ОБЩЕРОССИЙСКОГО ГОЛОСОВАНИЯ' in html_source:
            break
    vote_table = driver.find_element_by_link_text('Сводная таблица итогов голосования')
    vote_table.click()
    print_version = driver.find_element_by_link_text('Версия для печати')
    print_version.click()
    return print_version


links = pd.read_csv('all_levels_links.csv', index_col='Unnamed: 0')


def load_election_results(urls_df, region_level=0):
    '''
    TEST
    Input: dataframe with different level urls
    Output: loads pivot tables with election results


    '''
    # create folder for downloaded files
    saving_folder = f'/election results level_{region_level}'
    # if os.path.exists(saving_folder):
    #     print(f"Folder {saving_folder} already exists! /n Deleting folder")
    #     os.rmdir(saving_folder)
    # os.mkdir(saving_folder)
    # print(f"Folder {saving_folder} created!")

    # set a chromedriver saving settings
    chrome_options = webdriver.ChromeOptions() 
    prefs = {"profile.default_content_settings.popups": 0,
             "download.default_directory": 
                        f"{os.getcwd() + saving_folder}",#IMPORTANT - ENDING SLASH V IMPORTANT
             "directory_upgrade": True}
    chrome_options.add_experimental_option("prefs", prefs)
    driver = webdriver.Chrome('./chromedriver', chrome_options=chrome_options)

    for url in urls_df[f'REGION_LVL_{region_level}_URL']:
        get_election_result(url, driver)
        break


class ElectionRunner():
    '''
    Download election results
    Preprocess downloaded data

    '''

    def __init__(self, driver_path, saving_path):
        self.driver_path = driver_path
        self.saving_path = saving_path
    
    def load_election_results(self, urls_df, region_level):
        '''
        Input: dataframe with different level urls
        Output: loads pivot tables with election results


        '''
        # create folder for downloaded files
        saving_folder = f'{self.saving_path}/election_data_lvl_{region_level}'
        # if os.path.exists(saving_folder):
        #     print(f"Folder {saving_folder} already exists! /n Deleting folder")
        #     os.rmdir(saving_folder)
        # os.mkdir(saving_folder)
        # print(f"Folder {saving_folder} created!")

        # set a chromedriver saving settings
        chrome_options = webdriver.ChromeOptions() 
        prefs = {"profile.default_content_settings.popups": 0,
                 "download.default_directory": f"{os.getcwd() + saving_folder}",
                 "directory_upgrade": True}
        chrome_options.add_experimental_option("prefs", prefs)
        driver = webdriver.Chrome(self.driver_path, chrome_options=chrome_options)


        # remove break if all files needed
        for url in urls_df[f'REGION_LVL_{region_level}_URL']:
            get_election_result(url, driver)
            break
    
    def preprocess_data(self, data):
        cl_df = data.copy()
        for row in cl_df[f'{cl_df.columns[0]}']:
            if 'Дата' in str(row):
                vote_date = row.split(' ')[2]
            elif 'Наименование' in str(row):
                izbirkom_name = row.split(':')[-1].strip()
       
            
        cl_df_dr = cl_df.dropna(thresh = 2)
        cl_df_dr = cl_df_dr.T.reset_index(drop=True)
        cl_df_dr.columns = cl_df_dr.iloc[1]
        cl_df_dr.dropna()
        reg_name_col = cl_df_dr.iloc[:, 0].dropna()
        cl_df_dr['Избирательная комиссия'] = reg_name_col
        cl_df_dr.dropna()
        df_clean = cl_df_dr[[
            'Число участников голосования, включенных в список участников голосования на момент окончания голосования',
            'Число бюллетеней, выданных участникам голосования',
            'Число бюллетеней, содержащихся в ящиках для голосования',
            'Число недействительных бюллетеней', 'ДА', 'НЕТ', 'Избирательная комиссия']]
        df_clean = df_clean.drop(df_clean.index[:3])
        df_clean['Регион'] = izbirkom_name
        df_clean['Дата голосования'] = vote_date
        df_clean = df_clean.reset_index(drop=True)
        return df_clean


runner = ElectionRunner(driver_path = './chromedriver', saving_path = '')

runner.load_election_results(urls_df = links, region_level=0)

rep = pd.read_excel('election_data_lvl_0/report.xls')

prep_data = runner.preprocess_data(rep)



