#!/usr/bin/env python
# coding: utf-8
import logging
import multiprocessing
import os
from datetime import date

import django
import pandas as pd
from selenium import webdriver
from tqdm import tqdm
from webdriver_manager.chrome import ChromeDriverManager

from UpperLevelLinkCollector import get_upper_level_links
from selenium.webdriver.chrome.options import Options

from helper import get_links_UIK, get_election_result, get_candidates_list
import numpy as np

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elections_db.settings")
django.setup()

from ORM.models import CandidatePerformance

from ElectionDataPackage import ElectionDataPackage




logger = logging.getLogger(__name__)




class ElectionParser:
    CHUNK_SIZE = 10
    FOR = "За"

    candidate_column_translate = {'ФИО кандидата': CandidatePerformance.name.field.name,
                                  'Субъект выдвижения': "nominator",
                                  'Субьект выдвижения': "nominator",
                                  'Дата рождения кандидата': CandidatePerformance.candidate_birth_date.field.name,
                                  'Регистрация': 'registration',
                                  'регистрация': 'registration'
                                  }


    def __init__(self, debug=True):
        self.debug = debug
        if debug:
            #self.driver = webdriver.Chrome(ChromeDriverManager().install())
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        else:
            chrome_options = Options()
            chrome_options.add_argument("--headless")
            self.driver = webdriver.Chrome(ChromeDriverManager().install(), options=chrome_options)
        self.unmapped_row =set()

    def get_results_from_upper_level_df(self, upper_level_df):
        # collect links to UIK data
        election_packages = []
        for _, j in tqdm(upper_level_df.iterrows(), desc="Collecting links_to UIKs"):
            loop_ok=True
            logger.info('Loading election {}'.format(j.name))
            uik_links_data = get_links_UIK(j.election_url, dct={}, driver=self.driver)
            uik_links_data = pd.DataFrame.from_dict(uik_links_data, orient='columns')
            uiks_without_summary = uik_links_data.protocol_url[uik_links_data.summary_found.isna()].tolist()
            if uiks_without_summary:
                logger.warning("{} uiks without summary".format(str(len(uiks_without_summary))))

            # load actual election results
            unique_summaries = uik_links_data.summary_found.unique()
            results_data = {i: get_election_result(i, self.driver, level=1) for i in
                            tqdm(uik_links_data.protocol_url, desc="Loading result tables",
                                 total=uik_links_data.shape[0])}
            summary_data = {i: get_election_result(i, self.driver, level=2) for i in
                            tqdm(unique_summaries, desc="Loading summary tables", total=len(unique_summaries))}
            candidate_data = {i: get_candidates_list(i, self.driver) for i in
                             tqdm(unique_summaries, desc="Loading candidate tables", total=len(unique_summaries))}

            # process loaded results
            for summary_link in candidate_data.keys():
                candidate_data[summary_link] = self.prettify_candidate_list(candidate_data[summary_link])

            for summary_link in summary_data.keys():
                summary_data[summary_link] = self.prettify_election_dfs(summary_data[summary_link],
                                                                   candidate_data[summary_link], summary_link)

            for protocol_url in results_data.keys():
                results_data[protocol_url] = self.prettify_election_dfs(results_data[protocol_url],
                                                               candidate_data[uik_links_data.loc[
                                                                   uik_links_data.protocol_url == protocol_url, "summary_found"].iloc[
                                                                   0]],
                                                               protocol_url)
                if type(results_data[protocol_url])!=pd.DataFrame:
                    loop_ok=False
                    logging.warning('Unmapped row values found for election {}'.format(j.name))
                    break
            if not loop_ok:
                continue

            results_data = pd.concat(results_data.values(), axis=1)
            candidate_data = pd.concat(
                [df.assign(summary_link=summary_link) for summary_link, df in candidate_data.items()], axis=0)\
                .drop_duplicates(set(self.candidate_column_translate.values()))
            # check that summary values match
            for summary_link in summary_data.keys():
                self.check_sums_vs_summary_data(
                    results_data[uik_links_data.protocol_url[uik_links_data.summary_found == summary_link].tolist()],
                    summary_data[summary_link])


            results_data = pd.merge(results_data.T.reset_index().rename(columns={'index':'protocol_url'}),
                                    uik_links_data[['commission', 'protocol_url']], on='protocol_url')

            # create list of ElectionDataPackage objects
            package = ElectionDataPackage.create_packages(election_data=j,
                                                          candidate_data=candidate_data,
                                                          results_data=results_data)

            election_packages.append(package)
        return election_packages

    def parse_elections_main(self, start_date:date, end_date:date, debug=True):

        df_with_links = get_upper_level_links(start_date, end_date)

        # get links for  UIKs
        if debug:
            results = self.get_results_from_upper_level_df(df_with_links.iloc[:10])
            results = ElectionDataPackage.combine_packages(results)
            results.upload_to_database()
        else:
            split_df = np.array_split(df_with_links, np.ceil(df_with_links.shape[0]/self.CHUNK_SIZE))

            pool = multiprocessing.Pool(processes=4)
            results = list(tqdm.tqdm(pool.imap(self.get_results_from_upper_level_df, split_df), total=len(split_df), unit_scale=self.CHUNK_SIZE))
            results = ElectionDataPackage.combine_packages(results)
            results.upload_to_database()
            pool.close()

        return results


    def check_sums_vs_summary_data(self, results_data, summary_data):
        results_data = results_data.dropna().sum(axis=1)
        assert (results_data.sort_index()!=summary_data.iloc[:,0].sort_index()).sum()==0

    def prettify_election_dfs(self, df, candidate_list, link):
        assert len(df.columns) == 3

        df.columns = ['code', 'row_name', link]
        df.drop('code', axis=1, inplace=True)

        try:
            df[link] = df[link].str.replace(' .*', "", regex=True) # removing percentages, e.g. "10 20%"
        except AttributeError: # not a string column
            pass
        df[link] = df[link].astype(np.float)

        if type(candidate_list) is pd.DataFrame:
            candidates_present = self.get_candidates_present_in_election_results(df, candidate_list)
            if len(candidates_present)==1: # выборы с одним кандидатом
                df['row_name'].replace(self.FOR, candidates_present[0], inplace =True)
            candidate_rows = np.isin(df.row_name, candidates_present)
            df.loc[~candidate_rows, 'row_name'] = row_names = df.loc[~candidate_rows, 'row_name'].str.lower()
        else:
            df['row_name'] = row_names = df['row_name'].str.lower()

        unmapped_row = set(row_names.dropna().tolist()).difference(set(ElectionDataPackage.protocol_row_mapping_reversed.keys()))
        if len(unmapped_row)>0:
            self.unmapped_row |= unmapped_row
            return None

        df.dropna(axis=0, inplace=True)
        df.set_index('row_name', inplace=True)
        return df

    def prettify_candidate_list(self, candidate_df):
        candidate_df.columns = candidate_df.columns.droplevel(0)
        candidate_df = candidate_df.rename(columns = self.candidate_column_translate)
        candidate_df = candidate_df[candidate_df['registration'] == 'зарегистрирован']
        candidate_df = candidate_df[set(self.candidate_column_translate.values())]
        return candidate_df

    def get_candidates_present_in_election_results(self, result_df, candidate_list):
        return [candidate for candidate in candidate_list[CandidatePerformance.name.field.name] if np.isin(result_df['row_name'], candidate).max() == 1]




if __name__ == '__main__':
    ElectionParser(debug=True).parse_elections_main(date(2020,9,14), date(2020,10,14))