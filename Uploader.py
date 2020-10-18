import datetime
import os
from collections import OrderedDict

from elections_db.settings import SQLALCHEMY_DB_CREDENTIALS
from sqlalchemy import create_engine
from sqlalchemy.engine import url

import django
from django.db.models import IntegerField, BigIntegerField, CharField, ForeignKey, DateField, BooleanField, \
    DateTimeField, FloatField, ManyToManyField, OneToOneField, Max, AutoField

import pandas as pd
import numpy as np

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elections_db.settings")
django.setup()


class Uploader:
    ID='id'
    NEW_ID = 'new_id'
    NEW = 'new'

    FIELD_MATCHER = {AutoField: np.int64,
                     IntegerField: pd.Int64Dtype(),
                     BigIntegerField: pd.Int64Dtype(),
                     CharField: np.object,
                     ForeignKey: pd.Int64Dtype(),
                     DateField: np.object,
                     BooleanField: np.bool,
                     DateTimeField: np.datetime64,
                     FloatField: np.float,
                     ManyToManyField: pd.Int64Dtype(),
                     OneToOneField: pd.Int64Dtype()}

    TRANSLATE_COLUMNS_TO_OUR_BASE = {}

    TRANSLATE_VALUES_TO_OUR_BASE = {}

    @classmethod
    def change_column_names(self, df):
        df.rename(columns = self.TRANSLATE_COLUMNS_TO_OUR_BASE, inplace=True)

    @classmethod
    def change_column_values(self, df):
        df.replace(self.TRANSLATE_VALUES_TO_OUR_BASE, inplace=True)

    @classmethod
    def assure_df_compatibility(self, model, df, auto_id=True):
        '''
        checks that df contains all the required columns for uploading data to db and that their type is compatible;
        if auto_id is set to False and there is no id column in df, it will be created from df index
        '''

        model_field_names = OrderedDict([(a.get_attname(), a.__class__) for a in model._meta.fields])
        if auto_id:
            del model_field_names[self.ID]
        elif self.ID not in df.columns:
            df = df.reset_index().rename(columns = {'index': self.ID})
        missing_columns = np.setdiff1d(list(model_field_names.keys()), df.columns)
        if missing_columns.size>0:
            print('These columns are missing in downloaded df: {}'.format(list(missing_columns)))

        for col in missing_columns:
            df[col] = np.nan

        df = df[list(model_field_names.keys())]
        for colname, coltype in model_field_names.items():
            try:
                df[colname] = df[colname].astype(self.FIELD_MATCHER[coltype])
            except ValueError:
                raise ValueError("Can't convert df column to database required type")

        return df

    @classmethod
    def create_sqlalchemy_engine(self):
        '''
        required by df.to_sql() pandas method
        '''
        return create_engine(url.URL(**SQLALCHEMY_DB_CREDENTIALS))


    @classmethod
    def _make_new_index(self, df, index_start):
        '''
        creates a reference dataframe containing values for new and old indices
        '''
        new_index = pd.DataFrame({self.ID: df[self.ID],
                                  self.NEW_ID: np.arange(index_start, index_start+df.shape[0])
                                  })
        return new_index

    @classmethod
    def _make_new_index(self, df, index_start):
        '''
        creates a reference dataframe containing values for new and old indices
        '''
        new_index = pd.DataFrame({self.ID: df[self.ID],
                                  self.NEW_ID: np.arange(index_start, index_start+df.shape[0])
                                  })
        return new_index

    @classmethod
    def _replace_old_index_with_new(self, index_df, target_df, target_column_name):
        '''
        replaces values in target_df index columns with values of new index
        index df should have old keys in first column and new keys in second column
        '''
        new_index_name = self.NEW + target_column_name
        index_df.columns = [target_column_name, new_index_name]
        target_df = pd.merge(target_df, index_df, on=target_column_name, how='left')
        target_df.drop(target_column_name, inplace=True, axis=1)
        target_df.rename(columns={new_index_name: target_column_name}, inplace=True)
        return target_df

    @classmethod
    def parse_date(cls, date):
        return datetime.datetime.strptime(date, "%Y%m%d").date()