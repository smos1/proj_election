import datetime
import os
from collections import OrderedDict

import django
import pandas as pd
import requests
from io import BytesIO
import libarchive
from django.db.models import IntegerField, BigIntegerField, CharField, ForeignKey, DateField, BooleanField, \
    DateTimeField, FloatField, ManyToManyField, OneToOneField, Max, AutoField
import numpy as np
from sqlalchemy import create_engine
from sqlalchemy.engine import url

from elections_db.settings import SQLALCHEMY_DB_CREDENTIALS

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elections_db.settings")
django.setup()

from ORM.models import Commission, CommissionMember, Nominator


class CommissionDataDownloader:

    ID='id'
    NEW_ID = 'new_id'
    PARENT_ID = 'parent_id'
    NEW = 'new'
    IK_ID = 'ik_id'
    SNAPSHOT_DATE = 'snapshot_date'
    NAME = 'name'
    DEFAULT = 'default'

    GISLAB_SNAPSHOT_DATES = ["20140404",
                             "20140629",
                             "20141119",
                             "20150915",
                             "20160229",
                             "20160629",
                             "20170121",
                             "20170719",
                             "20180215",
                             "20180709",
                             "20180906",
                             "20190101",
                             "20190313",
                             "20190617",
                             "20190905",
                             "20191110",
                             "20200212",
                             "20200610",
                             "20200628",
                             "20200910"]

    BASE_GISLAB_URL = 'http://gis-lab.info/data/cik/'

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

    TRANSLATE_CIK_TO_OUR_BASE = {
        'parent_id': 'superior_commission_id',
        'type_ik': 'commission_type'
    }

    @classmethod
    def change_column_names(cls, df):
        df.rename(columns = cls.TRANSLATE_CIK_TO_OUR_BASE, inplace=True)


    @classmethod
    def create_sqlalchemy_engine(cls):
        '''
        required by df.to_sql() pandas method
        '''
        return create_engine(url.URL(**SQLALCHEMY_DB_CREDENTIALS))

    @classmethod
    def download_and_parse_all_snapshots(cls):
        for snapshot_date in cls.GISLAB_SNAPSHOT_DATES:
            date_in_datetime = cls.parse_date(snapshot_date)
            cik_uik_data = cls.get_df_from_site("cik_uik", snapshot_date)
            cik_people_data = cls.get_df_from_site("cik_people", snapshot_date)
            cls.save_all_data_to_db(cik_uik_data, cik_people_data, date_in_datetime)

    @classmethod
    def parse_date(cls, date):
        return datetime.datetime.strptime(date, "%Y%m%d").date()

    @classmethod
    def get_df_from_site(cls, data_type, snapshot_date):
        '''

        '''
        url = "".join([cls.BASE_GISLAB_URL, data_type, "_", snapshot_date, ".7z"])
        resp = requests.get(url).content
        with libarchive.memory_reader(resp) as archive:
            for file in archive:
                blocks = b''.join([b for b in file.get_blocks()])
                df = pd.read_csv(BytesIO(blocks))
                return df  # assuming one file per archive

    @classmethod
    def assure_df_compatibility(cls, model, df, auto_id=True):
        '''
        checks that df contains all the required columns for uploading data to db and that their type is compatible;
        if auto_id is set to False and there is no id column in df, it will be created from df index
        '''

        model_field_names = OrderedDict([(a.get_attname(), a.__class__) for a in model._meta.fields])
        if auto_id:
            del model_field_names[cls.ID]
        elif cls.ID not in df.columns:
            df = df.reset_index().rename(columns = {'index': cls.ID})
        cls.change_column_names(df)
        missing_columns = np.setdiff1d(list(model_field_names.keys()), df.columns)
        print('These columns are missing in downloaded df: {}'.format(list(missing_columns)))

        for col in missing_columns:
            df[col] = np.nan

        df = df[list(model_field_names.keys())]
        for colname, coltype in model_field_names.items():
            try:
                df[colname] = df[colname].astype(cls.FIELD_MATCHER[coltype])
            except ValueError:
                raise ValueError("Can't convert df column to database required type")

        return df

    @classmethod
    def _make_new_index(cls, df, index_start):
        '''
        creates a reference dataframe containing values for new and old indices
        '''
        new_index = pd.DataFrame({cls.ID: df[cls.ID],
                                  cls.NEW_ID: np.arange(index_start, index_start+df.shape[0])
                                  })
        return new_index

    @classmethod
    def _repalace_old_index_with_new(cls, index_df, target_df, target_column_name):
        '''
        replaces values in target_df index columns with values of new index
        '''
        new_index_name = cls.NEW + target_column_name
        index_df.columns = [target_column_name, new_index_name]
        target_df = pd.merge(target_df, index_df, on=target_column_name, how='left')
        target_df.drop(target_column_name, inplace=True, axis=1)
        target_df.rename(columns={new_index_name: target_column_name}, inplace=True)
        return target_df

    @classmethod
    def update_nominators(cls, nominator_list):
        '''
        checks if nominators from list exist in database and adds them if needed
        '''
        unique_nominators = np.unique(nominator_list)
        existing_nominators = list(Nominator.objects.all().values(cls.NAME))
        updatable_nominators = np.setdiff1d(unique_nominators, existing_nominators)

        ## add nominator classification_function_here
        engine = cls.create_sqlalchemy_engine()
        pd.DataFrame({cls.NAME:updatable_nominators}).to_sql(Nominator.name, if_exists='append', index=False, con=engine)

    @classmethod
    def save_all_data_to_db(cls, cik_uik_data, cik_people_data, snapshot_date):
        Commission.objects.filter(snapshot_date=snapshot_date).delete()
        CommissionMember.objects.filter(snapshot_date=snapshot_date).delete()

        max_commission_id = Commission.objects.aggregate(Max(cls.ID))['id__max']
        max_member_id = Commission.objects.aggregate(Max(cls.ID))['id__max']
        max_commission_id = max_commission_id if max_commission_id else -1
        max_member_id = max_member_id if max_member_id else -1

        cik_uik_data[cls.SNAPSHOT_DATE] = snapshot_date
        cik_people_data[cls.SNAPSHOT_DATE] = snapshot_date

        ## replacing ids in dataframe with new ones (to avoid conflicts within database)
        cik_index_df = cls._make_new_index(cik_uik_data, max_commission_id+1)
        cik_uik_data = cls._repalace_old_index_with_new(cik_index_df, cik_uik_data, cls.ID)
        cik_uik_data = cls._repalace_old_index_with_new(cik_index_df, cik_uik_data, cls.PARENT_ID)
        cik_people_data = cls._repalace_old_index_with_new(cik_index_df, cik_people_data, cls.IK_ID)

        people_index_df = cls._make_new_index(cik_people_data, max_member_id+1)
        cik_people_data = cls._repalace_old_index_with_new(people_index_df, cik_people_data, cls.ID)

        cik_uik_data = cls.assure_df_compatibility(Commission, cik_uik_data, auto_id=False)
        cik_people_data = cls.assure_df_compatibility(CommissionMember, cik_people_data, auto_id=False)

        engine = cls.create_sqlalchemy_engine()
        cik_uik_data.to_sql(Commission.objects.model._meta.db_table, if_exists='append', index=False, con=engine)
        cik_people_data.to_sql(CommissionMember.objects.model._meta.db_table, if_exists='append', index=False, con=engine)

        cls.update_nominators(cik_people_data)

if __name__ == '__main__':
    CommissionDataDownloader.download_and_parse_all_snapshots()