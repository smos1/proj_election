from Uploader import Uploader
from pathlib import Path
from ORM.models import Region
import os
import pandas as pd
from django.db.models import Max


class RegionUploader(Uploader):   
    BASE_DIR = Path(__file__).resolve(strict=True).parent
    DATA_DIR = os.path.join(BASE_DIR, "data")
    FILE_NAME = "regions.csv"

    @classmethod
    def read_data(self):
        return pd.read_csv(os.path.join(self.DATA_DIR, self.FILE_NAME), 
                                      encoding="utf-8")

    @classmethod
    def save_data_to_db(self, region_data):
        Region.objects.all().delete()
        max_region_id = Region.objects.aggregate(Max(self.ID))['id__max']
        print(max_region_id)
        max_region_id = max_region_id if max_region_id else -1

        region_index_df = self._make_new_index(region_data, max_region_id+1)
        region_data = self._replace_old_index_with_new(region_index_df, region_data, self.ID)
        region_data = self.assure_df_compatibility(Region, region_data, auto_id=False)

        engine = self.create_sqlalchemy_engine()
        region_data.to_sql(Region.objects.model._meta.db_table,
                            if_exists='append', index=False, con=engine, method='multi', chunksize=10)


if __name__ == '__main__':
    uploader = RegionUploader()
    regions_df = uploader.read_data()
    uploader.save_data_to_db(regions_df)