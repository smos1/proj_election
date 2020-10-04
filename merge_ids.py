import os
import django
from tqdm import tqdm
import math

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elections_db.settings")
django.setup()

from ORM.models import CikUik, ResultsVseros, Region
from django.core.exceptions import ObjectDoesNotExist

BATCH_SIZE = 100

def batch_update(results_batch):
    updated_records = []
    for result in results_batch:
        region = Region.objects.get(name=result.reg)
        try:
            uik = CikUik.objects.get(name=result.uik.replace("УИК","Участковая избирательная комиссия"), region=region.name_eng)
            result.uik_id = uik.id
            result.tik_id = uik.parent_id
            updated_records.append(result)
        except ObjectDoesNotExist:
            pass
    return updated_records


if __name__=="__main__":
    results_vseros = ResultsVseros.objects.all()
    batches_count = math.ceil(len(results_vseros)/float(BATCH_SIZE))
    with tqdm(total=batches_count) as progress_bar:
        for batch_number in range(batches_count):
            start_index = batch_number * BATCH_SIZE
            end_index = start_index + BATCH_SIZE
            batch_results = batch_update(results_vseros[start_index:end_index])
            ResultsVseros.objects.bulk_update(batch_results, fields = ['uik_id','tik_id'], batch_size= BATCH_SIZE)
            progress_bar.update(1)
            
            