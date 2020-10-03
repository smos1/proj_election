import os
import django
from pathlib import Path
from csv import DictReader 

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "elections_db.settings")
django.setup()

from ORM.models import Region

BASE_DIR = Path(__file__).resolve(strict=True).parent
DATA_DIR = os.path.join(BASE_DIR, "data")

if __name__=="__main__":
    with open(os.path.join(DATA_DIR,"regions.csv"), newline="", encoding="utf-8") as regions_csv:
        reader = DictReader(regions_csv)
        for row in reader:
            region, created = Region.objects.get_or_create(**row)
            region.save()
    