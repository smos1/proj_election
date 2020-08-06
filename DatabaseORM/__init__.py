import json

import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import psycopg2



with open('credentials.json') as credentials:
    credentials = json.load(credentials)

    url = sqlalchemy.engine.url.URL(drivername=credentials['drivername'],
                                    username=credentials['username'],
                                    password=credentials['password'],
                                    host=credentials['host'],
                                    port=credentials['port'],
                                    database=credentials['database'],
                                    query=None)
engine = create_engine(url)
Session = sessionmaker(bind=engine)

Base = declarative_base()