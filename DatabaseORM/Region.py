from sqlalchemy import String, Column, Integer

from DatabaseORM import Base

class Region(Base):

    __tablename__ = 'region'
    id = Column(Integer, primary_key=True)
    name = Column(String)

