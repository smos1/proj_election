from sqlalchemy import String, Column, Integer, Enum

from DatabaseORM import Base
from DatabaseORM.enums import DistrictType


class District(Base):

    __tablename__ = 'district'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    district_type = Column(Enum(DistrictType))

