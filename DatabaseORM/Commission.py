from sqlalchemy import String, Column, Integer, Enum, ForeignKey

from DatabaseORM import Base
from DatabaseORM.enums import CommissionType


class Commission(Base):

    __tablename__ = 'commission'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    commission_type = Column(Enum(CommissionType))
    address = Column(String)
    superior_commission_id = Column(Integer, ForeignKey('commission.id'))
    district_id = Column(Integer, ForeignKey('district.id'))
    election_id = Column(Integer, ForeignKey('election.id'))
    region_id = Column(Integer, ForeignKey('region.id'))
    longitude = Column(String)
    latitude = Column(String)
