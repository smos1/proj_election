from sqlalchemy import String, Column, Integer, Enum, ForeignKey, BigInteger, Date

from DatabaseORM import Base
from DatabaseORM.enums import CommissionType


class Commission(Base):

    __tablename__ = 'commission'
    id = Column(Integer, primary_key=True)
    iz_id = Column(BigInteger)

    name = Column(String)
    commission_type = Column(Enum(CommissionType))
    address = Column(String)
    superior_commission_id = Column(Integer, ForeignKey('commission.id'))
    region_id = Column(Integer, ForeignKey('region.id'))
    phone = Column(String)
    fax = Column(String)
    email = Column(String)
    end_date = Column(String)
    address_voteroom = Column(String)
    phone_voteroom = Column(String)
    lat_ik = Column(String)
    lon_ik = Column(String)
    lat_voteroom = Column(String)
    lon_voteroom = Column(String)
    snapshot_date = Column(Date)
