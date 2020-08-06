from sqlalchemy import String, Column, Integer, Enum, ForeignKey

from DatabaseORM import Base
from DatabaseORM.enums import CommissionPositionType


class CommissionMember(Base):

    __tablename__ = 'commission_member'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    position = Column(Enum(CommissionPositionType))
    commission_id = Column(Integer, ForeignKey('commission.id'))
    nominator_id = Column(Integer, ForeignKey('nominator.id'))
