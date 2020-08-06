from sqlalchemy import String, Column, Integer, Enum, ForeignKey

from DatabaseORM import Base
from DatabaseORM.enums import NominatorType

class Nominator(Base):

    __tablename__ = 'nominator'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    superior_nominator_id = Column(Integer, ForeignKey('nominator.id'))
    nominator_type = Column(Enum(NominatorType))
