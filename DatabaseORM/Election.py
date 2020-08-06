from sqlalchemy import String, Column, Integer, Enum, ForeignKey

from DatabaseORM import Base
from DatabaseORM.enums import ElectionLevel, ElectionMandateType


class Election(Base):

    __tablename__ = 'election'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    election_level = Column(Enum(ElectionLevel))
    election_mandate_type = Column(Enum(ElectionMandateType))
    mandates = Column(Integer)
    previous_election_id = Column(Integer, ForeignKey('election.id'))
    superior_election_id = Column(Integer, ForeignKey('election.id'))
