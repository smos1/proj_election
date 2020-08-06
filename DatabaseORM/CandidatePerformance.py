from sqlalchemy import String, Column, Integer, Enum, ForeignKey

from DatabaseORM import Base
from DatabaseORM.enums import CandidateType


class CandidatePerformance(Base):

    __tablename__ = 'candidate_performance'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    candidate_type = Column(Enum(CandidateType))
    commission_id = Column(Integer, ForeignKey('commission.id'))
    election_id = Column(Integer, ForeignKey('election.id'))
    nominator_id = Column(Integer, ForeignKey('nominator.id'))
    votes_number = Column(Integer)
