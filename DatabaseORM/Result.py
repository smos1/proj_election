from sqlalchemy import Column, Integer, ForeignKey

from DatabaseORM import Base

class Result(Base):

    __tablename__ = 'result'
    id = Column(Integer, primary_key=True)

    candidate_performance_id = Column(Integer, ForeignKey('candidate_performance.id'))
    commission_id = Column(Integer, ForeignKey('commission.id'))
    election_id = Column(Integer, ForeignKey('election.id'))
    votes_number = Column(Integer)
