from sqlalchemy import String, Column, Integer, Enum

from DatabaseORM import Base
from DatabaseORM.enums import CandidateType


class Candidate(Base):

    __tablename__ = 'candidate'
    id = Column(Integer, primary_key=True)

    name = Column(String)
    type = Column(Enum(CandidateType))
