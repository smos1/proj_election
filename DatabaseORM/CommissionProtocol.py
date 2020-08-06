from sqlalchemy import Column, Integer, ForeignKey

from DatabaseORM import Base


class CommissionProtocol(Base):
    __tablename__ = 'commission_protocol'
    id = Column(Integer, primary_key=True)

    commission_id = Column(Integer, ForeignKey('commission.id'))
    election_id = Column(Integer, ForeignKey('election.id'))
    amount_of_voters = Column(Integer)
    ballots_received = Column(Integer)
    ballots_given_out_early = Column(Integer)
    ballots_given_out_at_stations = Column(Integer)
    ballots_given_out_outside = Column(Integer)
    canceled_ballots = Column(Integer)
    ballots_found_outside = Column(Integer)
    ballots_found_at_station = Column(Integer)
    valid_ballots = Column(Integer)
    invalid_ballots = Column(Integer)
    lost_ballots = Column(Integer)
    appeared_ballots = Column(Integer)
