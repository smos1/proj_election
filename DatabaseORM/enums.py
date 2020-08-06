from enum import Enum, auto

class ElectionLevel(Enum):

    FEDERAL = auto()
    REGIONAL = auto()
    MUNICIPAL = auto()

class ElectionMandateType(Enum):

    SINGLE_MANDATE = auto()
    MULTI_MANDATE = auto()
    PARTY_LIST = auto()

class NominatorType(Enum):

    PARTY = auto()
    SELF_NOMINATION = auto()
    OTHER = auto()

class DistrictType(Enum):

    REGION = auto()
    WHOLE_COUNTRY = auto()
    OKRUG = auto()
    RAYON = auto()

class CommissionType(Enum):

    CIK = auto()
    OIK = auto()
    TIK = auto()
    UIK = auto()
    MIK = auto()

class CommissionPositionType(Enum):

    HEAD = auto()
    DEPUTY = auto()
    SECRETARY = auto()
    MEMBER = auto()

class CandidateType(Enum):

    PARTY = auto()
    PERSON = auto()
    OTHER = auto()



