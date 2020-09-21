from django.conf import settings
from django.db import models
from django.utils import timezone


class Candidate(models.Model):
    name = models.CharField(max_length=1000)
    CANDIDATE_TYPES = [('party', 'party'), ('person', 'person'), ('other', 'other')]
    type = models.CharField(max_length=20, choices=CANDIDATE_TYPES)

    def __str__(self):
        return self.name


class Nominator(models.Model):
    name = models.CharField(max_length=1000)
    superior_nominator = models.ForeignKey("self", on_delete=models.CASCADE,
                                           blank=True, null=True)
    NOMINATOR_TYPES = [('self_nomination', 'self_nomination'), ('other', 'other'),
                       ('party', 'party')]
    type = models.CharField(max_length=50, choices=NOMINATOR_TYPES)

    def __str__(self):
        return self.name


class Region(models.Model):
    name = models.CharField(max_length=200)

    def __str__(self):
        return self.name


class Election(models.Model):
    name = models.CharField(max_length=1000)
    ELECTION_LEVELS = [('federal', 'federal'), ('regional', 'regional'), ('municipal', 'municipal')]
    level = models.CharField(max_length=50, choices=ELECTION_LEVELS)
    MANDATE_TYPES = [('single_mandate', 'single_mandate'), ('multi_mandate', 'multi_mandate'),
                     ('party_list', 'party_list')]
    mandate_type = models.CharField(max_length=50, choices=MANDATE_TYPES)
    mandates = models.IntegerField()
    previous_election = models.ForeignKey("self", on_delete=models.DO_NOTHING,
                                          related_name='next_elections',
                                          blank=True, null=True)
    superior_election = models.ForeignKey("self", on_delete=models.DO_NOTHING,
                                          related_name='child_elections',
                                          blank=True, null=True)

    def __str__(self):
        return self.name


class Commission(models.Model):
    name = models.CharField(max_length=1000)
    COMMISION_TYPES = [('CIK', 'Centralnya'), ('OIK', 'Okruzhnaya'), ('TIK', 'Territorialnaya'),
                       ('UIK', 'Uchastkovaya'), ('MIK', 'Municipalnaya')]
    type = models.CharField(max_length=10, choices=COMMISION_TYPES)
    address = models.CharField(max_length=2000)
    superior_commission = models.ForeignKey("self", on_delete=models.CASCADE,
                                            blank=True, null=True)
    region = models.ForeignKey(Region, on_delete=models.CASCADE,
                               blank=True, null=True)
    phone = models.CharField(max_length=250,
                             blank=True, null=True)
    fax = models.CharField(max_length=250,
                           blank=True, null=True)
    email = models.CharField(max_length=250,
                             blank=True, null=True)
    end_date = models.CharField(max_length=250,
                                blank=True, null=True)
    address_voteroom = models.CharField(max_length=250,
                                        blank=True, null=True)
    phone_voteroom = models.CharField(max_length=250,
                                      blank=True, null=True)
    lat_ik = models.CharField(max_length=250,
                              blank=True, null=True)
    lon_ik = models.CharField(max_length=250,
                              blank=True, null=True)
    lat_voteroom = models.CharField(max_length=250,
                                    blank=True, null=True)
    lon_voteroom = models.CharField(max_length=250,
                                    blank=True, null=True)
    snapshot_date = models.DateField(blank=True, null=True)

    def __str__(self):
        return self.name


class CommissionMember(models.Model):
    name = models.CharField(max_length=1000)
    POSITIONS = [('head', 'head'), ('deputy', 'deputy'), ('secretary', 'secretary'), ('member', 'member')]
    position = models.CharField(max_length=50, choices=POSITIONS)
    commission = models.ForeignKey(Commission, on_delete=models.CASCADE)
    nominator = models.ForeignKey(Nominator, on_delete=models.CASCADE)

    def __str__(self):
        return self.name


class CommissionProtocol(models.Model):
    commission = models.ForeignKey(Commission, on_delete=models.CASCADE)
    election = models.ForeignKey(Election, on_delete=models.CASCADE)
    amount_of_voters = models.IntegerField()
    ballots_received = models.IntegerField()
    ballots_given_out_early = models.IntegerField()
    ballots_given_out_at_stations = models.IntegerField()
    ballots_given_out_outside = models.IntegerField()
    canceled_ballots = models.IntegerField()
    ballots_found_outside = models.IntegerField()
    ballots_found_at_station = models.IntegerField()
    valid_ballots = models.IntegerField()
    invalid_ballots = models.IntegerField()
    lost_ballots = models.IntegerField()
    appeared_ballots = models.IntegerField()


class District(models.Model):
    name = models.CharField(max_length=1000)
    DISTRICT_TYPES = [('region', 'region'), ('okrug', 'okrug'), ('rayon', 'rayon'),
                      ('whole_country', 'whole_country')]
    type = models.CharField(max_length=50, choices=DISTRICT_TYPES)

    def __str__(self):
        return self.name


class CandidatePerformance(models.Model):
    name = models.CharField(max_length=1000)
    candidate = models.ForeignKey(Candidate, on_delete=models.CASCADE)
    commission = models.ForeignKey(Commission, on_delete=models.CASCADE)
    election = models.ForeignKey(Election, on_delete=models.CASCADE)
    nominator = models.ForeignKey(Nominator, on_delete=models.CASCADE)
    votes = models.IntegerField()

    def __str__(self):
        return self.name
