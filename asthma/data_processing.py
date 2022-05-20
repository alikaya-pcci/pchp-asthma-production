from asthma.data_validation import *
from asthma.claim.claim_data_processing_cls import *
from asthma.claim.claim_member_level_cls import *
from asthma.pharmacy.pharmacy_data_processing_cls import *


class ClaimViewDataProcessing:

    def __init__(self, filepath):
        self._df = ClaimViewDataValidation(filepath).get_validated_data()
        self._processed = False

    def get_processed_data(self):
        ProcessMemberMedicaidIDs().process_medicaid_ids(self._df)
        IdentifyAsthmaRelatedClaims().extract_asthma_flags(self._df)
        IdentifyVisitTypes().extract_visit_types(self._df)
        self._processed = True
        return self._df

    def get_member_level_data(self):
        if not self._processed:
            self._df = self.get_processed_data()

        comorbidities = IdentifyComorbidities().identify_comorbidities(self._df)
        visits = IdentifyPastVisits().get_past_visits(self._df)
        return visits.merge(comorbidities, how='outer')


class PharmacyViewDataProcessing:

    def __init__(self, filepath):
        self._df = PharmacyViewDataValidation(filepath).get_validated_data()

    def get_member_level_data(self):
        IdentifyControllersRelievers().get_controllers_and_relievers(self._df)
        amr = CalculateAMRScore().get_amr_scores(self._df)
        amr['member_medicaid_id'] = (amr.member_medicaid_id.astype(int)
                                     .astype(str))
        controllers = GetLastThreeControllers(self._df).get_controllers()
        controllers['member_medicaid_id'] = (controllers.member_medicaid_id
                                             .astype(int).astype(str))
        return amr.merge(controllers, how='outer')


class GetCombinedMemberLevelData:

    def __init__(self, filepath_claim, filepath_pharmacy):
        self._fc = filepath_claim
        self._fp = filepath_pharmacy

    def get_data(self):
        claims = ClaimViewDataProcessing(self._fc).get_member_level_data()
        pharma = PharmacyViewDataProcessing(self._fp).get_member_level_data()
        return claims.merge(pharma, how='outer')
