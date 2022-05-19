from asthma.data_validation import *
from asthma.claim_data_processing_cls import *
from asthma.claim_member_level_cls import *


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

        df_member_level = IdentifyPastVisits().get_past_visits(self._df)
        return df_member_level


class PharmacyViewDataProcessing:

    def __init__(self, filepath):
        self._df = PharmacyViewDataValidation(filepath).get_validated_data()

    def get_member_level_data(self):
        pass
