from asthma.data_validation import ClaimViewDataValidation
from asthma.claim_data_processing_cls import *


class ClaimViewDataProcessing:

    def __init__(self, filepath):
        self._df = ClaimViewDataValidation(filepath).get_validated_data()
        self._multiple_medicaid_ids = None

    def get_processed_data(self):
        ProcessMemberMedicaidIDs().process_medicaid_ids(self._df)
        IdentifyAsthmaRelatedClaims().extract_asthma_flags(self._df)
        IdentifyVisitTypes().extract_visit_types(self._df)
        return self._df
