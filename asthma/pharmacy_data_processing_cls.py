from asthma.codebook import *
from fuzzywuzzy import fuzz, process


class IdentifyControllersAndRelievers:

    def __init__(self):
        self._processed_generic_product_name = False
        self._processed_claim_status = False

    def _process_generic_product_name(self, df):
        arr = df.generic_product_name.str.strip().str.upper().values
        df['generic_product_name'] = arr
        self._processed_generic_product_name = True

    def _process_claim_status(self, df):
        arr = df.claim_status.str.strip().str.upper().values
        df['claim_status'] = arr
        self._processed_claim_status = True

    def _identify_matching_controllers(self, df):
        if not self._processed_generic_product_name:
            self._process_generic_product_name(df)

        matching_controllers = (df
            .generic_product_name.drop_duplicates()
            .loc[lambda x: x != ''].to_frame().apply(
                lambda x: process.extractOne(x['generic_product_name'],
                                             CONTROLLERS,
                                             scorer=fuzz.token_sort_ratio,
                                             score_cutoff=100), axis=1)
            .dropna().map(lambda x: x[0]).drop_duplicates().values)
        self.controllers = matching_controllers

    def get_matching_controllers(self, df):
        if not hasattr(self, 'controllers'):
            self._identify_matching_controllers(df)
        return self.controllers

    def identify_controllers(self, df):
        if not hasattr(self, 'controllers'):
            self._identify_matching_controllers(df)

        if not self._processed_claim_status:
            self._process_claim_status(df)

        df['controller'] = 0
        idx = df.loc[
            lambda x: ((x.claim_status == 'PAID') &
                       (x.generic_product_name.isin(self.controllers)))].index
        df.loc[idx, 'controller'] = 1

    def identify_relievers(self, df):
        if not self._processed_generic_product_name:
            self._process_generic_product_name(df)

        df['reliever'] = 0
        idx = df.loc[
            lambda x: ((x.claim_status == 'PAID') &
                       (x.generic_product_name.map(
                           lambda x: (('LEVALBUTEROL' in x) or
                                      ('ALBUTEROL' in x) or
                                      ('METAPROTERENOL' in x) or
                                      ('PIRBUTEROL' in x)))))].index
        df.loc[idx, 'reliever'] = 1

    def get_controllers_and_relievers(self, df):
        self.identify_controllers(df)
        self.identify_relievers(df)
