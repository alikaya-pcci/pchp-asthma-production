from asthma.codebook import *
from fuzzywuzzy import fuzz, process


class IdentifyControllersAndRelievers:

    @staticmethod
    def _process_generic_product_name(df):
        arr = df.generic_product_name.str.strip().str.upper().values
        df['generic_product_name'] = arr

    def _identify_matching_controllers(self, df):
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

    def _members_with_controllers(self, df):
        if not hasattr(self, 'controllers'):
            self._identify_matching_controllers(df)

        members_with_controllers = (df
            .loc[lambda x: x.claim_status.str.strip() == 'PAID']
            .loc[lambda x: x.generic_product_name.isin(self.controllers)])
        self.data = members_with_controllers

    def extract_controllers_by_days_supply(self, df):
        if not hasattr(self, 'data'):
            self._members_with_controllers(df)

        df_controllers = (self.data
            .groupby('member_medicaid_id')['days_supply'].sum().to_frame()
            .reset_index())
        arr = df_controllers.member_medicaid_id.astype(int).astype(str)
        df_controllers['member_medicaid_id'] = arr
        return df_controllers

    def extract_controllers_by_count(self, df):
        if not hasattr(self, 'data'):
            self._members_with_controllers(df)

        df_controllers = (self.data
            .groupby('member_medicaid_id').size()
            .to_frame(name='controller_count').reset_index())
        arr = df_controllers.member_medicaid_id.astype(int).astype(str)
        df_controllers['member_medicaid_id'] = arr
        return df_controllers

    @staticmethod
    def extract_relievers(df):
        df_relievers = (df.loc[lambda x: x.claim_status.str.strip() == 'PAID']
            .loc[lambda x: x.generic_product_name.map(
                lambda y: (('LEVALBUTEROL' in y) or
                           ('ALBUTEROL' in y) or
                           ('METAPROTERENOL' in y) or
                           ('PIRBUTEROL' in y)))]
            .groupby('member_medicaid_id').size().to_frame(name='relievers')
            .reset_index())
        arr = df_relievers.member_medicaid_id.astype(int).astype(str)
        df_relievers['member_medicaid_id'] = arr
        return df_relievers

    @staticmethod
    def get_amr_scores(df):
        pass

