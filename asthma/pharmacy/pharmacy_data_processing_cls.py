from asthma.codebook import *
from fuzzywuzzy import fuzz, process


class IdentifyControllersRelievers:

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


class CalculateAMRScore:

    @staticmethod
    def _check_data_for_amr(df):
        for col in ['controller', 'reliever']:
            if col not in df.columns:
                msg = f"""AMR cannot be calculated without "{col}". Please use 
                IdentifyControllerAndReliever().get_controllers_and_relievers() 
                method for calculating controller and reliever binary variables.
                """
                raise KeyError(msg)

    def get_amr_score_old(self, df):
        self._check_data_for_amr(df)
        temp = df.groupby(['member_medicaid_id', 'claim_start_date']).agg(
            controller_=('controller', 'sum'), reliever_=('reliever', 'sum'))
        temp['controller_'] = (temp.controller_ > 0).astype(int)
        temp['reliever_'] = (temp.reliever_ > 0).astype(int)
        temp.reset_index(inplace=True)
        df_final = temp.groupby('member_medicaid_id').agg(
            num_controller_old=('controller_', 'sum'),
            num_reliever_old=('reliever_', 'sum'))
        df_final.reset_index(inplace=True)
        df_final['AMR_old'] = df_final.num_controller_old.div(
            df_final.num_controller_old.add(df_final.num_reliever_old))
        return (df_final.dropna(subset='AMR_old').assign(
            member_medicaid_id=lambda x: x.member_medicaid_id.astype(int),
            AMR_old=lambda x: x.AMR_old.round(1)))

    def get_amr_score_count(self, df):
        self._check_data_for_amr(df)
        temp = df.loc[lambda x: x.controller == 1].groupby(
            'member_medicaid_id').agg(
            num_controller_count=('controller', 'sum'),
            num_reliever_new=('reliever', 'sum'))
        temp.reset_index(inplace=True)
        temp['AMR_count'] = temp.num_controller_count.div(
            temp.num_controller_count.add(temp.num_reliever_new))
        return (temp.dropna(
            subset='AMR_count').assign(
            member_medicaid_id=lambda x: x.member_medicaid_id.astype(int),
            AMR_count=lambda x: x.AMR_count.round(1)))

    def get_amr_score_days_supply(self, df):
        self._check_data_for_amr(df)
        temp = df.loc[lambda x: x.controller == 1].groupby(
            'member_medicaid_id').agg(total_days_supply=('days_supply', 'sum'),
                                      num_reliever=('reliever', 'sum'))
        temp.reset_index(inplace=True)
        temp['num_controller_days_supply'] = temp.total_days_supply.div(30)
        temp['AMR_days_supply'] = temp.num_controller_days_supply.div(
            temp.num_controller_days_supply.add(temp.num_reliever))
        return (temp[
            ['member_medicaid_id', 'num_controller_days_supply',
             'AMR_days_supply']].dropna(subset='AMR_days_supply').assign(
            member_medicaid_id=lambda x: x.member_medicaid_id.astype(int),
            num_controller_days_supply=lambda x: (x.num_controller_days_supply
                                                  .round(2)),
            AMR_days_supply=lambda x: x.AMR_days_supply.round(1)))

    def get_amr_scores(self, df):
        df_amr_old = self.get_amr_score_old(df)
        df_amr_count = self.get_amr_score_count(df)
        df_amr_supply = self.get_amr_score_days_supply(df)
        return df_amr_old.merge(df_amr_count, how='outer').merge(
            df_amr_supply, how='outer')


class GetLastThreeControllers:

    def __init__(self, df):
        self._data = (df[
            ['member_medicaid_id', 'claim_start_date', 'drug_strength',
             'drug_product_name', 'claim_status', 'refill_code', 'days_supply',
             'generic_product_name', 'pharmacy_name', 'pharmacy_phone_number',
             'controller']]
                      .loc[lambda x: x.controller == 1]
                      .drop('controller', axis=1)
                      .reset_index(drop=True))

    @staticmethod
    def _extract_last_controller(group):
        temp = group.sort_values('claim_start_date', ascending=False)
        if temp.shape[0] >= 1:
            return temp.head(1)
        else:
            return None

    def _get_last_controller(self):
        temp = self._data.groupby('member_medicaid_id').apply(
            self._extract_last_controller)
        temp.reset_index(drop=True, inplace=True)
        col_names = [f'{col}_rec1'
                     for col in temp.drop('member_medicaid_id', axis=1).columns]
        temp.columns = ['member_medicaid_id'] + col_names
        return temp

    @staticmethod
    def _extract_last_second_controller(group):
        temp = group.sort_values('claim_start_date', ascending=False)
        if temp.shape[0] >= 2:
            return temp.head(2).tail(1)
        else:
            return None

    def _get_last_second_controller(self):
        temp = self._data.groupby('member_medicaid_id').apply(
            self._extract_last_second_controller)
        temp.reset_index(drop=True, inplace=True)
        col_names = [f'{col}_rec2'
                     for col in temp.drop('member_medicaid_id', axis=1).columns]
        temp.columns = ['member_medicaid_id'] + col_names
        return temp

    @staticmethod
    def _extract_last_third_controller(group):
        temp = group.sort_values('claim_start_date', ascending=False)
        if temp.shape[0] >= 3:
            return temp.head(3).tail(1)
        else:
            return None

    def _get_last_third_controller(self):
        temp = self._data.groupby('member_medicaid_id').apply(
            self._extract_last_third_controller)
        temp.reset_index(drop=True, inplace=True)
        col_names = [f'{col}_rec3'
                     for col in temp.drop('member_medicaid_id', axis=1).columns]
        temp.columns = ['member_medicaid_id'] + col_names
        return temp

    def get_controllers(self):
        df1 = self._get_last_controller()
        df2 = self._get_last_second_controller()
        df3 = self._get_last_third_controller()
        return df1.merge(df2, how='outer').merge(df3, how='outer')
