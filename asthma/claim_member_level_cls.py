import re
import numpy as np
from datetime import datetime
from dateutil.relativedelta import relativedelta
from asthma.codebook import *


class IdentifyComorbidities:

    def _get_diagnosis_columns(self, df):
        r = r'(?!.*admit)(?!.*desc)(?!.*icd)claim_(header|line)_diagnosis'
        pattern = re.compile(r)
        self.diag_columns = [col for col in df.columns if pattern.match(col)]

    def identify_allergic_rhinitis_diagnoses(self, df):
        if not hasattr(self, 'diag_columns'):
            self._get_diagnosis_columns(df)

        temp = df[['member_medicaid_id']]
        temp['allergic_co'] = 0
        for code_col in self.diag_columns:
            icd_col = f'{code_col}_icd_vers'
            arr = np.where(df[code_col].isin(ALLERGIC_ICD_10_CODES), 1, 0)
            idx = (df.loc[lambda x: x[icd_col] == 9]
                   .loc[lambda x: (x[code_col].ge(ALLERGIC_ICD_9_MIN_THRESH) &
                                   x[code_col].le(ALLERGIC_ICD_9_MAX_THRESH))]
                   .index)
            if idx.shape[0]:
                arr[idx] = 1

            idx_al = np.where(arr == 1)
            temp['allergic_co'].loc[idx_al] = 1

        df_ = temp.groupby('member_medicaid_id').sum().reset_index()
        df_['allergic_co'] = (df_.allergic_co > 0).astype(int)
        return df_

    def identify_obesity_diagnoses(self, df):
        if not hasattr(self, 'diag_columns'):
            self._get_diagnosis_columns(df)

        temp = df[['member_medicaid_id']]
        temp['obesity_co'] = 0
        for code_col in self.diag_columns:
            icd_col = f'{code_col}_icd_vers'
            arr = np.where(df[code_col].isin(OBESITY_ICD_10_CODES), 1, 0)
            idx = (df.loc[lambda x: x[icd_col] == 9]
                   .loc[lambda x: (x[code_col].ge(OBESITY_ICD_9_MIN_THRESH) &
                                   x[code_col].le(OBESITY_ICD_9_MAX_THRESH))]
                   .index)
            if idx.shape[0]:
                arr[idx] = 1

            idx_ob = np.where(arr == 1)
            temp['obesity_co'].loc[idx_ob] = 1

        df_ = temp.groupby('member_medicaid_id').sum().reset_index()
        df_['obesity_co'] = (df_.obesity_co > 0).astype(int)
        return df_

    def identify_obstructive_sleep_apnea_diagnoses(self, df):
        if not hasattr(self, 'diag_columns'):
            self._get_diagnosis_columns(df)

        temp = df[['member_medicaid_id']]
        temp['obs_sleep_co'] = 0
        for code_col in self.diag_columns:
            icd_col = f'{code_col}_icd_vers'
            arr = np.where(df[code_col] == OBS_SLEEP_ICD_10_CODE, 1, 0)
            idx = (df.loc[lambda x: x[icd_col] == 9]
                   .loc[lambda x: x[code_col] == OBS_SLEEP_ICD_9_CODE].index)
            if idx.shape[0]:
                arr[idx] = 1

            idx_obs = np.where(arr == 1)
            temp['obs_sleep_co'].loc[idx_obs] = 1

        df_ = temp.groupby('member_medicaid_id').sum().reset_index()
        df_['obs_sleep_co'] = (df_.obs_sleep_co > 0).astype(int)
        return df_

    def identify_gerd_diagnoses(self, df):
        if not hasattr(self, 'diag_columns'):
            self._get_diagnosis_columns(df)

        temp = df[['member_medicaid_id']]
        temp['GERD_co'] = 0
        for code_col in self.diag_columns:
            icd_col = f'{code_col}_icd_vers'
            arr = np.where(df[code_col].isin(GERD_ICD_10_CODES), 1, 0)

            idx = (df.loc[lambda x: x[icd_col] == 9]
                   .loc[lambda x: x[code_col] == GERD_ICD_9_CODE].index)
            if idx.shape[0]:
                arr[idx] = 1

            idx_gerd = np.where(arr == 1)
            temp['GERD_co'].loc[idx_gerd] = 1

        df_ = temp.groupby('member_medicaid_id').sum().reset_index()
        df_['GERD_co'] = (df_.GERD_co > 0).astype(int)
        return df_

    def identify_comorbidities(self, df):
        df_allergic = self.identify_allergic_rhinitis_diagnoses(df)
        df_obesity = self.identify_obesity_diagnoses(df)
        df_obs_sleep = self.identify_obstructive_sleep_apnea_diagnoses(df)
        df_gerd = self.identify_gerd_diagnoses(df)
        member_data = (df.member_medicaid_id.drop_duplicates()
                       .sort_values(ignore_index=True).to_frame())
        return (member_data.merge(df_allergic, how='left')
                .merge(df_obesity, how='left').merge(df_obs_sleep, how='left')
                .merge(df_gerd, how='left'))


class PastVisitsBaseClass:

    def __init__(self, df):
        columns = ['member_medicaid_id', 'ED', 'inpt', 'outpt', 'dos_from',
                   'visitID', 'total_paid_amt', 'claimid', 'prm_as',
                   'prm_sec_as', 'attending_providerid']

        no_dos_column = False
        for column in columns + ['dos']:
            if column not in df.columns:
                if column == 'dos':
                    dos = df.dos_from.map(lambda x: datetime.strptime(
                        x.strftime('%Y-%m-%d'), '%Y-%m-%d'))
                    no_dos_column = True
                else:
                    m = f'Past visits cannot be calculated without "{column}".'
                    raise KeyError(m)

        self.data = df[columns].copy()
        if no_dos_column:
            self.data['dos'] = dos
        self.data.drop('dos_from', axis=1, inplace=True)
        self.data.drop_duplicates(inplace=True)
        self.period = self.data.dos.max()
        self.member_data = (self.data.member_medicaid_id.drop_duplicates()
                            .sort_values(ignore_index=True).to_frame())


class IdentifyPastEDVisits(PastVisitsBaseClass):

    def __init__(self, df):
        super().__init__(df)

    def _calculate_all_cause_past_ed_visits(self, months_back):
        temp = self.data[['member_medicaid_id', 'ED', 'total_paid_amt', 'dos']]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=months_back))))]
        temp['ED_amt'] = temp.ED.mul(temp.total_paid_amt)
        df = temp.groupby(['member_medicaid_id', 'dos']).agg(
            is_ED=('ED', 'sum'), ED_paid_amt=('ED_amt', 'sum'))
        df['is_ED'] = (df.is_ED > 0).astype(int)
        df.reset_index(inplace=True)
        return df

    def _calculate_asthma_past_ed_visits(self, months_back):
        temp = self.data[
            ['member_medicaid_id', 'ED', 'total_paid_amt', 'dos', 'prm_sec_as']]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=months_back))))]
        temp['ED_as'] = temp.ED.mul(temp.prm_sec_as)
        temp['ED_as_amt'] = (temp.ED.mul(temp.total_paid_amt)
                             .mul(temp.prm_sec_as))
        df = temp.groupby(['member_medicaid_id', 'dos']).agg(
            is_as_ED=('ED_as', 'sum'), ED_as_paid_amt=('ED_as_amt', 'sum'))
        df['is_as_ED'] = (df.is_as_ED > 0).astype(int)
        df.reset_index(inplace=True)
        return df

    def get_past_12_months_ed_visits(self):
        df = self._calculate_all_cause_past_ed_visits(12)
        df12 = df.loc[lambda x: x.is_ED == 1].groupby('member_medicaid_id').agg(
            ED_n12=('is_ED', 'sum'),
            ED_d=('dos', 'max'),
            ED_pd_12=('ED_paid_amt', 'sum')).reset_index()
        df_as = self._calculate_asthma_past_ed_visits(12)
        df12_as = (df_as.loc[lambda x: x.is_as_ED == 1]
                   .groupby('member_medicaid_id')
                   .agg(ED_as_n12=('is_as_ED', 'sum'),
                        ED_as_d=('dos', 'max'),
                        ED_as_pd_12=('ED_as_paid_amt', 'sum'))
                   .reset_index())
        how = 'left'
        df_final = self.member_data.merge(df12, how=how).merge(df12_as, how=how)
        for c in ['ED_n12', 'ED_pd_12', 'ED_as_n12', 'ED_as_pd_12']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_6_months_ed_visits(self):
        df = self._calculate_all_cause_past_ed_visits(6)
        df6 = df.loc[lambda x: x.is_ED == 1].groupby('member_medicaid_id').agg(
            ED_n6=('is_ED', 'sum')).reset_index()
        df_as = self._calculate_asthma_past_ed_visits(6)
        df6_as = (df_as.loc[lambda x: x.is_as_ED == 1]
                  .groupby('member_medicaid_id')
                  .agg(ED_as_n6=('is_as_ED', 'sum')).reset_index())
        how = 'left'
        df_final = self.member_data.merge(df6, how=how).merge(df6_as, how=how)
        for c in ['ED_n6', 'ED_as_n6']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_3_months_ed_visits(self):
        df = self._calculate_all_cause_past_ed_visits(3)
        df3 = df.loc[lambda x: x.is_ED == 1].groupby('member_medicaid_id').agg(
            ED_n3=('is_ED', 'sum')).reset_index()
        df_as = self._calculate_asthma_past_ed_visits(3)
        df3_as = (df_as.loc[lambda x: x.is_as_ED == 1]
                  .groupby('member_medicaid_id')
                  .agg(ED_as_n3=('is_as_ED', 'sum')).reset_index())
        how = 'left'
        df_final = self.member_data.merge(df3, how=how).merge(df3_as, how=how)
        for c in ['ED_n3', 'ED_as_n3']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_ed_visits(self):
        df12 = self.get_past_12_months_ed_visits()
        df6 = self.get_past_6_months_ed_visits()
        df3 = self.get_past_3_months_ed_visits()
        return (self.member_data.merge(df12, how='left')
                .merge(df6, how='left').merge(df3, how='left'))


class IdentifyPastInpatientVisits(PastVisitsBaseClass):

    def __init__(self, df):
        super().__init__(df)

    def _calculate_all_cause_past_inpt_visits(self, months_back):
        columns = ['member_medicaid_id', 'inpt', 'total_paid_amt', 'dos']
        temp = self.data[columns]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=months_back))))]
        temp['inpt_amt'] = temp.inpt.mul(temp.total_paid_amt)
        df = temp.groupby(['member_medicaid_id', 'dos']).agg(
            is_inpt=('inpt', 'sum'), inpt_paid_amt=('inpt_amt', 'sum'))
        df['is_inpt'] = (df.is_inpt > 0).astype(int)
        df.reset_index(inplace=True)
        return df

    def _calculate_asthma_past_inpt_visits(self, months_back):
        columns = ['member_medicaid_id', 'total_paid_amt', 'prm_sec_as', 'inpt',
                   'dos']
        temp = self.data[columns]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=months_back))))]
        temp['inpt_as'] = temp.inpt.mul(temp.prm_sec_as)
        temp['inpt_as_amt'] = (temp.inpt.mul(temp.total_paid_amt)
                               .mul(temp.prm_sec_as))
        df = temp.groupby(['member_medicaid_id', 'dos']).agg(
            is_as_inpt=('inpt_as', 'sum'),
            inpt_as_paid_amt=('inpt_as_amt', 'sum'))
        df['is_as_inpt'] = (df.is_as_inpt > 0).astype(int)
        df.reset_index(inplace=True)
        return df

    def get_past_12_months_inpt_visits(self):
        df = self._calculate_all_cause_past_inpt_visits(12)
        df12 = (df.loc[lambda x: x.is_inpt == 1].groupby('member_medicaid_id')
                .agg(inpt_n12=('is_inpt', 'sum'),
                     inpt_d=('dos', 'max'),
                     inpt_pd_12=('inpt_paid_amt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_inpt_visits(12)
        df12_as = (df_as.loc[lambda x: x.is_as_inpt == 1]
                   .groupby('member_medicaid_id')
                   .agg(inpt_as_n12=('is_as_inpt', 'sum'),
                        inpt_as_d=('dos', 'max'),
                        inpt_as_pd_12=('inpt_as_paid_amt', 'sum'))
                   .reset_index())
        how = 'left'
        df_final = self.member_data.merge(df12, how=how).merge(df12_as, how=how)
        for c in ['inpt_n12', 'inpt_pd_12', 'inpt_as_n12', 'inpt_as_pd_12']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_6_months_inpt_visits(self):
        df = self._calculate_all_cause_past_inpt_visits(6)
        df6 = (df.loc[lambda x: x.is_inpt == 1].groupby('member_medicaid_id')
               .agg(inpt_n6=('is_inpt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_inpt_visits(6)
        df6_as = (df_as.loc[lambda x: x.is_as_inpt == 1]
                  .groupby('member_medicaid_id')
                  .agg(inpt_as_n6=('is_as_inpt', 'sum')).reset_index())
        how = 'left'
        df_final = self.member_data.merge(df6, how=how).merge(df6_as, how=how)
        for c in ['inpt_n6', 'inpt_as_n6']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_3_months_inpt_visits(self):
        df = self._calculate_all_cause_past_inpt_visits(3)
        df3 = (df.loc[lambda x: x.is_inpt == 1].groupby('member_medicaid_id')
               .agg(inpt_n3=('is_inpt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_inpt_visits(3)
        df3_as = (df_as.loc[lambda x: x.is_as_inpt == 1]
                  .groupby('member_medicaid_id')
                  .agg(inpt_as_n3=('is_as_inpt', 'sum')).reset_index())
        how = 'left'
        df_final = self.member_data.merge(df3, how=how).merge(df3_as, how=how)
        for c in ['inpt_n3', 'inpt_as_n3']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    @staticmethod
    def _extract_unique_inpt_visits(df, is_asthma=False):
        if is_asthma:
            columns = ['member_medicaid_id', 'dos', 'is_as_inpt']
            new_df = df.loc[lambda x: x.is_as_inpt == 1][columns]
        else:
            columns = ['member_medicaid_id', 'dos', 'is_inpt']
            new_df = df.loc[lambda x: x.is_inpt == 1][columns]

        new_df['same_guy'] = (
            new_df.member_medicaid_id == (new_df.member_medicaid_id.shift(1)
                                          .fillna(method='bfill')))
        new_df['too_close'] = (
            new_df.dos.subtract(new_df.dos.shift(1).fillna(method='bfill')).map(
                lambda x: x.days) == 1)
        new_df = new_df.loc[~(new_df.same_guy & new_df.too_close)]
        new_df.drop(['same_guy', 'too_close'], axis=1, inplace=True)
        return new_df

    def get_all_cause_unique_inpt_visits(self):
        df12u = self._calculate_all_cause_past_inpt_visits(12)
        df12u = self._extract_unique_inpt_visits(df12u)
        df12u = (df12u.groupby('member_medicaid_id')
                 .agg(inpt_u_n12=('is_inpt', 'sum'))
                 .reset_index())
        df3u = self._calculate_all_cause_past_inpt_visits(3)
        df3u = self._extract_unique_inpt_visits(df3u)
        df3u = (df3u.groupby('member_medicaid_id')
                .agg(inpt_u_n3=('is_inpt', 'sum'))
                .reset_index())
        how = 'left'
        df_final = self.member_data.merge(df12u, how=how).merge(df3u, how=how)
        for c in ['inpt_u_n12', 'inpt_u_n3']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_asthma_unique_inpt_visits(self):
        df12u_as = self._calculate_asthma_past_inpt_visits(12)
        df12u_as = self._extract_unique_inpt_visits(df12u_as, is_asthma=True)
        df12u_as = (df12u_as.groupby('member_medicaid_id')
                    .agg(inpt_as_u_n12=('is_as_inpt', 'sum'))
                    .reset_index())
        df3u_as = self._calculate_asthma_past_inpt_visits(3)
        df3u_as = self._extract_unique_inpt_visits(df3u_as, is_asthma=True)
        df3u_as = (df3u_as.groupby('member_medicaid_id')
                   .agg(inpt_as_u_n3=('is_as_inpt', 'sum'))
                   .reset_index())
        df_final = (self.member_data.merge(df12u_as, how='left')
                    .merge(df3u_as, how='left'))
        for c in ['inpt_as_u_n12', 'inpt_as_u_n3']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_inpt_visits(self):
        df12 = self.get_past_12_months_inpt_visits()
        df6 = self.get_past_6_months_inpt_visits()
        df3 = self.get_past_3_months_inpt_visits()
        dfu = self.get_all_cause_unique_inpt_visits()
        dfu_as = self.get_asthma_unique_inpt_visits()
        return (self.member_data
                .merge(df12, how='left')
                .merge(df6, how='left')
                .merge(df3, how='left')
                .merge(dfu, how='left')
                .merge(dfu_as, how='left'))


class IdentifyPastOutpatientVisits(PastVisitsBaseClass):

    def __init__(self, df):
        super().__init__(df)

    def _calculate_all_cause_past_outpatient_visits(self, months_back):
        temp = self.data[
            ['member_medicaid_id', 'outpt', 'total_paid_amt', 'dos',
             'attending_providerid']]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=months_back))))]
        temp['outpt_amt'] = temp.outpt.mul(temp.total_paid_amt)
        df = temp.groupby(['member_medicaid_id', 'dos']).agg(
            is_outpt=('outpt', 'sum'), outpt_paid_amt=('outpt_amt', 'sum'),
            attending_providerid=('attending_providerid', 'first'))
        df['is_outpt'] = (df.is_outpt > 0).astype(int)
        df.reset_index(inplace=True)
        return df

    def _calculate_asthma_past_outpatient_visits(self, months_back):
        temp = self.data[
            ['member_medicaid_id', 'outpt', 'total_paid_amt', 'dos',
             'prm_sec_as']]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=months_back))))]
        temp['outpt_as'] = temp.outpt.mul(temp.prm_sec_as)
        temp['outpt_as_amt'] = (temp.outpt.mul(temp.total_paid_amt)
                                .mul(temp.prm_sec_as))
        df = temp.groupby(['member_medicaid_id', 'dos']).agg(
            is_as_outpt=('outpt_as', 'sum'),
            outpt_as_paid_amt=('outpt_as_amt', 'sum'))
        df['is_as_outpt'] = (df.is_as_outpt > 0).astype(int)
        df.reset_index(inplace=True)
        return df

    def get_past_12_months_outpt_visits(self):
        df = self._calculate_all_cause_past_outpatient_visits(12)
        df12 = (df.loc[lambda x: x.is_outpt == 1].groupby('member_medicaid_id')
                .agg(outpt_n12=('is_outpt', 'sum'),
                     outpt_d=('dos', 'max'),
                     outpt_pd_12=('outpt_paid_amt', 'sum'),
                     attending_providerid=('attending_providerid', 'first'))
                .reset_index())
        df_as = self._calculate_asthma_past_outpatient_visits(12)
        df12_as = (df_as.loc[lambda x: x.is_as_outpt == 1]
                   .groupby('member_medicaid_id')
                   .agg(outpt_as_n12=('is_as_outpt', 'sum'),
                        outpt_as_d=('dos', 'max'),
                        outpt_as_pd_12=('outpt_as_paid_amt', 'sum'))
                   .reset_index())
        how = 'left'
        df_final = self.member_data.merge(df12, how=how).merge(df12_as, how=how)
        for c in ['outpt_n12', 'outpt_pd_12', 'outpt_as_n12', 'outpt_as_pd_12']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_6_months_outpt_visits(self):
        df = self._calculate_all_cause_past_outpatient_visits(6)
        df6 = (df.loc[lambda x: x.is_outpt == 1].groupby('member_medicaid_id')
               .agg(outpt_n6=('is_outpt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_outpatient_visits(6)
        df6_as = (df_as.loc[lambda x: x.is_as_outpt == 1]
                  .groupby('member_medicaid_id')
                  .agg(outpt_as_n6=('is_as_outpt', 'sum')).reset_index())
        how = 'left'
        df_final = self.member_data.merge(df6, how=how).merge(df6_as, how=how)
        for c in ['outpt_n6', 'outpt_as_n6']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def get_past_3_months_outpt_visits(self):
        df = self._calculate_all_cause_past_outpatient_visits(3)
        df3 = (df.loc[lambda x: x.is_outpt == 1].groupby('member_medicaid_id')
               .agg(outpt_n3=('is_outpt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_outpatient_visits(3)
        df3_as = (df_as.loc[lambda x: x.is_as_outpt == 1]
                  .groupby('member_medicaid_id')
                  .agg(outpt_as_n3=('is_as_outpt', 'sum')).reset_index())
        how = 'left'
        df_final = self.member_data.merge(df3, how=how).merge(df3_as, how=how)
        for c in ['outpt_n3', 'outpt_as_n3']:
            df_final[c].fillna(0, inplace=True)
        return df_final

    def _get_max_doc_by_member(self):
        temp = self.data[
            ['member_medicaid_id', 'outpt', 'dos', 'attending_providerid']]
        temp = temp.loc[lambda x: (
                (x.dos <= self.period) &
                (x.dos >= (self.period - relativedelta(months=24))))]
        df = temp.groupby(
            ['member_medicaid_id', 'dos', 'attending_providerid']).agg(
            is_outpt=('outpt', 'sum'))
        df.reset_index(inplace=True)
        df['is_outpt'] = (df.is_outpt > 0).astype(int)
        df = (df.loc[lambda x: x.is_outpt == 1]
              .groupby(['member_medicaid_id', 'attending_providerid'])
              .agg(outpt_freq=('is_outpt', 'sum'),
                   max_doc=('attending_providerid', 'first'))
              .reset_index()
              .sort_values(['member_medicaid_id', 'outpt_freq'],
                           ascending=[True, False])
              .drop_duplicates('member_medicaid_id', keep='first')
              [['member_medicaid_id', 'max_doc']])
        return df

    def get_past_outpt_visits(self):
        df12 = self.get_past_12_months_outpt_visits()
        df6 = self.get_past_6_months_outpt_visits()
        df3 = self.get_past_3_months_outpt_visits()
        df_max_doc = self._get_max_doc_by_member()
        return (self.member_data.merge(df12, how='left')
                .merge(df6, how='left').merge(df3, how='left')
                .merge(df_max_doc, how='left'))


class IdentifyPastVisits:

    @staticmethod
    def get_past_visits(df):
        df_ed = IdentifyPastEDVisits(df).get_past_ed_visits()
        df_inpt = IdentifyPastInpatientVisits(df).get_past_inpt_visits()
        df_outpt = IdentifyPastOutpatientVisits(df).get_past_outpt_visits()
        return df_ed.merge(df_inpt).merge(df_outpt)
