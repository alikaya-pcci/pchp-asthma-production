from datetime import datetime
from dateutil.relativedelta import relativedelta


class IdentifyComorbidities:
    pass


class IdentifyPastVisits:

    def __init__(self, df):
        columns = ['member_medicaid_id', 'ED', 'inpt', 'outpt', 'dos_from',
                   'visitID', 'total_paid_amt', 'claimid', 'prm_as',
                   'prm_sec_as', 'attending_providerid']
        for column in columns:
            if column not in df.columns:
                msg = f'Past visits cannot be calculated without "{column}".'
                raise KeyError(msg)

        self.data = df[columns].copy()
        self.data['dos'] = self.data.dos_from.map(
            lambda x: datetime.strptime(x.strftime('%Y-%m-%d'), '%Y-%m-%d'))
        self.data.drop('dos_from', axis=1, inplace=True)
        self.data.drop_duplicates(inplace=True)
        self.period = self.data.dos.max()
        self.member_data = (self.data.member_medicaid_id.drop_duplicates()
                            .sort_values(ignore_index=True).to_frame())


class IdentifyPastEDVisits(IdentifyPastVisits):

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
        return self.member_data.merge(df12, how=how).merge(df12_as, how=how)

    def get_past_6_months_ed_visits(self):
        df = self._calculate_all_cause_past_ed_visits(6)
        df6 = df.loc[lambda x: x.is_ED == 1].groupby('member_medicaid_id').agg(
            ED_n6=('is_ED', 'sum')).reset_index()
        df_as = self._calculate_asthma_past_ed_visits(6)
        df6_as = (df_as.loc[lambda x: x.is_as_ED == 1]
                  .groupby('member_medicaid_id')
                  .agg(ED_as_n6=('is_as_ED', 'sum')).reset_index())
        how = 'left'
        return self.member_data.merge(df6, how=how).merge(df6_as, how=how)

    def get_past_3_months_ed_visits(self):
        df = self._calculate_all_cause_past_ed_visits(3)
        df3 = df.loc[lambda x: x.is_ED == 1].groupby('member_medicaid_id').agg(
            ED_n3=('is_ED', 'sum')).reset_index()
        df_as = self._calculate_asthma_past_ed_visits(3)
        df3_as = (df_as.loc[lambda x: x.is_as_ED == 1]
                  .groupby('member_medicaid_id')
                  .agg(ED_as_n3=('is_as_ED', 'sum')).reset_index())
        how = 'left'
        return self.member_data.merge(df3, how=how).merge(df3_as, how=how)

    def get_past_ed_visits(self):
        df12 = self.get_past_12_months_ed_visits()
        df6 = self.get_past_6_months_ed_visits()
        df3 = self.get_past_3_months_ed_visits()
        return (self.member_data.merge(df12, how='left')
                .merge(df6, how='left').merge(df3, how='left'))


class IdentifyPastInpatientVisits(IdentifyPastVisits):

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
        return self.member_data.merge(df12, how=how).merge(df12_as, how=how)

    def get_past_6_months_inpt_visits(self):
        df = self._calculate_all_cause_past_inpt_visits(6)
        df6 = (df.loc[lambda x: x.is_inpt == 1].groupby('member_medicaid_id')
               .agg(inpt_n6=('is_inpt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_inpt_visits(6)
        df6_as = (df_as.loc[lambda x: x.is_as_inpt == 1]
                  .groupby('member_medicaid_id')
                  .agg(inpt_as_n6=('is_as_inpt', 'sum')).reset_index())
        how = 'left'
        return self.member_data.merge(df6, how=how).merge(df6_as, how=how)

    def get_past_3_months_inpt_visits(self):
        df = self._calculate_all_cause_past_inpt_visits(3)
        df3 = (df.loc[lambda x: x.is_inpt == 1].groupby('member_medicaid_id')
               .agg(inpt_n3=('is_inpt', 'sum')).reset_index())
        df_as = self._calculate_asthma_past_inpt_visits(3)
        df3_as = (df_as.loc[lambda x: x.is_as_inpt == 1]
                  .groupby('member_medicaid_id')
                  .agg(inpt_as_n3=('is_as_inpt', 'sum')).reset_index())
        how = 'left'
        return self.member_data.merge(df3, how=how).merge(df3_as, how=how)

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
        return self.member_data.merge(df12u, how='left').merge(df3u, how='left')

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
        how = 'left'
        return self.member_data.merge(df12u_as, how=how).merge(df3u_as, how=how)

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


