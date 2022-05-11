import re
import numpy as np
import pandas as pd
from pandas.testing import assert_series_equal
from asthma.codebook import *


class IdentifyAsthmaRelatedClaims:

    @staticmethod
    def _process_diagnosis_code_column(df, col):
        arr = df[col].str.strip().copy()
        arr = arr.str.upper()
        if arr.str.contains(' ').sum() > 0:
            arr = arr.str.split().str.join('')

        idx = (arr
               .loc[lambda x: x.str.contains('\\.') == False]
               .loc[lambda x: x.str.len() > 3]
               .index)
        if idx.shape[0]:
            arr[idx] = arr[idx].map(lambda x: x[:3] + '.' + x[3:]).values

        idx = arr.loc[lambda x: x.str.strip() == ''].index
        if idx.shape[0]:
            arr[idx] = np.nan
        return arr

    @staticmethod
    def _identify_asthma_claims(df, code_col):
        icd_col = f'{code_col}_icd_vers'
        arr = np.where(
            df[code_col].isin(ASTHMA_ICD_10_CM_CODES), 1, 0)
        arr = pd.Series(arr)
        idx = (df
               .loc[lambda x: x[icd_col] == 9]
               .loc[lambda x: (x[code_col]
                               .str.startswith(ASTHMA_ICD_9_MIN_THRESH))]
               .index)
        if idx.shape[0]:
            arr[idx] = 1
        return arr.values

    @staticmethod
    def _get_diagnosis_columns(df):
        r = r'(?!.*admit)(?!.*desc)(?!.*icd)claim_(header|line)_diagnosis'
        pattern = re.compile(r)
        return [col for col in df.columns if pattern.match(col)]

    def extract_asthma_flags(self, df):
        if not isinstance(df, pd.core.frame.DataFrame):
            raise TypeError('Only pandas data frames.')

        diagnosis_cols = self._get_diagnosis_columns(df)
        print('Extracting asthma flags ...')
        print('   Processing Diagnosis codes ...')
        for col in diagnosis_cols:
            df[col] = self._process_diagnosis_code_column(df, col)

        print('   Identifying asthma-related claims ...')
        df['prm_as'] = 0
        df['prm_sec_as'] = 0
        for col in diagnosis_cols:
            arr = self._identify_asthma_claims(df, col)
            idx = np.where(arr == 1)
            if 'primary' in col:
                df['prm_as'].loc[idx] = 1
            df['prm_sec_as'].loc[idx] = 1

        idx_as = df.loc[lambda x: x.prm_as == 1].claimid.unique()
        df.loc[lambda x: x.claimid.isin(idx_as), 'prm_as'] = 1

        idx_sec_as = df.loc[lambda x: x.prm_sec_as == 1].claimid.unique()
        df.loc[lambda x: x.claimid.isin(idx_sec_as), 'prm_sec_as'] = 1


class IdentifyVisitTypes:

    @staticmethod
    def _process_pos_codes(df):
        arr = df.place_of_service.str.strip().copy()
        arr = arr.str.replace('Not Applicable', '00')
        arr = arr.astype(int)
        idx_null = arr.loc[lambda x: x == 0].index
        arr.loc[idx_null] = np.nan
        return arr

    @staticmethod
    def _generate_visit_ids(df):
        return (df[['member_medicaid_id', 'dos_from']]
                .assign(MEMBER_ID=lambda x: x.member_medicaid_id.astype(str),
                        dos=lambda x: x.dos_from.map(
                            lambda row: row.strftime('%Y%m%d')))
                [['MEMBER_ID', 'dos']]
                .apply(lambda x: '-'.join(x.values), axis=1))

    @staticmethod
    def _identify_ed_rev_codes(df):
        return np.where(df.revenue_code.isin(ED_REV_CODES), 1, 0)

    @staticmethod
    def _identify_inpt_rev_codes(df):
        return np.where(df.revenue_code.isin(INPT_REV_CODES), 1, 0)

    @staticmethod
    def _identify_outpt_rev_codes(df):
        return np.where(df.revenue_code.isin(OUTPT_REV_CODES), 1, 0)

    @staticmethod
    def _identify_ed_pos_codes(df):
        return np.where(df.place_of_service.isin(ED_POS_CODES), 1, 0)

    @staticmethod
    def _identify_inpt_pos_codes(df):
        return np.where(df.place_of_service.isin(INPT_POS_CODES), 1, 0)

    @staticmethod
    def _identify_outpt_pos_codes(df):
        return np.where(df.place_of_service.isin(OUTPT_POS_CODES), 1, 0)

    def _identify_inpatient_visits(self, df):
        arr_rev = self._identify_inpt_rev_codes(df)
        arr_pos = self._identify_inpt_pos_codes(df)
        arr_pos[np.where(arr_rev == 1)] = 1
        return arr_pos

    def _identify_ed_visits(self, df):
        arr_rev = self._identify_ed_rev_codes(df)
        arr_pos = self._identify_ed_pos_codes(df)
        arr_pos[np.where(arr_rev == 1)] = 1
        return arr_pos

    def _identify_outpatient_visits(self, df):
        arr_rev = self._identify_outpt_rev_codes(df)
        arr_pos = self._identify_outpt_pos_codes(df)
        arr_pos[np.where(arr_rev == 1)] = 1
        return arr_pos

    def extract_visit_types(self, df):
        print('Identifying visit types ...')
        df['place_of_service'] = self._process_pos_codes(df)
        df['visitID'] = self._generate_visit_ids(df)

        # identify cases in the order of inpatient-ed-outpatient
        df['inpt'] = self._identify_inpatient_visits(df)
        inpt_visit_ids = df.loc[lambda x: x.inpt == 1].visitID.unique()
        df.loc[lambda x: x.visitID.isin(inpt_visit_ids), 'inpt'] = 1
        print('   Inpatient visits extracted...')

        df['ED'] = 0
        arr_ED = np.where(self._identify_ed_visits(df) == 1)
        ed_visit_ids = df.loc[arr_ED].visitID.unique()
        ed_visit_ids = list(set(ed_visit_ids).difference(set(inpt_visit_ids)))
        df.loc[lambda x: x.visitID.isin(ed_visit_ids), 'ED'] = 1
        print('   ED visits extracted...')

        arr_outpt = np.where(self._identify_outpatient_visits(df) == 1)
        outpt_visit_ids = df.loc[arr_outpt].visitID.unique()
        outpt_visit_ids = set(outpt_visit_ids).difference(set(ed_visit_ids))
        outpt_visit_ids = list(outpt_visit_ids.difference(set(inpt_visit_ids)))
        df.loc[lambda x: x.visitID.isin(outpt_visit_ids), 'outpt'] = 1
        print('   Outpatient visits extracted...')

        assert set(inpt_visit_ids).intersection(set(ed_visit_ids)) == set()
        assert set(inpt_visit_ids).intersection(set(outpt_visit_ids)) == set()
        assert set(ed_visit_ids).intersection(set(outpt_visit_ids)) == set()


class ProcessMemberMedicaidIDs:

    def __init__(self):
        self._multiple_medicaid_ids = None

    @staticmethod
    def _process_member_medicaid_ids(df):
        return df.member_medicaid_id.str.strip()

    @staticmethod
    def _identify_alphanumeric_ids(df):
        idx_alpha = (df.member_medicaid_id.loc[lambda x: ~x.str.isnumeric()]
                     .unique())
        if idx_alpha.shape[0]:
            print('   {:,} out of {:,} members have alphanumeric Medicaid IDs '
                  '(associates with {:,} out of {:,} records).'
                  .format(idx_alpha.shape[0],
                          df.member_medicaid_id.unique().shape[0],
                          (df
                           .loc[lambda x: x.member_medicaid_id.isin(idx_alpha)]
                           .shape[0]),
                          df.shape[0]))

    def _check_multiple_medicaid_ids(self, df):
        print('Checking if multiple Medicaid IDs exist...')
        left = df.member_medicaid_id.value_counts()
        right = (df[['member_medicaid_id', 'claimid']]
                 .sort_values(['claimid', 'member_medicaid_id'])
                 .drop_duplicates('claimid')
                 .member_medicaid_id
                 .value_counts())

        try:
            assert_series_equal(left, right)
        except AssertionError:
            diff = set(left.index).difference(set(right.index))
            claims = (df.loc[lambda x: x.member_medicaid_id.isin(list(diff))]
                      .claimid.unique())
            idxs = (df.loc[lambda x: x.claimid.isin(claims)]
                    .member_medicaid_id
                    .unique())
            print('   There are multiple Medicaid IDs for some members, which '
                  'affects {:,} members and {:,} claims.'
                  .format(len(diff),
                          (df.loc[lambda x: x.member_medicaid_id.isin(idxs)]
                          .shape[0])))

            l = []
            for idx in idxs:
                d = {}
                name = (df.loc[lambda x: x.member_medicaid_id == idx,
                               ['member_first_name', 'member_last_name']]
                        .drop_duplicates()
                        .apply(lambda _df: ' '.join(_df.values), axis=1)
                        .values[0])
                d['idx'] = idx
                d['name'] = name.strip()
                l.append(d)
            self._multiple_medicaid_ids = pd.DataFrame(l)

    def process_medicaid_ids(self, df):
        print('Checking Member Medicaid IDs ...')
        df['member_medicaid_id'] = self._process_member_medicaid_ids(df)
        self._identify_alphanumeric_ids(df)
        self._check_multiple_medicaid_ids(df)

    def get_members_with_multiple_ids(self):
        return self._multiple_medicaid_ids
