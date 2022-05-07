import pandas as pd
import numpy as np
from collections import ChainMap
from pandas.testing import assert_series_equal
from asthma.data_validation import ClaimViewDataValidation
from asthma.codebook import *


class ClaimViewDataProcessing:

    def __init__(self, filepath):
        self.filepath = filepath
        self._data_validation = None
        self._df = None

    class IdentifyAsthmaRelatedClaims:
        def __init__(self, outer_class):
            self._df = outer_class._df

        def _process_diagnosis_code_column(self, col):
            arr = self._df[col].str.strip().copy()
            arr = arr.str.upper()
            if arr.str.contains(' ').sum() > 0:
                arr = arr.str.split().str.join('')

            idx = (arr
                   .loc[lambda x: x.str.contains('\\.') == False]
                   .loc[lambda x: x.str.len() > 3]
                   .index)
            if len(idx):
                arr[idx] = arr[idx].map(lambda x: x[:3] + '.' + x[3:]).values
            idx = arr.loc[lambda x: x.str.strip() == ''].index
            if len(idx):
                arr[idx] = np.nan
            return arr

        def _identify_asthma_claims(self, code_col, icd_col):
            arr = np.where(
                self._df[code_col].isin(ASTHMA_ICD_10_CM_CODES), 1, 0)
            arr = pd.Series(arr)
            idx = (self._df
                   .loc[lambda x: x[icd_col] == 9]
                   .loc[lambda x: (x[code_col]
                                   .str.startswith(ASTHMA_ICD_9_MIN_THRESH))]
                   .index)
            if len(idx):
                arr[idx] = 1
            return arr.values

        def _extract_asthma_flags(self):
            print('Extracting asthma flags ...')
            diag_prod_cols = self._data_validation.get_code_icd_vers_columns()
            print('   Processing Diagnosis codes ...')
            for group in ['header_diagnosis', 'line_diagnosis']:
                for col in diag_prod_cols[group]['code']:
                    self._df[col] = self._process_diagnosis_code_column(col)

            print('   Identifying asthma-related claims ...')
            for code_col, icd_col in zip(
                    diag_prod_cols['header_diagnosis']['code'],
                    diag_prod_cols['header_diagnosis']['icd']):
                if code_col == 'claim_header_diagnosis_code_primary':
                    self._df['prm_as'] = self._identify_asthma_claims(code_col,
                                                                      icd_col)
                    self._df['prm_sec_as'] = self._identify_asthma_claims(
                        code_col,
                        icd_col)
                else:
                    arr = self._identify_asthma_claims(code_col, icd_col)
                    idx = np.where(arr == 1)
                    self._df.prm_sec_as.loc[idx] = 1

            idx_as = self._df.loc[lambda x: x.prm_as == 1].claimid.unique()
            self._df.loc[lambda x: x.claimid.isin(idx_as), 'prm_as'] = 1

            idx_sec_as = self._df.loc[
                lambda x: x.prm_sec_as == 1].claimid.unique()
            self._df.loc[lambda x: x.claimid.isin(idx_sec_as), 'prm_sec_as'] = 1
            print('Done!!!', end='\n\n')
    def _get_validated_data(self):
        if self._df is None:
            self._data_validation = ClaimViewDataValidation(self.filepath)
            self._df = self._data_validation.get_validated_data()

    def _process_member_medicaid_ids(self):
        # drop all alphanumeric member ids
        self._df['member_medicaid_id'] = self._df.member_medicaid_id.str.strip()
        idx_alpha = (self._df.member_medicaid_id
                     .loc[lambda x: ~x.str.isnumeric()]
                     .unique())
        print('{:,} out of {:,} members have alphanumeric Medicaid IDs '
              '(associates with {:,} out of {:,} records).'
              .format(idx_alpha.shape[0],
                      self._df.member_medicaid_id.unique().shape[0],
                      (self._df
                          .loc[lambda x: x.member_medicaid_id.isin(idx_alpha)]
                          .shape[0]),
                      self._df.shape[0]))
        self._df = (self._df
                    .loc[lambda x: x.member_medicaid_id.str.isnumeric()]
                    .copy())
        self._df['member_medicaid_id'] = self._df.member_medicaid_id.astype(int)
        self._df.reset_index(drop=True, inplace=True)
        print('\tAlphanumeric Medicaid IDs are dropped.')
        print('Done!', end='\n\n')

    def _check_multiple_medicaid_ids(self):
        print('Checking if multiple Medicaid IDs exist...')
        left = self._df.member_medicaid_id.value_counts()
        right = (self._df[['member_medicaid_id', 'claimid']]
                 .sort_values(['claimid', 'member_medicaid_id'])
                 .drop_duplicates('claimid')
                 .member_medicaid_id
                 .value_counts())

        try:
            assert_series_equal(left, right)
        except AssertionError:
            diff = set(left.index).difference(set(right.index))
            claims = (self._df
                      .loc[lambda x: x.member_medicaid_id.isin(list(diff))]
                      .claimid.unique())
            idxs = (self._df
                    .loc[lambda x: x.claimid.isin(claims)]
                    .member_medicaid_id
                    .unique())
            print('\tThere are multiple Medicaid IDs for some members, which '
                  'affects {:,} members and {:,} claims.'
                  .format(len(diff),
                          (self._df
                              .loc[lambda x: x.member_medicaid_id.isin(idxs)]
                              .shape[0])))

            l = []
            for idx in idxs:
                d = {}
                name = (self._df
                    .loc[lambda x: x.member_medicaid_id == idx,
                         ['member_first_name', 'member_last_name']]
                    .drop_duplicates()
                    .apply(lambda _df: ' '.join(_df.values), axis=1)
                    .values[0])
                d['idx'] = idx
                d['name'] = name.strip()
                l.append(d)

            df = pd.DataFrame(l)
            df = df.groupby('name')['idx'].apply(list).reset_index().copy()
            dicts = [(*df.idx
                      .map(lambda _l: sorted(_l, reverse=True))
                      .map(lambda _l: [{_l[0]: key} for key in _l[1:]])
                      .values)]
            dicts = dict(ChainMap(*[item for ls in dicts for item in ls]))
            mappings = [(value, key) for key, value in dicts.items()]
            arr = self._df.member_medicaid_id.copy()
            arr.replace(to_replace=[item[0] for item in mappings],
                        value=[item[1] for item in mappings],
                        inplace=True)

            num_new_idx = arr.unique().shape[0]
            num_old_idx = self._df.member_medicaid_id.unique().shape[0]
            try:
                assert num_new_idx < num_old_idx
                self._df['member_medicaid_id'] = arr.values
                print('\tMultiple Medicaid IDs reduced to one unique ID.')
            except AssertionError:
                print('\tMapping multiple Medicaid IDs terminated.')
        print('Done!', end='\n\n')



    def _identify_ed_rev_codes(self):
        return np.where(self._df.revenue_code.isin(ED_REV_CODES), 1, 0)

    def _identify_inpt_rev_codes(self):
        return np.where(self._df.revenue_code.isin(INPT_REV_CODES), 1, 0)

    def _identify_outpt_rev_codes(self):
        return np.where(self._df.revenue_code.isin(OUTPT_REV_CODES), 1, 0)

    def _process_pos_codes(self):
        arr = self._df.place_of_service.str.strip().copy()
        arr = arr.str.replace('Not Applicable', '00')
        arr = arr.astype(int)
        idx_null = arr.loc[lambda x: x == 0].index
        arr.loc[idx_null] = np.nan
        return arr

    def _generate_visit_ids(self):
        return (self._df[['member_medicaid_id', 'dos_from']]
                .assign(MEMBER_ID=lambda df: df.member_medicaid_id.astype(str),
                        dos=lambda df: df.dos_from.map(
                            lambda row: row.strftime('%Y%m%d')))
                [['MEMBER_ID', 'dos']]
                .apply(lambda df: '-'.join(df.values), axis=1))

    def _identify_ed_pos_codes(self):
        return np.where(self._df.place_of_service.isin(ED_POS_CODES), 1, 0)

    def _identify_inpt_pos_codes(self):
        return np.where(self._df.place_of_service.isin(INPT_POS_CODES), 1, 0)

    def _identify_outpt_pos_codes(self):
        return np.where(self._df.place_of_service.isin(OUTPT_POS_CODES), 1, 0)

    def _identify_inpatient_visits(self):
        arr_rev = self._identify_inpt_rev_codes()
        arr_pos = self._identify_inpt_pos_codes()
        arr_pos[np.where(arr_rev == 1)] = 1
        return arr_pos

    def _identify_ed_visits(self):
        arr_rev = self._identify_ed_rev_codes()
        arr_pos = self._identify_ed_pos_codes()
        arr_pos[np.where(arr_rev == 1)] = 1
        return arr_pos

    def _identify_outpatient_visits(self):
        arr_rev = self._identify_outpt_rev_codes()
        arr_pos = self._identify_outpt_pos_codes()
        arr_pos[np.where(arr_rev == 1)] = 1
        return arr_pos

    def _extract_visit_types(self):
        print('Identifying visit types ...')
        self._df['place_of_service'] = self._process_pos_codes()
        self._df['visitID'] = self._generate_visit_ids()

        # identify cases in the order of inpatient-ed-outpatient
        self._df['inpt'] = self._identify_inpatient_visits()
        inpt_visit_ids = self._df.loc[lambda x: x.inpt == 1].visitID.unique()
        idx = self._df.loc[lambda x: x.visitID.isin(inpt_visit_ids)].index
        self._df.loc[idx, 'inpt'] = 1
        print('   Inpatient visits extracted...')

        self._df['ED'] = self._identify_ed_visits()
        ed_visit_ids = self._df.loc[lambda x: x.ED == 1].visitID.unique()
        ed_visit_ids = list(set(ed_visit_ids).difference(set(inpt_visit_ids)))
        idx = self._df.loc[lambda x: x.visitID.isin(ed_visit_ids)].index
        self._df.loc[idx, 'ED'] = 1
        print('   ED visits extracted...')

        self._df['outpt'] = self._identify_outpatient_visits()
        outpt_visit_ids = self._df.loc[lambda x: x.outpt == 1].visitID.unique()
        outpt_visit_ids = set(outpt_visit_ids).difference(set(ed_visit_ids))
        outpt_visit_ids = list(outpt_visit_ids.difference(set(inpt_visit_ids)))
        idx = self._df.loc[lambda x: x.visitID.isin(outpt_visit_ids)].index
        self._df.loc[idx, 'outpt'] = 1
        print('   Outpatient visits extracted...')

        assert set(inpt_visit_ids).intersection(set(ed_visit_ids)) == set()
        assert set(inpt_visit_ids).intersection(set(outpt_visit_ids)) == set()
        assert set(ed_visit_ids).intersection(set(outpt_visit_ids)) == set()
        print('Done!!!', end='\n\n')

    def set_data(self, df):
        self._df = df

    def get_processed_data(self):
        if self._df is None:
            self._get_validated_data()

        self._process_member_medicaid_ids()
        self._check_multiple_medicaid_ids()
        self._extract_asthma_flags()
        self._extract_visit_types()
        return self._df
