import re
import warnings
from pandas.testing import assert_index_equal


class DiagnosisCodeValidation:

    def __init__(self):
        self._validated = False

    @staticmethod
    def _filter_code_and_icd_columns(df, group, icd=False):
        if icd:
            r = r'(?!.*admit)(?!.*desc)(?=.*icd)(?=.*{})'.format(group)
        else:
            r = r'(?!.*admit)(?!.*desc)(?!.*icd)(?=.*{})'.format(group)
        pattern = re.compile(r)
        return [col for col in df.columns if pattern.match(col)]

    def _select_code_and_icd_columns(self, df):
        d_cols = {}
        for group in ['header_diagnosis', 'line_diagnosis', 'procedure']:
            code_cols = self._filter_code_and_icd_columns(df, group)
            icd_cols = self._filter_code_and_icd_columns(df, group, icd=True)
            d_cols[group] = {'code': code_cols, 'icd': icd_cols}
        self.code_icd_columns = d_cols
        return d_cols

    def _validate_diagnosis_and_procedure_code_columns(self, df):
        d_cols = self._select_code_and_icd_columns(df)
        for key, value in d_cols.items():
            code_cols, icd_cols = value['code'], value['icd']
            for code, icd in zip(code_cols, icd_cols):
                code_null_idx = (df[code].loc[lambda x: x.str.strip() == '']
                                 .index)
                icd_null_idx = df[icd].loc[lambda x: x.isnull()].index

                try:
                    assert_index_equal(code_null_idx, icd_null_idx)
                except AssertionError:
                    if not set(icd_null_idx).issubset(set(code_null_idx)):
                        msg = f'{code} needs manual missing values check!'
                        raise ValueError(msg)

    def validate(self, df):
        print('   Validating Diagnosis and Procedure Codes ...', end=' ')
        if self._validated is False:
            self._validate_diagnosis_and_procedure_code_columns(df)
            self._validated = True
        print('Done!')


class ValidateRevenueCodes:

    @staticmethod
    def _validate_revenue_codes(revenue_codes):
        assert revenue_codes.isnull().sum() < revenue_codes.shape[0]
        assert revenue_codes.max() < 10_000
        assert revenue_codes.min() > 99

    def validate(self, revenue_codes):
        print('   Validating Revenue Codes ...', end=' ')
        self._validate_revenue_codes(revenue_codes)
        print('Done!')


class ValidatePlaceOfServiceCodes:

    @staticmethod
    def _validate_place_of_service_codes(place_of_service_codes):
        arr = place_of_service_codes.str.strip().copy()
        arr = arr.str.replace('Not Applicable', '00')
        arr = arr.astype(int)
        assert arr.loc[lambda x: x == 0].shape[0] < arr.shape[0]
        assert arr.max() < 100
        assert arr.min() >= 0

    def validate(self, place_of_service_codes):
        print('   Validating Place of Service Codes ...', end=' ')
        self._validate_place_of_service_codes(place_of_service_codes)
        print('Done!')

























