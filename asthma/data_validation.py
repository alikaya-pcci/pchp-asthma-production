import os
import re
import warnings
import pandas as pd
from pandas.testing import assert_index_equal
from asthma.validate_schema import ValidateSchema


class ClaimViewDataValidation:

    def __init__(self, filepath):
        if os.path.exists(filepath):
            self._filepath = filepath
        else:
            raise FileNotFoundError('No such file in the given path.')

        self._df = None
        self._validated = None

    class DiagnosisCodeValidation:
        def __init__(self, outer_class):
            self._df = outer_class._df

        def _filter_code_and_icd_columns(self, group, icd=False):
            if icd:
                r = r'(?!.*admit)(?!.*desc)(?=.*icd)(?=.*{})'.format(group)
            else:
                r = r'(?!.*admit)(?!.*desc)(?!.*icd)(?=.*{})'.format(group)
            pattern = re.compile(r)
            return [col for col in self._df.columns if pattern.match(col)]

        def _select_code_and_icd_columns(self):
            d_cols = {}
            for group in ['header_diagnosis', 'line_diagnosis', 'procedure']:
                code_cols = self._filter_code_and_icd_columns(group)
                icd_cols = self._filter_code_and_icd_columns(group, icd=True)
                d_cols[group] = {'code': code_cols, 'icd': icd_cols}
            return d_cols

        def _validate_diagnosis_and_procedure_code_columns(self):
            print('   Validating Diagnosis and Procedure codes ...')
            d_cols = self._select_code_and_icd_columns()
            for key, value in d_cols.items():
                code_cols, icd_cols = value['code'], value['icd']
                for code, icd in zip(code_cols, icd_cols):
                    code_null_idx = (self._df[code]
                                     .loc[lambda x: x.str.strip() == '']
                                     .index)
                    icd_null_idx = (self._df[icd]
                                    .loc[lambda x: x.isnull()]
                                    .index)

                    try:
                        assert_index_equal(code_null_idx, icd_null_idx)
                    except AssertionError:
                        if not set(icd_null_idx).issubset(set(code_null_idx)):
                            msg = f'{code} needs manual missing values check!'
                            raise ValueError(msg)

        def get_code_icd_columns(self):
            return self._select_code_and_icd_columns()

        def validate(self):
            self._validate_diagnosis_and_procedure_code_columns()

    class ValidateColumns:
        def __init__(self, outer_class):
            self._df = outer_class._df

        def _validate_revenue_codes(self):
            print('   Validating Revenue codes ...')
            assert self._df.revenue_code.isnull().sum() < self._df.shape[0]
            assert self._df.revenue_code.max() < 10_000
            assert self._df.revenue_code.min() > 99

        def _validate_place_of_service_codes(self):
            print('   Validating Place of Service codes ...')
            arr = self._df.place_of_service.str.strip().copy()
            arr = arr.str.replace('Not Applicable', '00')
            arr = arr.astype(int)
            assert arr.max() < 100
            assert arr.min() >= 0

        def validate(self):
            self._validate_revenue_codes()
            self._validate_place_of_service_codes()

    def _validate_schema(self):
        if self._df is None:
            print('Validating schema ...')
            validator = ValidateSchema(self._filepath)
            validator.validate_schemas()

    def _read_data_from_filepath(self):
        self._validate_schema()
        print('Reading data from the path ...')
        self._df = pd.read_parquet(self._filepath)

    def _process_column_names(self):
        return [c.lower().strip().replace(' ', '_') for c in self._df.columns]

    def validate(self):
        if self._df is None:
            self._read_data_from_filepath()

        self._df.columns = self._process_column_names()
        ClaimViewDataValidation.DiagnosisCodeValidation(self).validate()
        ClaimViewDataValidation.ValidateColumns(self).validate()
        self._validated = True
        print('Validation process completed!', end='\n\n')

    def get_raw_data(self):
        if self._df is None:
            self._read_data_from_filepath()
            return self._df
        elif self._validated is True:
            msg = 'Raw data no longer available. Use get_validated_data()'
            warnings.warn(msg)
            return None

    def get_validated_data(self):
        if not self._validated: self.validate()
        return self._df
