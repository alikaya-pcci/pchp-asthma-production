import os
import pandas as pd
from asthma.validate_schema import ValidateSchema
from asthma.claim_data_validation_cls import *


class ViewDataValidation:

    def __init__(self, filepath):
        if os.path.exists(filepath):
            self._filepath = filepath
        else:
            raise FileNotFoundError('No such file in the given path.')

        self._df = None
        self._column_names = None

    def _validate_schema(self):
        if self._df is None:
            print('Validating schema ...')
            validator = ValidateSchema(self._filepath)
            validator.validate_schemas()

    def _read_data_from_filepath(self):
        self._validate_schema()
        print('Reading data from the path ...')
        self._df = pd.read_parquet(self._filepath)
        self._column_names = self._df.columns
        self._df.columns = self._process_column_names()

    def _process_column_names(self):
        return [c.lower().strip().replace(' ', '_') for c in self._df.columns]

    def get_raw_data(self):
        if self._df is None:
            self._read_data_from_filepath()
        return self._df.rename(
            columns={old: new
                     for old, new in zip(self._df.columns, self._column_names)})


class ClaimViewDataValidation(ViewDataValidation):

    def __init__(self, filepath):
        super().__init__(filepath)
        self._validated = False

    def validate(self):
        if self._df is None:
            self._read_data_from_filepath()

        DiagnosisCodeValidation().validate(self._df)
        ValidateRevenueCodes().validate(self._df.revenue_code)
        ValidatePlaceOfServiceCodes().validate(self._df.place_of_service)
        self._validated = True
        print('Validation process completed!', end='\n\n')

    def get_validated_data(self):
        if not self._validated:
            self.validate()
        return self._df


class PharmacyViewDataValidation(ViewDataValidation):

    def __init__(self, filepath):
        super().__init__(filepath)
        self._validated = False

    def validate(self):
        if self._df is None:
            self._read_data_from_filepath()

        assert self._df.days_supply.isnull().sum() == 0
        assert self._df.claim_start_date.isnull().sum() == 0
        assert self._df.member_age_on_date_of_service.min() >= 0
        assert self._df.member_age_on_date_of_service.max() <= 17
        self._validated = True

    def get_validated_data(self):
        if not self._validated:
            self.validate()
        return self._df
