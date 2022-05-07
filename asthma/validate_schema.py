import os
import pandas as pd
import pyarrow.parquet as pq
from pandas.testing import assert_frame_equal


class ValidateSchema:

    def __init__(self, filepath):
        if os.path.exists(filepath):
            self._filepath = filepath
        else:
            raise FileNotFoundError('No such file found in the given path.')

    def _read_data_schema(self):
        ext = os.path.splitext(self._filepath)[-1]
        if ext == '.parquet':
            schema = pq.read_schema(self._filepath, memory_map=True)
            df_schema = pd.DataFrame(
                [{'column_name': name, 'data_type': dtype}
                 for name, dtype in zip(schema.names, schema.types)])
            df_schema['data_type'] = df_schema.data_type.astype(str)
            return df_schema
        else:
            raise TypeError('The data has to be in the .parquet format.')

    @staticmethod
    def _read_claim_default_schema():
        path = ('/home/{}/T-Drive/PCHPAsthma/Data/'
                'MSSQL_Data/schema_claims_view_20220406.json'
                .format(os.environ['USER']))
        return pd.read_json(path)

    @staticmethod
    def _read_pharmacy_default_schema():
        path = ('/home/{}/T-Drive/PCHPAsthma/Data/'
                'MSSQL_Data/schema_pharmacy_view_20220406.json'
                .format(os.environ['USER']))
        return pd.read_json(path)

    def validate_schemas(self):
        if 'claim' in self._filepath.lower():
            default = self._read_claim_default_schema()
        elif 'pharma' in self._filepath.lower():
            default = self._read_pharmacy_default_schema()
        else:
            default = None

        data = self._read_data_schema()
        assert_frame_equal(default, data)
