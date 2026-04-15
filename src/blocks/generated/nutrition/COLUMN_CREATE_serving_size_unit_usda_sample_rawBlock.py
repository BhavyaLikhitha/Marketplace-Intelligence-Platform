import pandas as pd
from src.blocks.base import Block


class COLUMN_CREATE_serving_size_unit_usda_sample_rawBlock(Block):
    name = "COLUMN_CREATE_serving_size_unit_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform new column to serving_size_unit"
    inputs = []
    outputs = ['serving_size_unit']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # Create new string column with null values
        df['serving_size_unit'] = None
        df['serving_size_unit'] = df['serving_size_unit'].astype(str)
        return df