import pandas as pd
from src.blocks.base import Block


class COLUMN_CREATE_serving_size_usda_sample_rawBlock(Block):
    name = "COLUMN_CREATE_serving_size_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform new column to serving_size"
    inputs = []
    outputs = ['serving_size']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # Create new column with NaN float values
        df['serving_size'] = float('nan')
        return df