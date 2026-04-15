import pandas as pd
from src.blocks.base import Block


class COLUMN_CREATE_category_usda_sample_rawBlock(Block):
    name = "COLUMN_CREATE_category_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform new column to category"
    inputs = []
    outputs = ['category']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # Create new string column with null values
        df['category'] = None
        df['category'] = df['category'].astype(str)
        return df