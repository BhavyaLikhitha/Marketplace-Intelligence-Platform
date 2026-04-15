import pandas as pd
from src.blocks.base import Block


class COLUMN_CREATE_ingredients_usda_sample_rawBlock(Block):
    name = "COLUMN_CREATE_ingredients_usda_sample_raw"
    domain = "nutrition"
    description = "Auto-generated: Transform new column to ingredients"
    inputs = []
    outputs = ['ingredients']
    
    def run(self, df: pd.DataFrame, config: dict | None = None) -> pd.DataFrame:
        df = df.copy()
        # Create new string column with null values
        df['ingredients'] = None
        df['ingredients'] = df['ingredients'].astype(str)
        return df