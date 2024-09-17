import random
from datetime import date, datetime, timedelta
import pandas as pd
import numpy as np


def create_df(rows: int = 10, columns: int = 10, index_type: str = 'date') -> pd.DataFrame:
    if index_type == 'date':
        start_date = date(2000, 1, 1)
    else:
        start_date = datetime(2000, 1, 1)
    def column_value(col: int):
        if col % 2 == 0:
            return random.random()
        if col % 3 == 0:
            return int(random.random() * 10)
        return np.nan
    df = pd.DataFrame([
        {
            f'c{i}': column_value(i)
            for i in range(columns)
        }
        for _ in range(rows)
    ], index=[
        start_date + timedelta(days=i)
        for i in range(rows)
    ])
    return df
