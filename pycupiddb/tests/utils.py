import random
from datetime import date, timedelta
import pandas as pd
import numpy as np


def create_df(rows: int = 10, columns: int = 10) -> pd.DataFrame:
    start_date = date(2000, 1, 1)
    df = pd.DataFrame([
        {
            f'c{i}': random.random() if i % 2 == 0 else np.nan
            for i in range(columns)
        }
        for _ in range(rows)
    ], index=[
        start_date + timedelta(days=i)
        for i in range(rows)
    ])
    return df
