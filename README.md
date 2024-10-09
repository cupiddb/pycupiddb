# CupidDB Python Client

## Run an instance of CupidDB
```bash
docker run --rm -p 5995:5995 wiamsuri/cupiddb:v0.4.0
```

## Installation
```bash
pip install pycupiddb
```

## Basic Usage
```python
from pycupiddb import CupidClient, RowFilter
import pandas as pd

df = pd.DataFrame({'a': [1, 2, 3], 'b': [4, 5, 6]})

cupid = CupidClient(host='localhost', port=5995)

cupid.set(key='key', value=df)

df = cupid.get_dataframe(key='key')

filters = [
    RowFilter(column='b', logic='gte', value=5, data_type='int'),
]
df = cupid.get_dataframe(
    key='key',
    columns=['a'],
    filters=filters,
)
```
