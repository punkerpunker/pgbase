## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes. See deployment for notes on how to deploy the project on a live system.

## Installing

Installation could be as simple as:
```bash
sudo pip3 install base
```
To verify installation try:
```python
import mlbase
```

## Example Usage

```python
from pgbase.database.db import DB
# - создаем объект класса DB
db = DB(user='<username>')

# - выполняем команду с помощью курсора
db.cur.execute('alter table schema_name.table_name add column columnname float')
# - комитим изменения
db.commit()

# - селектим таблицу в pandas.DataFrame
import pandas as pd
df = pd.read_sql('select zalupa from schema_name.table_name', db.engine)
```

## Contributing

Please read CONTRIBUTING for details on our code of conduct, and the process for submitting pull requests to us.

## Authors

Gleb Vazhenin - Initial work - punkerpunker

## License

This project doesn't have any license yet.



