## Installation

To install pydb to your machine, run the following script from the root of your project's directory:

```
pip3 install pyDBMS
```

----

# PyDBMS

**Description**:  Pydb provides users with a light-weight, easy to use ORM(Object Relational Mapping) for multiple DBMS systems. The primary goal is to make communicating between databases as easy as possible using a unified base model type and abstract database interface. Pydb is designed to be easily extended to other languages as needed by the user. 


## Dependencies

When pydb is installed all libraries that will also be installed through pip. 
Pydb currently has no external dependencies until non-native databases are supported.

## Usage
### Models
Users have the ability to create their own custom models and seamlessly add them to their database of choice.

**example_model.py**
```py
from pyDBMS import Model, Text, Float, Integer
class ExampleModel(Model):
  __table_name__ = 'example_models'
  __primary_keys__ = 'model_id' # alternatively ['model_id']

  model_id = Text()
  other_column = Integer()
  another_column = Float()
  
```
### Selecting Model Entries From The Database
Users may select entries from the database. They can use filters as seen below:
```py
db = SQLiteDB('example.db')

results1 = db.select(ExampleModel, model_id='uuid(1)')
or
results2 = db.select(ExampleModel, other_column=[100,200], another_column=2.0)
```
or they can select all the models in the database of the given type with:
```py
db = SQLiteDB('example.db')

all_example_models = db.select(ExamplModel)
```
### Inserting And Updating Entries
Users can insert and update model entries across multiple databases with a uniform interface as seen below
```py
local_db = SQLiteDB('local.db')
remote_db = PostgreSQLDB(host='http://db.example.com/',username='admin',password='password123')

# ************************
# process creating models
# ************************
model1 = ExampleModel(model_id='test_id',other_column=100)

local_db.insert(model1)
# same model can be used to upload to postgres DB
remote_db.insert(model1)

model1['another_column'] = 3.14

#update both db's
local_db.update(model1)
remote_db.update(model1)


```

### Query Existing Databases Info
```py
db = SQLiteDB('/location/for/database')

# list tables and columns

for table in db.tables():
  print(table + ':')
  for column in db.get_columns(table):
    print(column)
```

## How to test the software

Pydb is developed using Test-Driven Development. All the unittests can be run using the following command in the root directory:
```bash
python3 -m unittest discover
```

## Known issues

Pydb currently only supports the Sqlite database as the requirements are being elicited.

## Getting help

If you have questions, concerns, bug reports, etc, please file an issue in this repository's Issue Tracker.


## Getting involved

If you are interested in contributing fixes or features to MonoGame, please read our [CONTRIBUTOR](CONTRIBUTING.md) guide first.
