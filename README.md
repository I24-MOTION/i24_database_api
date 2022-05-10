

# Custom I-24 Database API package
#### Version: 0.1.1
#### Date revised: 05/10/2022

### Installation
With the desired python venv / conda env activated, use the following command in shell:

`pip install git+https://github.com/yanb514/i24_database_api@<tag>`

where `<tag>` is either a branch name (e.g. `master`) or a tag name (e.g. `v0.1`)
    
Then, to import the data reader or writer object and establish a connection:

```
from i24_database_api.db_reader import DBReader
from i24_database_api.db_writer import DBWriter
dbr = DBReader(host=host, port=port, username=username, password=password,
                database_name=database_name, collection_name=collection_name)
```


### Requirements
- pymongo


## Usage examples

### Use case 1: Range query

The following code demonstrates the use of the iterative query based on a query parameter. 

```python
rri = dbr.read_query_range(range_parameter='last_timestamp', range_greater_equal=300, range_less_than=330, range_increment=None)
while True:
    try:
        print(next(rri)["ID"]) # access documents in rri one by one
    except StopIteration:
        print("END OF ITERATION")
        break

print("Using for-loop to read range")
for result in dbr.read_query_range(range_parameter='last_timestamp', range_greater_equal=300, range_less_than=330, range_increment=None):
    print(result["ID"])
print("END OF ITERATION")
```

The console log output of the code is the following. Notice the PID in the `extra` field of each message remains the same, even for the imported function.
```
last timestamp: 304.17, starting_x: 32806.20, ID: 3600083.0
last timestamp: 306.00, starting_x: 32771.59, ID: 3600084.0
last timestamp: 310.90, starting_x: 32533.66, ID: 3600086.0
last timestamp: 312.73, starting_x: 32805.35, ID: 400088.0
last timestamp: 313.23, starting_x: 31897.72, ID: 3600087.0
last timestamp: 316.53, starting_x: 31594.89, ID: 3600088.0
last timestamp: 324.50, starting_x: 31166.60, ID: 3600089.0
last timestamp: 325.07, starting_x: 32076.31, ID: 400089.0
last timestamp: 328.93, starting_x: 30132.66, ID: 3600090.0
```


### Use case 2: Concurrent writing with multithreading
When bulk write to database, this package offers the choice to do non-blocking (concurrent) insert:

```python
dbw = DBWriter(host=host, port=port, username=username, password=password,
                database_name=database_name, server_id=server_id, process_name=process_name, 
                process_id=process_id, session_config_id=session_config_id)
dbw.db.command("collMod", "test_collection", validator=schema.RAW_SCHEMA)
col = dbw.db["test_collection"]

# insert a document of python dictionary format
doc1 = {
        "timestamp": [1.1,2.0,3.0],
        "first_timestamp": 1.0,
        "last_timestamp": 3.0,
        "x_position": [1.2]} 

print("# documents in collection before insert: ", col.count_documents({}))
dbw.write_fragment(doc1) 
print("# documents in collection after insert: ", col.count_documents({}))

# insert a document using keyword args
print("# documents in collection before insert: ", col.count_documents({}))
dbw.write_one_trajectory(collection_name = "test_collection" , timestamp = [1.1,2.0,3.0],
                           first_timestamp = 1.0,
                           last_timestamp = 3.0,
                           x_position = [1.2])
print("# documents in collection after insert: ", col.count_documents({}))
```
As of v0.1.1, if a document violates the schema, it bypasses the validation check and prints a message in the console. In future versions a schema rule will be strictly enforced.

### In future versions

Additional future enhancements include: 
- use logger in db_writer
