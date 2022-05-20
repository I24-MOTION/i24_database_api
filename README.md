

# Custom I-24 Database API package
#### Version: 0.1.1
#### Date revised: 05/10/2022

### Installation
With the desired python venv / conda env activated, use the following command in shell:

`pip install git+https://github.com/yanb514/i24_database_api@<tag>`

where `<tag>` is either a branch name (e.g. `main`) or a tag name (e.g. `v0.1`)
    
Then, to import the data reader or writer object and establish a connection:

```
from i24_database_api.db_reader import DBReader
from i24_database_api.db_writer import DBWriter
dbr = DBReader(host=host, port=port, username=username, password=password,
                database_name=database_name, collection_name=collection_name)
```
Access the corresponding client connection by
```
dbr.client
dbw.client
```

Access the corresponding client connection by
```
dbr.db
dbw.db
```

Note that only one collection is assigned to each reader or writer object for the convenience of read and write. For example:
```
dbr.collection
```
gives you the same access as the following command
```
dbr.client.db.collection
```

DBReader's user role should be read-only, and DBWriter's user role should be write-only. To access or change user privileges, specify usernames and privileges in mongod shell.
More details: 
https://stackoverflow.com/questions/23943651/mongodb-admin-user-not-authorized
https://www.codexpedia.com/devops/mongodb-authentication-setting/
https://www.mongodb.com/docs/manual/tutorial/manage-users-and-roles/

### Requirements
- pymongo

### Latest version supports
- continuous range query
- concurrent insert
- schema enforcement (pass schema rule as .json file)

## Usage examples for DBReader

#### Query a single document
```python
dbr.find_one(index_name, value)
```
#### Query based on filter
This API follows pymongo implementation, a more abstracted version of pymongo's collection.find()
```python
query_filter = {"_id": {"$in": fragment_ids}}
query_sort = [("last_timestamp", "ASC")])
dbr.read_query(query_filter, query_sort)
```


#### Iterative range query

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

## Usage examples for DBWriter

Instantiate a DBWriter object
```python
dbw = DBWriter(host=host, port=port, username=username, password=password,
                database_name=database_name, server_id=server_id, process_name=process_name, 
                process_id=process_id, session_config_id=session_config_id,schema_file=schema_file)
```

#### Create a collection
A collection with specified ```collection_name``` is automatically created upon instantiating the DBWriter object. If a schema file (in json) is given, the writer object adds validation rule to the collection based on the json file. 
Otherwise, it gives a warning "no schema provided", and proceeds without validation rule.

A collection can also be created after the DBWriter object is instantiated, simply call
```python
dbw.create_collection(collection_name = collection_name, schema = schema_file) # schema is optional
```

#### Concurrent insert with multithreading
When bulk write to database, this package offers the choice to do non-blocking (concurrent) insert:

```
col = dbw.collection

# insert a document of python dictionary format -> pass it as kwargs
doc1 = {
        "timestamp": [1.1,2.0,3.0],
        "first_timestamp": 1.0,
        "last_timestamp": 3.0,
        "x_position": [1.2]} 

print("# documents in collection before insert: ", col.count_documents({}))
dbw.write_one_trajectory(**doc1) 
print("# documents in collection after insert: ", col.count_documents({}))

# insert a document using keyword args directly
print("# documents in collection before insert: ", col.count_documents({}))
dbw.write_one_trajectory(collection_name = "test_collection" , timestamp = [1.1,2.0,3.0],
                           first_timestamp = 1.0,
                           last_timestamp = 3.0,
                           x_position = [1.2])
print("# documents in collection after insert: ", col.count_documents({}))
```
As of v0.1.1, if a document violates the schema, it bypasses the validation check and throws a warning in the console. 

### In future versions

Additional future enhancements include: 
- use logger in db_writer
- simplify object initiation with less arguments
- add built-in user privilege checking (but this step requires authentication). After temporary disable authentication in mongod.conf, one can do
```python
dbr.client.admin.command({"usersInfo": "readonly" })['users'][0]['roles']
```
to get all the user info. Check the specified user has only "read only" privilege or not. Similar for DBWriter.
