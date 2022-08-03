

# Custom I-24 Database API package

#### Version: main
#### Date revised: 08/03/2022

### Requirements
- pymongo

### Installation
With the desired python venv / conda env activated, use the following command in shell:

`pip install git+https://github.com/yanb514/i24_database_api@<tag>`

where `<tag>` is either a branch name (e.g. `main`), a tag name (e.g. `v0.3`), or the latest version (`latest`)
    
Then, to import the data reader or writer object and establish a connection to client
- initialize with one liner. ```default_param``` a dictionary read from a config file (template see test_param_template.config).
``` python
default_param = {
  "host": "<mongodb-host>",
  "port": 27017,
  "username": "<mongodb-username>",
  "password": "<mongodb-password>"
}
dbc = DBClient(**default_param)
```
If connect to a specific database (optional) or collection (optional):
```python
dbc = DBClient(**default_param, database_name = <database_name>, collection_name = <collection_name>)
```
Either ways ```dbc.client``` is essentially a wrapper of ```pymongo.MongoClient``` object, and inherits all properties and functions of it.

List all collections
```python
dbc.list_collection_names(), or equivalently
dbc.db.list_collection_names()
```
Easily switch to another database:
```python
newdb = dbc.client[<new_database_name>]
newdb.list_collection_names()
```
Connect to the last updated collection in a database:
```python
dbc = DBClient(**default_param, database_name = <database_name>, collection_name = <collection_name>, latest_collection=True)
print(dbc.collection_name)
```



Drop (delete) a collection
```python
dbr.collection.drop(), or
dbr.db[<some_collection_name>].drop(), or access another db
dbr.client[<some_database>][<some_collection_name>].drop()
```

Bulk delete collections in current database (```dbc.db```) by
```python
dbc.delete_collection([list_of_cols_to_be_deleted])
```
Mark collections to be safe from deletion:
```python
dbc.mark_safe([safe_collection_list])
```



User roles:
More details: 
https://stackoverflow.com/questions/23943651/mongodb-admin-user-not-authorized
https://www.codexpedia.com/devops/mongodb-authentication-setting/
https://www.mongodb.com/docs/manual/tutorial/manage-users-and-roles/



### Other features:
- continuous range query
- async insert
- schema enforcement (pass schema rule as .json file)


#### Query a single document
```python
dbc.find_one(index_name, value)
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

produces
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

```python
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
As of v0.2, if a document violates the schema, it bypasses the validation check and throws a warning in the console. 


#### Schema examples
"Reconciled trajectories" collection
```
{
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["timestamp", "last_timestamp", "x_position"],
        "properties": {
            "configuration_id": {
                "bsonType": "int",
                "description": "A unique ID that identifies what configuration was run. It links to a metadata document that defines all the settings that were used system-wide to generate this trajectory fragment"
                },
            "coarse_vehicle_class": {
                "bsonType": "int",
                "description": "Vehicle class number"
                },
            
            "timestamp": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "Corrected timestamp. This timestamp may be corrected to reduce timestamp errors."
                },
            
 
            "road_segment_ids": {
                "bsonType": "array",
                "items": {
                    "bsonType": "int"
                    },
                "description": "Unique road segment ID. This differentiates the mainline from entrance ramps and exit ramps, which get distinct road segment IDs."
                },
            "x_position": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "Array of back-center x position along the road segment in feet. The  position x=0 occurs at the start of the road segment."
                },
            "y_position": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "array of back-center y position across the road segment in feet. y=0 is located at the left yellow line, i.e., the left-most edge of the left-most lane of travel in each direction."
                },
            
            "length": {
                "bsonType": "double",
                "description": "vehicle length in feet."
                },
            "width": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "vehicle width in feet"
                },
            "height": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "vehicle height in feet"
                },
            "direction": {
                "bsonType": "int",
                "description": "-1 if westbound, 1 if eastbound"
                }

            }
        }
    }
```
https://github.com/yanb514/i24_database_api/blob/main/test/config/reconciled_schema.json




### In future versions

Additional future enhancements include: 
- Use logger in db_writer
- Allow more customization in DBWriter, such as max time out etc.
- Add built-in user privilege checking (but this step requires authentication). After temporary disable authentication in mongod.conf, one can do
```python
dbr.client.admin.command({"usersInfo": "readonly" })['users'][0]['roles']
```
to get all the user info. Check the specified user has only "read only" privilege or not. Similar for DBWriter.
