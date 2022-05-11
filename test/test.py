from src.i24_database_api.db_reader import DBReader
from src.i24_database_api.db_writer import DBWriter
from config import test_parameters


# %% Test connection
try:
    dbr = DBReader(host=test_parameters.DEFAULT_HOST, port=test_parameters.DEFAULT_PORT, username=test_parameters.READONLY_USER,   
               password=test_parameters.DEFAULT_PASSWORD,
               database_name=test_parameters.DB_NAME, collection_name=test_parameters.RAW_COLLECTION)
except Exception as e:
    print(e)

# %% Test read_query Done
dbr.create_index(["last_timestamp", "first_timestamp", "starting_x", "ending_x"])
res = dbr.read_query(query_filter = {"last_timestamp": {"$gt": 5, "$lt":330}}, query_sort = [("last_timestamp", "ASC"), ("starting_x", "ASC")],
                   limit = 0)

for doc in res:
    print("last timestamp: {:.2f}, starting_x: {:.2f}, ID: {}".format(doc["last_timestamp"], doc["starting_x"], doc["ID"]))


#%% Test read_query_range (no range_increment) Done
print("Using while-loop to read range")
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


#%% Test read_query_range (with range_increment) Done
print("Using while-loop to read range")
rri = dbr.read_query_range(range_parameter='last_timestamp', range_greater_equal=300, range_less_than=330, range_increment=10,static_parameters = ["direction"], static_parameters_query = [("$eq", dir)])
while True:
    try:
        print("Current range: {}-{}".format(rri._current_lower_value, rri._current_upper_value))
        for doc in next(rri): # next(rri) is a cursor
            print(doc["last_timestamp"])
    except StopIteration:
        print("END OF ITERATION")
        break

#%% Test read_query_range with no upper or lower bounds (with range_increment) Done
print("Using while-loop to read range")
rri = dbr.read_query_range(range_parameter='last_timestamp', range_less_equal = 320, range_increment=10,static_parameters = ["direction"], static_parameters_query = [("$eq", dir)])

iteration = 0
while iteration < 5:
    try:
        print("Current range: {}-{}".format(rri._current_lower_value, rri._current_upper_value))
        for doc in next(rri): # next(rri) is a cursor
            print(doc["last_timestamp"])
    except StopIteration:
        print("END OF ITERATION")
        break
    iteration += 1
    
 

#%% Test insert with schema checking (validation)
test_schema = {
    "$jsonSchema": {
        "bsonType": "object",
        "required": ["timestamp", "first_timestamp"],
        "properties": {
            "configuration_id": {
                "bsonType": "int",
                "description": "A unique ID that identifies what configuration was run. It links to a metadata document that defines all the settings that were used system-wide to generate this trajectory fragment"
                },
            "timestamp": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": ""
                },
            "first_timestamp": {
                "bsonType": "double",
                "description": ""
                },
            "last_timestamp": {
                "bsonType": "double",
                "description": ""
                },
            "x_position": {
                "bsonType": "array",
                "items": {
                    "bsonType": "double"
                    },
                "description": "Array of back-center x position along the road segment in feet. The  position x=0 occurs at the start of the road segment."
                },
            
            }
        }
    }

dbw = DBWriter(host=test_parameters.DEFAULT_HOST, port=test_parameters.DEFAULT_PORT, username=test_parameters.DEFAULT_USERNAME,   
               password=test_parameters.DEFAULT_PASSWORD,
               database_name=test_parameters.DB_NAME, collection_name = "test_collection",
               server_id=1, process_name=1, process_id=1, session_config_id=1, collection_schema=test_schema)


col = dbw.collection

print(col.count_documents({}))
dbw.write_one_trajectory(timestamp = [1.1,2.0,3.0],
                    first_timestamp = 1.0,
                    last_timestamp = 3.0,
                    x_position = [1.2])
print(col.count_documents({}))

doc = {"timestamp": [1.1,2.0,4.0],
                   "first_timestamp": 1.0,
                   "last_timestamp": 3.0,
                   "x_position": [1.2]}
dbw.write_one_trajectory(collection_name = "test_collection", **doc)
print(col.count_documents({}))


