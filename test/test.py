
from src.i24_database_api.db_reader import DBReader
from src.i24_database_api.db_writer import DBWriter
from src.i24_database_api import db_parameters, schema
import queue

# %% Test connection
try:
    dbr = DBReader(host=db_parameters.DEFAULT_HOST, port=db_parameters.DEFAULT_PORT, username=db_parameters.DEFAULT_USERNAME,   
               password=db_parameters.DEFAULT_PASSWORD,
               database_name=db_parameters.DB_NAME, collection_name=db_parameters.RAW_COLLECTION)
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
    
    
#%% Test DBWriter write_stitch Done
# query some test data to write to a new collection
print("Connect to DBReader")
test_dbr = DBReader(host=db_parameters.DEFAULT_HOST, port=db_parameters.DEFAULT_PORT, username=db_parameters.DEFAULT_USERNAME,   
               password=db_parameters.DEFAULT_PASSWORD,
               database_name=db_parameters.DB_NAME, collection_name=db_parameters.GT_COLLECTION)
print("Query...")
test_res = test_dbr.read_query(query_filter = {"last_timestamp": {"$gt": 5, "$lt":600}}, query_sort = [("last_timestamp", "ASC"), ("starting_x", "ASC")],
                   limit = 0)
test_res = list(test_res)
print("Connect to DBWriter")
dbw = DBWriter(host=db_parameters.DEFAULT_HOST, port=db_parameters.DEFAULT_PORT, username=db_parameters.DEFAULT_USERNAME,   
               password=db_parameters.DEFAULT_PASSWORD,
               database_name=db_parameters.DB_NAME, server_id=1, process_name=1, process_id=1, session_config_id=1)
print("Writing {} documents...".format(len(test_res)))

col = dbw.db[db_parameters.STITCHED_COLLECTION]
col.drop()
for doc in test_res:
    dbw.write_stitched_trajectory(doc, fragment_ids = [1,2,3])

          
      

#%% Test queue refill on static database DONE

# if queue is below a threshold, refill with the next range chunk

q = queue.Queue()
MIN_QUEUE_SIZE = 5
RANGE_INCREMENT = 50 # IN SEC
INSERT_NUM = 50
GETQ_NUM = 50

dbr = DBReader(host=db_parameters.DEFAULT_HOST, port=db_parameters.DEFAULT_PORT, username=db_parameters.DEFAULT_USERNAME,   
               password=db_parameters.DEFAULT_PASSWORD,
               database_name=db_parameters.DB_NAME, collection_name=db_parameters.RAW_COLLECTION)
rri = dbr.read_query_range(range_parameter='last_timestamp', range_greater_equal=0, range_less_than=400, range_increment=RANGE_INCREMENT)

while True:
    i = 0
    while i < GETQ_NUM: # simulate processing queue
        if not q.empty():
            q.get() 
        if q.qsize() <= MIN_QUEUE_SIZE: # only move to the next query range if queue is low in stock
            next_batch = next(rri)
            for doc in next_batch:
                q.put(doc)
        i += 1 
        
        print("Current range: {}-{}".format(rri._current_lower_value, rri._current_upper_value))
        print("q size: ", q.qsize())
    break
#    print("collection size", dbr.collection.count_documents({}))
    
#%% Test live queue refill with dummy database Done 

def insert_many_with_count(collection, insert_num, start):
    many_documents = ({"key": i+start} for i in range(insert_num))
    collection.insert_many(many_documents)
    return start + insert_num
    
q = queue.Queue()
MIN_QUEUE_SIZE = 5
RANGE_INCREMENT = 10 # IN SEC
INSERT_NUM = 5
T_BUFFER = 10

pipeline = [{'$match': {'operationType': 'insert'}}] # watch for insertion only

# initiate a mongodb replica set
dbr = DBReader(host=db_parameters.DEFAULT_HOST, port=db_parameters.DEFAULT_PORT, username=db_parameters.DEFAULT_USERNAME,   
                password=db_parameters.DEFAULT_PASSWORD,
                database_name=db_parameters.DB_NAME, collection_name="test_cs_collection")

rri = dbr.read_query_range(range_parameter='key', range_greater_equal=0, range_less_than=600, range_increment=RANGE_INCREMENT)


test_collection = dbr.db["test_cs_collection"]
test_collection.drop()
test_collection = dbr.db["test_cs_collection"]
# test_collection_rs = dbr_rs.db["test_cs_collection"] 
# dbr_rs.client.close()

iteration = 0
t_max = 0
start = 0

while iteration < 6:
    count = 0
    
    while q.qsize() <= MIN_QUEUE_SIZE: # only move to the next query range if queue is low in stock
        with test_collection.watch(pipeline) as stream:  
#            test_collection.insert_many(({'key': INSERT_NUM * iteration + i} for i in range(INSERT_NUM)))
            start = insert_many_with_count(test_collection, INSERT_NUM, start)
            change = stream.try_next()
            if change:
                print("new change: ", change["fullDocument"]["key"])
                t_max = max(change["fullDocument"]["key"], t_max) # reset the t_max cursor
                if t_max > rri._current_upper_value + T_BUFFER:
                    print("refilling queue...")
                    for doc in next(rri):
    #                        print(doc)
                        q.put(doc)
        count +=1 
                        
    if not q.empty():
        doc = q.get()
        print("** process", doc["key"])
    
    iteration += 1
    print("q size after: ", q.qsize())
#    print("collection size", test_collection.count_documents({}))
    print("tmax", t_max)
    print("current change", change["fullDocument"]["key"], "upper bound", rri._current_upper_value)



#%% Test insert with schema checking (validation)
dbw.db.command("collMod", "test_collection", validator=schema.RAW_SCHEMA)

col = dbw.db["test_collection"]
doc1 = {
        "timestamp": [1.1,2.0,3.0],
        "first_timestamp": 1.0,
        "last_timestamp": 3.0,
        "configuration_id": 1,
        "x_position": [1.2]} # this would work
doc2 = {
       "time": [1,2,3]
       } # this won't insert successfully
print(col.count_documents({}))
col.insert_one(doc1, bypass_document_validation = False) 
print(col.count_documents({}))







