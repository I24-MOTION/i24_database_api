import pymongo
from threading import Thread
import json
import warnings

# TODO: replace print with log   
class DBWriter:
    """
    MongoDB database writer; uses asynchronous query mechanism in "motor" package by default.
    """

    def __init__(self, host, port, username, password, database_name, collection_name,
                 server_id, process_name, process_id, session_config_id, num_workers = 200, max_idle_time_ms = None, schema_file = None):
        """
        :param host: Database connection host name.
        :param port: Database connection port number.
        :param username: Database authentication username.
        :param password: Database authentication password.
        :param database_name: Name of database to connect to (do not confuse with collection name).
        :param server_id: ID value for the server this writer is running on.
        :param process_name: Name of the process this writer is attached to (writing from).
        :param process_id: ID value for the process this writer is attached to (writing from).
        :param session_config_id: Configuration ID value that was assigned to this run/session of data processing.
        :param schema_file: json file path
        """
        
        self.server_id = server_id
        self.process_name = process_name
        self.process_id = process_id
        self.session_config_id = session_config_id

        # Connect immediately upon instantiation.
        self.client = pymongo.MongoClient(host=host, port=port, username=username, 
                                          password=password,
                                          connect=True, 
                                          maxIdleTimeMS = max_idle_time_ms,
                                          connectTimeoutMS = 5000,
                                          )
        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            raise ConnectionError("Could not connect to MongoDB using pymongo.")
            
        self.db = self.client[database_name]
        
        if schema_file: # add validator
            f = open(schema_file)
            collection_schema = json.load(f)
            f.close()
            self.create_collection(collection_name, collection_schema)
        else: # remove validator
            warnings.warn("No schema rule is specified, remove the validator in collection {}".format(collection_name), UserWarning)
            self.create_collection(collection_name, None)
            self.db.command("collMod", collection_name, validator={})
        self.collection = self.db[collection_name]
        

    def create_collection(self, collection_name, schema = None):
        try: 
            self.db.create_collection(collection_name)
        except: 
            warnings.warn("Collection {} already exists".format(collection_name), UserWarning)
            pass
        if schema:
            self.db.command("collMod", collection_name, validator=schema)
        
    def insert_one_schema_validation(self, collection, document):
        '''
        A wrapper around pymongo insert_one, which is a thread-safe operation
        bypass_document_validation = True: enforce schema
        '''
        try:
            collection.insert_one(document, bypass_document_validation = False)
        except Exception as e: # schema violated
            warnings.warn("Schema violated. Insert anyways. Full error: {}".format(e), UserWarning)
            collection.insert_one(document, bypass_document_validation = True)
            
        
    def write_one_trajectory(self, thread = True, collection_name = None, **kwargs):
        """
        Write an arbitrary document specified in kwargs to a specified collection. No schema enforcment.
        :param thread: a boolean indicating if multi-threaded write is used
        :param collection_name: a string for write collection destination
        
        Use case:
        e.g.1. 
        dbw.write_one_trajectory(timestamp = [1,2,3], x_position = [12,22,33])
        e.g.2. 
        traj = {"timestamp": [1,2,3], "x_position": [12,22,33]}
        dbw.write_one_trajectory(**traj)
        """
        if collection_name:
            col = self.db[collection_name] # get default collection during construction
        else:
            col = self.collection
        
        doc = {} 
        for key,val in kwargs.items():
            doc[key] = val

        # add extra fields in doc
        configuration_id = self.session_config_id
        compute_node_id = self.server_id   
        
        doc["configuration_id"] = configuration_id
        doc["compute_node_id"] = compute_node_id
        
        if not thread:
            self.insert_one_schema_validation(col, doc)
        else:
            # fire off a thread
            t = Thread(target=self.insert_one_schema_validation, args=(col, doc,))
            t.daemon = True
            t.start()   
            
    
    
    def __del__(self):
        """
        Upon DBWriter deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except:
            pass