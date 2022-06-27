import pymongo
from threading import Thread
import json
import warnings
import sys

# TODO: replace print with log   
class DBWriter:
    """
    MongoDB database writer; uses asynchronous query mechanism in "motor" package by default.
    """

    def __init__(self, default_param, host=None, port=None, username=None, password=None, database_name=None, collection_name=None,
                 server_id=None, process_name=None, process_id=None, session_config_id=None, max_idle_time_ms = None, schema_file = None):
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
        if not isinstance(default_param, dict): # convert to dictionary first
            default_param = default_param.__dict__
            
        # Get default parameters
        if not collection_name:
            raise Exception("collection_name is required upon initiating DBWriter")
        if not server_id and "server_id" not in default_param:
            raise Exception("server_id is not found in database configuration")
        if not process_name and "process_name" not in default_param:
            raise Exception("process_name is not found in database configuration")
        if not session_config_id and "session_config_id" not in default_param:
            raise Exception("session_config_id is not found in database configuration")
        if not process_id and "process_id" not in default_param:
            raise Exception("process_id is not found in database configuration")
            
            
        if not host: host = default_param["default_host"]
        if not port: port = default_param["default_port"]
        if not username: username = default_param["default_username"]
        if not password: password = default_param["default_password"]
        if not database_name: database_name = default_param["db_name"]
        if not server_id: server_id = default_param["server_id"]
        if not process_name: process_name = default_param["process_name"]
        if not process_id: process_id = default_param["process_id"]
        if not session_config_id: session_config_id = default_param["session_config_id"]
        
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
        
        try: 
            self.db.create_collection(collection_name)
        except:
            pass
        
        self.collection = self.db[collection_name]
        self.collection_name = collection_name
        
    
        # check for schema. If exists a schema json file, update the collection validator. Otherwise remove the validator        
        if schema_file: # add validator
            f = open(schema_file)
            collection_schema = json.load(f)
            self.schema = collection_schema
            f.close()
            self.db.command("collMod", collection_name, validator=collection_schema)
    
        else: # remove validator
            warnings.warn("No schema rule is specified, remove the validator in collection {}".format(collection_name), UserWarning)
            self.db.command("collMod", collection_name, validator={})
            self.schema = None
        
        
    def reset_collection(self, another_collection_name = None):
        """
        Reset self.collection. If another_collection_name is provided, reset that collection instead
        """
        if another_collection_name is None:
            self.collection.drop()
            self.db.create_collection(self.collection_name)
            self.collection = self.db[self.collection_name]
            if self.schema:
                self.db.command("collMod", self.collection_name, validator=self.schema)
        else:
            self.db[another_collection_name].drop()
            self.db.create_collection[another_collection_name]
            
        
        
    def insert_one_schema_validation(self, collection, document):
        """
        A wrapper around pymongo insert_one, which is a thread-safe operation
        bypass_document_validation = True: enforce schema
        """
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
            # self.threads.append(t)
            t.daemon = True
            t.start()   
            
    
    # def join_threads(self):
    #     """
    #     For handling graceful shutdown. All threads for insert are joined
    #     """
    #     for t in self.threads:
    #         t.join()
        
    def count(self):
        return self.collection.count_documents({})


    def __del__(self):
        """
        Upon DBWriter deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except:
            pass