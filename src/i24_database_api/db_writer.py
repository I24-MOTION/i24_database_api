import pymongo
from threading import Thread
import db_parameters, schema
# from i24_logger.log_writer import logger 
     
# TODO: replace print with log   
class DBWriter:
    """
    MongoDB database writer; uses asynchronous query mechanism in "motor" package by default.
    """

    def __init__(self, host, port, username, password, database_name,
                 server_id, process_name, process_id, session_config_id, num_workers = 200):
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
        """
        
        self.host, self.port = host, port
        self.username, self.password = username, password
        self.db_name = database_name
        self.server_id = server_id
        self.process_name = process_name
        self.process_id = process_id
        self.session_config_id = session_config_id

        # Connect immediately upon instantiation.
        self.client = pymongo.MongoClient(host=host, port=port, username=username, 
                                          password=password,
                                          connect=True, 
                                          connectTimeoutMS=5000,
                                          )
        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            # print("Server not available")
            raise ConnectionError("Could not connect to MongoDB using pymongo.")
            
        self.db = self.client[database_name]
        
        # create three critical collections
        try: self.db.create_collection(db_parameters.RAW_COLLECTION)
        except: pass   
        try: self.db.create_collection(db_parameters.STITCHED_COLLECTION)
        except: pass
        try: self.db.create_collection(db_parameters.RECONCILED_COLLECTION)
        except: pass
    
        # set rules for schema. enable schema checking when insert using
        # col.insert_one(doc)
        # disable schema checking: col.insert_one(doc, bypass_document_validation=False)
        # TODO: make a function to add customized schema to a specific collection, ground_truth, for example
        self.db.command("collMod", db_parameters.RAW_COLLECTION, validator=schema.RAW_SCHEMA)
        self.db.command("collMod", db_parameters.STITCHED_COLLECTION, validator=schema.STITCHED_SCHEMA)
        self.db.command("collMod", db_parameters.RECONCILED_COLLECTION, validator=schema.RECONCILED_SCHEMA)
        

    def insert_one_schema_validation(self, collection, document):
        '''
        A wrapper around pymongo insert_one, which is a thread-safe operation
        bypass_document_validation = True: enforce schema
        '''
        try:
            collection.insert_one(document, bypass_document_validation = False)
        except Exception as e: # schema violated
            # logger.warning("Schema violated during insertion. Insert with validation bypassed.")
            # print("Schema violated during insertion. Insert with validation bypassed.")
            # print(e)
            collection.insert_one(document, bypass_document_validation = True)
            
        
    def write_one_trajectory(self, thread = True, collection_name = "test_collection", **kwargs):
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
        
        col = self.db[collection_name]
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
            
            
    def write_fragment(self, document, thread = True):
        """
        Write a raw trajectory according to the data schema, found here:
            https://docs.google.com/document/d/1xli3N-FCvIYhvg7HqaQSOcKY44B6ZktGHgcsRkjf7Vg/edit?usp=sharing
        :param document: a dictionary following json format
        :param thread: set True if create a thread for insert in the background (fast), False if blocking insert_one
        :return: None
        """   
        self.write_one_trajectory(thread = True , collection_name = db_parameters.RAW_COLLECTION , **document)
        

    def write_stitched_trajectory(self, document, thread = True):
        """
        Write a stitched trajectory reference document according to the data schema, found here:
            https://docs.google.com/document/d/1vyLgsz6y0SrpTXWZNOS5fSgMnmwCr3xD0AB6MgYWl-w/edit?usp=sharing
        :param document: a dictionary following json format
        :return: None
        """
        self.write_one_trajectory(thread = True , collection_name = db_parameters.STITCHED_COLLECTION , **document )
    
    
    def write_reconciled_trajectory(self, document, thread = True):
        """
        Write a reconciled/post-processed trajectory according to the data schema, found here:
            https://docs.google.com/document/d/1Qh4OYOhOi1Kh-7DEwFfLx8NX8bjaFdviD2Q0GsfgR9k/edit?usp=sharing
        :param document: a dictionary following json format
        :return: None
        """
        self.write_one_trajectory(thread = True , collection_name = db_parameters.RECONCILED_COLLECTION , **document )
            
            
    def write_metadata(self, metadata):
        # TODO: add this function
        pass


    def write_ground_truth_trajectory(self, thread = True, **kwargs):
        """
        Write a ground truth trajectory according to the data schema, found here:
            https://docs.google.com/document/d/1zbjPycZlGNPOwuPVtY5GkS3LvIZwMDOtL7yFc575kSw/edit?usp=sharing
        Values that are in the schema, but assigned by the database are: db_write_timestamp, _id
        Values that are in the schema, but calculated implicitly from others are: first_timestamp, last_timestamp,
            starting_x, ending_x.
        Values that are in the schema, but given to DBWriter at instantiation are: configuration_id.
        :param vehicle_id: Same vehicle_id assigned during stitching.
        :param fragment_ids: Array of fragment_id values associated to current vehicle_id.
        :param coarse_vehicle_class: Vehicle coarse class number.
        :param fine_vehicle_class: Vehicle fine class number.
        :param timestamps: Corrected timestamps; may be corrected to reduce timestamp errors.
        :param road_segment_id: Unique road segment ID; differentiates mainline from ramps.
        :param x_positions: Array of back-center x-position along the road segment in feet. X=0 is beginning of segment.
        :param y_positions: Array of back-center y-position across the road segment in feet. Y=0 is located at the left
            yellow line, i.e., the left-most edge of the left-most lane of travel in each direction.
        :param length: Vehicle length in feet.
        :param width: Vehicle width in feet.
        :param height: Vehicle height in feet.
        :param direction: Indicator of roadway direction (-1 or +1).
        :return: None
        """
        # TODO make it to the same format as all other write functions
        pass
    
    
    def __del__(self):
        """
        Upon DBWriter deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except:
            pass