from src.i24_database_api.db_reader import DBReader
from src.i24_database_api.db_writer import DBWriter
from i24_configparse.parse import parse_cfg
import os
import unittest


class DBTest(unittest.TestCase):
    cwd = os.getcwd()
    cfg = "./config"
    config_path = os.path.join(cwd,cfg)
    os.environ["user_config_directory"] = config_path
    db_param = parse_cfg("TEST", cfg_name = "test_param.config")

    schema_file1 = 'config/raw_schema.json'
    schema_file2 = 'config/stitched_schema.json'
    schema_file3 = 'config/reconciled_schema.json'

    def test_reader_connection_read_privilege(self):
        '''
        test connection to dbr, and user insert privilege should be disabled (read-only user)
        '''
        try:
            dbr = DBReader(host=self.db_param.default_host, port=self.db_param.default_port, 
                        username=self.db_param.readonly_user, password=self.db_param.default_password,
                        database_name=self.db_param.db_name, collection_name=self.db_param.raw_collection)
        except Exception as e:
            self.fail(e)
            return
            
        # read-only user cannot write
        with self.assertRaises(Exception):
            dbr.collection.insert_one({"key": "value"})
            
        
    # @unittest.skip("")
    def test_collection_indices_setup(self):
        '''
        test if DBReader properly sets up the indices for the collection
        '''
        try:
            dbr = DBReader(host=self.db_param.default_host, port=self.db_param.default_port, 
                        username=self.db_param.readonly_user, password=self.db_param.default_password,
                        database_name=self.db_param.db_name, collection_name=self.db_param.raw_collection)
        except Exception as e:
            self.fail(e)
            return
        
        existing_indices = dbr.collection.index_information().keys()
        expected_indices = ["first_timestamp", "last_timestamp", "ending_x", "starting_x"]
        for index in expected_indices:
            self.assertTrue(index+"_1" in existing_indices or index+"_-1" in existing_indices, "{} is not in existing indices".format(index))

    

    # @unittest.skip("")
    def test_read_query_without_range_increment(self):
        '''
        no range increment, should return all within lower and upper bound
        '''
        try:
            dbr = DBReader(host=self.db_param.default_host, port=self.db_param.default_port, 
                        username=self.db_param.readonly_user, password=self.db_param.default_password,
                        database_name=self.db_param.db_name, collection_name=self.db_param.raw_collection)
        except Exception as e:
            self.fail(e)
            return
        
        rri = dbr.read_query_range(range_parameter='last_timestamp', range_greater_equal=300, range_less_than=330, range_increment=None)
        last_time = None
        while True:
            try:
                last_time = next(rri)["last_timestamp"] # access documents in rri one by one
            except StopIteration:
                break
            
        self.assertTrue(last_time <= 330)

        return
    
    # @unittest.skip("")
    def test_read_query_with_range_increment(self):
        '''
        with range increment, should return chunks of queried items
        '''
        try:
            dbr = DBReader(host=self.db_param.default_host, port=self.db_param.default_port, 
                        username=self.db_param.readonly_user, password=self.db_param.default_password,
                        database_name=self.db_param.db_name, collection_name=self.db_param.raw_collection)
        except Exception as e:
            self.fail(e)
            return
        
        rri = dbr.read_query_range(range_parameter='last_timestamp', range_greater_equal=300, range_less_than=330, range_increment=10,
                                   static_parameters = ["direction"], static_parameters_query = [("$eq", dir)])
        count = 0
        with self.assertRaises(StopIteration):
            while True:            
                next(rri)
                count += 1
    
        self.assertEqual(count, 3, "Number of iterations incorrect")
        return
    
    def read_query_no_bounds(self):
        '''
        when bounds for query not specified, default to current min and max as bounds
        '''
        return
    
    # @unittest.skip("")
    def test_writer_connection_write_privilege(self):
        '''
        test connection to DBWriter, and user insert privilege should be enabled
        '''
        try:
            with self.assertWarns(Warning): # if no schema, should throw an warning
                dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                        username=self.db_param.default_username, password=self.db_param.default_password,
                        database_name=self.db_param.db_name, collection_name="test_collection",
                        server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=None)
            
                # try writing
                c1 = dbw.collection.count_documents({})
                dbw.write_one_trajectory(thread=False, key=1)
                c2 = dbw.collection.count_documents({})
                dbw.collection.drop()
                self.assertEqual(c2-c1, 1, "Message not successfully inserted")
            
        except Exception as e:
            self.fail(e)
            return  
        
       
    # @unittest.skip("")
    def test_write_with_kwargs_schema1(self):
        '''
        write to db by specifying keywords arguments
        '''
        try:
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file1)
        
            # write with schema rule
            c1 = dbw.collection.count_documents({})
            dbw.write_one_trajectory(thread = False, timestamp = [1.1,2.0,3.0],
                                first_timestamp = 1,
                                last_timestamp = 3.0,
                                x_position = [1.2])
            c2 = dbw.collection.count_documents({}) 
            dbw.collection.drop() # validation rule is dropped as well
            self.assertEqual(c2-c1, 1, "Message not successfully inserted with correct schema rule")
            
            # not according to schema rule, reset validator rule according to json file
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file1)
            
            with self.assertWarns(Warning): # should throw an warning   
                c1 = dbw.collection.count_documents({})
                dbw.write_one_trajectory(thread = False, timestamp = [1.1,2.0,3],
                                    first_timestamp = 1,
                                    last_timestamp = 3.0,
                                    x_position = [1.2])
                c2 = dbw.collection.count_documents({})
                dbw.collection.drop()
                self.assertEqual(c2-c1, 1, "Message not successfully inserted with incorrect schema rule")
            
        except Exception as e:
            self.fail(e)
            return  
        
        
    def test_write_with_kwargs_schema2(self):
        '''
        write to db by specifying keywords arguments
        '''
        try:
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file2)
        
            # write with schema rule
            c1 = dbw.collection.count_documents({})
            dbw.write_one_trajectory(fragment_ids = ["1", "2", "627c04d20187c4ff12f9b7c0"])
            c2 = dbw.collection.count_documents({})
            dbw.collection.drop()
            self.assertEqual(c2-c1, 1, "Message not successfully inserted with correct schema rule")
            
            # not according to schema rule
            # not according to schema rule, reset validator rule according to json file
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file2)
            with self.assertWarns(Warning):
                c1 = dbw.collection.count_documents({})
                dbw.write_one_trajectory(fragment_ids = [1,2])
                c2 = dbw.collection.count_documents({})
                dbw.collection.drop()
                self.assertEqual(c2-c1, 1, "Message not successfully inserted with incorrect schema rule")
            
        except Exception as e:
            self.fail(e)
            return  
       
    # @unittest.skip("")
    def test_write_with_kwargs_schema3(self):
        '''
        write to db by specifying keywords arguments
        '''
        try:
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file3)
        
            # write with schema rule
            c1 = dbw.collection.count_documents({})
            dbw.write_one_trajectory(thread = False, timestamp = [1.1,2.0,3.0],
                                first_timestamp = 1.0,
                                last_timestamp = 3.0,
                                x_position = [1.2])
            c2 = dbw.collection.count_documents({}) 
            dbw.collection.drop() # validation rule is dropped as well
            self.assertEqual(c2-c1, 1, "Message not successfully inserted with correct schema rule")
            
            # not according to schema rule, reset validator rule according to json file
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file3)
            
            with self.assertWarns(Warning): # should throw an warning   
                c1 = dbw.collection.count_documents({})
                dbw.write_one_trajectory(thread = False, timestamp = [1.1,2.0,3],
                                    first_timestamp = 1,
                                    last_timestamp = 3.0,
                                    x_position = [1.2])
                c2 = dbw.collection.count_documents({})
                dbw.collection.drop()
                self.assertEqual(c2-c1, 1, "Message not successfully inserted with incorrect schema rule")
            
        except Exception as e:
            self.fail(e)
            return  
        
    
    # @unittest.skip("")
    def test_write_with_dictionary(self):
        '''
        write to db by wrapping all fields into a dictionary
        '''
        try:
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file1)
        
            # write with schema rule
            c1 = dbw.collection.count_documents({})
            doc = {"timestamp": [1.1,2.0,3.0],
                                "first_timestamp": 1,
                                "last_timestamp": 3.0,
                                "x_position": [1.2]}
            dbw.write_one_trajectory(**doc)
            c2 = dbw.collection.count_documents({})
            dbw.collection.drop()
            self.assertEqual(c2-c1, 1, "Message (dict) not successfully inserted with correct schema rule")
            
            # not according to schema rule
            dbw = DBWriter(host=self.db_param.default_host, port=self.db_param.default_port, 
                    username=self.db_param.default_username, password=self.db_param.default_password,
                    database_name=self.db_param.db_name, collection_name="test_collection",
                    server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=self.schema_file1)
        
            with self.assertWarns(Warning):
                c1 = dbw.collection.count_documents({})
                doc = {"timestamp": [1.1,2.0,3],
                                    "first_timestamp": 1,
                                    "last_timestamp": 3.0,
                                    "x_position": [1.2]}
                
                dbw.write_one_trajectory(**doc)
                c2 = dbw.collection.count_documents({})
                dbw.collection.drop()
                self.assertEqual(c2-c1, 1, "Message (dict) not successfully inserted with incorrect schema rule")
            
        except Exception as e:
            self.fail(e)
            return  
        
        return
    
   
    
if __name__ == '__main__':
    unittest.main()

 