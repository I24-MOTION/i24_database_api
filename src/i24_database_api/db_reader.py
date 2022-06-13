
import pymongo
from collections import defaultdict
import warnings  
        
        
class DBReader:
    """
    MongoDB database reader, specific to a collection in the database. This object is typically fairly persistent, i.e.,
        sticks around for a while to execute multiple queries.
    """

    def __init__(self, host, port, username, password, database_name, collection_name):
        """
        Connect to the specified MongoDB instance, test the connection, then set the specific database and collection.
        :param host: Database connection host name.
        :param port: Database connection port number.
        :param username: Database authentication username.
        :param password: Database authentication password.
        :param database_name: Name of database to connect to (do not confuse with collection name).
        :param collection_name: Name of database collection from which to query.
        """
        # Connect immediately upon instantiation.
        self.client = pymongo.MongoClient(host=host, port=port, username=username, password=password,
                                          connect=True, connectTimeoutMS=5000)
        try:
            self.client.admin.command('ping')
        except pymongo.errors.ConnectionFailure:
            warnings.warn("Server not available")
            raise ConnectionError("Could not connect to MongoDB.")

        self.db = self.client[database_name]
        self.collection = self.db[collection_name]
        
        # create indices
        self.create_index(["first_timestamp", "last_timestamp", "starting_x", "ending_x", "configuration_id", "_id"])

        # Class variables that will be set and reset during iterative read across a range.
        self.range_iter_parameter = None
        self.range_iter_sort = None
        self.range_iter_start = None
        self.range_iter_start_closed_interval = None
        self.range_iter_increment = None
        self.range_iter_stop = None
        self.range_iter_stop_closed_interval = None
        
        # For live read
        self.change_stream_cursor = None

    def __del__(self):
        """
        Upon DBReader deletion, close the client/connection.
        :return: None
        """
        try:
            self.client.close()
        except pymongo.errors.PyMongoError:
            pass

    def read_query(self, query_filter, query_sort = None,
                   limit = 0):
        """
        Executes a read query against the database collection.
        :param query_filter: Currently a dict following pymongo convention (need to abstract this).
        :param query_sort: List of tuples: (field_to_sort, sort_direction); direction is ASC/ASCENDING or DSC/DESCENDING
        :param limit: Numerical limit for number of documents returned by query.
        :return:
        """
        if query_sort is not None:
            sort_fields = []
            for sort_field, sort_dir in query_sort:
                if sort_dir.upper() in ('ASC', 'ASCENDING'):
                    sort_fields.append((sort_field, pymongo.ASCENDING))
                elif sort_dir.upper() in ('DSC', 'DESCENDING'):
                    sort_fields.append((sort_field, pymongo.DESCENDING))
                else:
                    raise ValueError("Invalid direction for sort. Use 'ASC'/'ASCENDING' or 'DSC'/'DESCENDING'.")
        else:
            sort_fields = None

        # If user passed None, substitute an empty dictionary (per the PyMongo convention).
        if query_filter is None:
            filter_field = {}
        else:
            filter_field = query_filter

        result = self.collection.find(filter=filter_field, limit=limit, sort=sort_fields)
        # return the pymongo.cursor.Cursor
        return result

    # TODO: also datetime for range bounds??
    def read_query_range(self, range_parameter,
                         range_greater_than = None,
                         range_greater_equal= None,
                         range_less_than = None,
                         range_less_equal = None,
                         range_increment = None,
                         query_sort = None,
                         limit = 0,
                         static_parameters = None,
                         static_parameters_query = None):
        """
        Iterate across a query range in portions.
        Usage:
        ```
            # Method 1: FOR loop across function call
            for result in dbr.read_query_range(range_parameter='t', range_greater_than=0, range_less_equal=100,
                                                range_increment=10):
                print(result)
                
            # Method 2: WHILE loop with next(...)
            rqr = dbr.read_query_range(range_parameter='t', range_greater_equal=0, range_less_than=100,
                                        range_increment=10)
            while True:
                try:
                    result = next(rqr)
                    print(result)
                except StopIteration:
                    print("END OF ITERATION")
                    break
        ```
        :param range_parameter: One document field across which to run range queries.
        :param range_greater_than: Sets a '>' bound on `range_parameter` for the query or successive queries.
        :param range_greater_equal: Sets a '>' bound on `range_parameter` for the query or successive queries.
        :param range_less_than: Sets a '>' bound on `range_parameter` for the query or successive queries.
        :param range_less_equal: Sets a '>' bound on `range_parameter` for the query or successive queries.
        :param range_increment: When None, executes the range query as a one-off and returns result; otherwise,
            returns iterable of queries/results.
        :param query_sort: List of tuples: (field_to_sort, sort_direction); direction is ASC/ASCENDING or DSC/DESCENDING
        :param limit: Numerical limit for number of documents returned by query.
        :param static_parameters: (Multiple) document fields across with to query directly from.
            e.g., ["direction", "starting_x"]
        :param static_parameters_query: Operators correspond to static_parameters
            e.g., [("$eq", -1), ("$gt", 100)]
        :return: iterator across range-segmented queries (each query executes when __next__() is called in iteration)
        """
        # # no bounds: raise error TODO: start querying from the min value
        # if range_greater_than is None and range_greater_equal is None and range_less_than is None \
        #         and range_less_equal is None:
        #     raise ValueError("Must specify lower and or upper bound (inclusive or exlusive) for range query.")
            
        # # only bounded on one side: TODO: start querying from the min value
        # if (range_greater_than is None and range_greater_equal is None) or \
        #         (range_less_than is None and range_less_equal is None):
        #     raise NotImplementedError("Infinite ranges not currently supported.")

        # Save static query parameters to attributes
        if static_parameters:
            self.static_parameters = static_parameters
            self.static_parameters_query = static_parameters_query
        


        # if no range_increment, query everything between lower bound and upper bound
        if range_increment is None:
            query_filter = defaultdict(dict)            
            # more operations: https://www.mongodb.com/docs/manual/reference/operator/query/
            operators = ["$gt","$gte","$lt","$lte"]  
            values = [range_greater_than, range_greater_equal, range_less_than, range_less_equal]
            for i, operator in enumerate(operators):
                if values[i]: 
                    query_filter[range_parameter][operator] = values[i]
            # add static parameter filters if any is provided
            if self.static_parameters:
                for i, static_parameter in enumerate(self.static_parameters):
                    query_filter[static_parameter][self.static_parameters_query[i][0]] = self.static_parameters_query[i][1]
            return self.read_query(query_filter=query_filter, query_sort=query_sort, limit=limit)
        
        else:
            self.range_iter_parameter = range_parameter
            self.range_iter_increment = range_increment
            self.range_iter_sort = query_sort

            if range_greater_equal is not None: # left closed [a, ~
                self.range_iter_start = range_greater_equal
                self.range_iter_start_closed_interval = True
            elif range_greater_than is not None: # left open (a, ~
                self.range_iter_start = range_greater_than
                self.range_iter_start_closed_interval = False
            else:
                # TODO: temporarily set start and end point to the min and max values. For live stream, this is not applicable.
                self.range_iter_start = self.get_min(range_parameter)
                self.range_iter_start_closed_interval = True

            if range_less_equal is not None: # right closed a, b]
                self.range_iter_stop = range_less_equal
                self.range_iter_stop_closed_interval = True
            elif range_less_than is not None: # right open a, b)
                self.range_iter_stop = range_less_than
                self.range_iter_stop_closed_interval = False
            else:
                # TODO: temporarily set start and end point to the min and max values. Works on static database collections only.
                self.range_iter_stop = self.get_max(range_parameter)
                self.range_iter_stop_closed_interval = True
                
        return iter(self)
    

    def __iter__(self):
        if self.range_iter_parameter is None or self.range_iter_start is None or self.range_iter_increment is None \
                or self.range_iter_stop is None or self.range_iter_start_closed_interval is None \
                or self.range_iter_stop_closed_interval is None:
            raise AttributeError("Iterable DBReader only supported via `read_query_range(...).")
        return DBReadRangeIterator(self)

    # simple query functions
    def get_first(self, index_name):
        '''
        get the first document from MongoDB by index_name
        TODO: should match index_name
        '''
        return self.collection.find_one(sort=[(index_name, pymongo.ASCENDING)])
        
    def get_last(self, index_name):
        '''
        get the last document from MongoDB by index_name 
        TODO: should match index_name
        '''
        return self.collection.find_one(sort=[(index_name, pymongo.DESCENDING)])
    
    def find_one(self, index_name, index_value):
        return self.collection.find_one({index_name: index_value})
        
    def is_empty(self):
        return self.count() == 0
        
    def get_keys(self): 
        oneKey = self.collection.find().limit(1)
        for key in oneKey:
            return key.keys()
        
    def create_index(self, indices):
        try:
            all_field_names = self.collection.find_one({}).keys()
            existing_indices = self.collection.index_information().keys()
            for index in indices:
                if index in all_field_names:
                    if index+"_1" not in existing_indices and index+"_-1" not in existing_indices:
                        self.collection.create_index(index)     
        except Exception as e:
            warnings.warn("Index not created. Full error: {}".format(e))
            pass
        return
    
    def get_range(self, index_name, start, end): 
        return self.collection.find({
            index_name : { "$gte" : start, "$lt" : end}}).sort(index_name, pymongo.ASCENDING)
    
    def count(self):
        return self.collection.count_documents({})
    
    def get_min(self, index_name):
        return self.get_first(index_name)[index_name]
    
    def get_max(self, index_name):
        return self.get_last(index_name)[index_name]
    
    def exists(self, index_name, value):
        return self.collection.count_documents({index_name: value }, limit = 1) != 0
    
    

class DBReadRangeIterator:
    """
    Iterable class for executing successive queries using a DBReader. The range iteration values must be set in the
        DBReader before instantiating this object. They will be set back to None upon the end of iteration.
    """

    def __init__(self, db_reader):
        self._reader = db_reader
        self._current_lower_value = self._reader.range_iter_start
        self._current_upper_value = self._current_lower_value + self._reader.range_iter_increment
        # Initialize first/last iteration indicator variables.
        self._first_iter = True
        self._last_iter_exit_flag = False

    def _reset_range_iter(self):
        """
        Goes into the DBReader instance and resets all of its range iteration values back to None.
        :return: None
        """
        self._reader.range_iter_parameter = None
        self._reader.range_iter_sort = None
        self._reader.range_iter_start = None
        self._reader.range_iter_start_closed_interval = None
        self._reader.range_iter_increment = None
        self._reader.range_iter_stop = None
        self._reader.range_iter_stop_closed_interval = None

    def _update_values(self):
        """
        Increments the current iteration lower and upper bound. No interval open/closed indication needed because
            iterations other than the first and last are always [lower, upper) interval format.
        :return: None
        """
        self._current_lower_value = self._current_upper_value
        self._current_upper_value = self._current_upper_value + self._reader.range_iter_increment

    def __next__(self):
        """
        Runs the next range query based on the current values (self._current_...). Computes the next current values
            as well as the open/closed intervals. Sets and reacts to a flag for last iteration and raises
            StopIteration exception when complete.
        :return: result of next read query within the iteration range
        """
        # If the last iteration set this flag, then we need to stop iteration.
        # But if this current iteration is the last one that will return anything, we'll set the flag this time.
        if self._last_iter_exit_flag is True:
            self._reset_range_iter()
            raise StopIteration

        # Check if this will be the last query -- i.e., the current upper value met or exceeded the range stop.
        if self._current_upper_value >= self._reader.range_iter_stop:
            # Set the flag to exit next iteration.
            self._last_iter_exit_flag = True
            query_upper_value = self._reader.range_iter_stop
        else:
            query_upper_value = self._current_upper_value

        # If this is the first iteration, check whether we are doing open or closed interval on greater-than side.
        if self._first_iter is True:
            if self._reader.range_iter_start_closed_interval is True:
                gt, gte = None, self._current_lower_value
            else:
                gt, gte = self._current_lower_value, None
        # After first iteration, always do closed interval on greater-than side. [lower, upper)
        else:
            gt, gte = None, self._current_lower_value

        # If this is the last iteration, check whether we are doing open or closed interval on the less-than side.
        # We will only reach this point if this is the last results-gathering iteration.
        # The exit flag indicates we're about to stop, but we still need to get one more set of results.
        if self._last_iter_exit_flag is True:
            if self._reader.range_iter_stop_closed_interval is True:
                lt, lte = None, query_upper_value
            else:
                lt, lte = query_upper_value, None
        # Before last iteration, always do open interval on less-than side. [lower, upper)
        else:
            lt, lte = query_upper_value, None

        # Now that the range is calculated, execute outright (no increment) using `DBReader.read_query_range(...)`.
        # We use the range function so that we don't have to do the formatting of the query filter manually.
        iter_result = self._reader.read_query_range(range_parameter=self._reader.range_iter_parameter,
                                                    range_greater_than=gt, range_greater_equal=gte,
                                                    range_less_than=lt, range_less_equal=lte,
                                                    query_sort=self._reader.range_iter_sort, range_increment=None)
        # No matter what, this is not the first iteration anymore at this point.
        self._first_iter = False

        # Increment the values for the next iteration.
        # Even if this is the last results-gathering iteration, it's fine to increment the values.
        self._update_values()
        return iter_result

    def __iter__(self):
        """
        Needed in order to place DBReader.read_range_query(...) into a FOR loop.
        :return: self
        """
        return self
