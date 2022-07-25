# from i24_database_api.DBReader import DBReaderfrom multiprocessing import Process, Queue, Value, Managerimport transformation import batch_updatefrom ctypes import c_char_p    def transform(host, port, username, password, read_database_name, read_collection_name):    # re-wrap parameters    config = {        	"host": host,        	"port": port,        	"username": username,        	"password": password,        	"read_database_name": read_database_name,        	"read_collection_name": read_collection_name,        	"write_database_name": "transformed",        	"write_collection_name": read_collection_name    }    manager=Manager()    mode = manager.Value(c_char_p,"")    # mode = None        # initialize Queue for multiprocessing    # - transform pushes mongoDB operation requests to this queue, which batch_update would listen from    batch_update_connection = Queue()        # start 2 child processes    print("[Main] Starting Transformation process...")    proc_transform = Process(target=transformation.run, args=(config, mode, None, batch_update_connection, ))    proc_transform.start()    print("[Main] Starting Batch Update process...")    proc_batch_update = Process(target=batch_update.run, args=(config, mode,batch_update_connection, ))    proc_batch_update.start()        proc_transform.join()    proc_batch_update.join()    print("[Main] TRANSFORMATION TO THE DARK SIDE COMPLETE.")    # sys.exit(0)    # print('mode is '+ mode.value)    