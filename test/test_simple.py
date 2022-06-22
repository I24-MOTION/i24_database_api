#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 22 11:40:39 2022

@author: yanbing_wang
"""
from src.i24_database_api.db_reader import DBReader
from src.i24_database_api.db_writer import DBWriter
from i24_configparse import parse_cfg
import os

cwd = os.getcwd()
cfg = "config"
config_path = os.path.join(cwd,cfg)
os.environ["user_config_directory"] = config_path
os.environ["my_config_section"] = "DEFAULT"
db_param = parse_cfg("my_config_section", cfg_name = "database_param.config")

schema_file1 = 'config/raw_schema.json'
schema_file2 = 'config/stitched_schema.json'

dbw = DBWriter(host=db_param.default_host, port=db_param.default_port, 
        username=db_param.default_username, password=db_param.default_password,
        database_name=db_param.db_name, collection_name="repeat_collection",
        server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=None)


# dbw = DBWriter(host=db_param.default_host, port=db_param.default_port, 
#         username=db_param.default_username, password=db_param.default_password,
#         database_name=db_param.db_name, collection_name="repeat_collection",
#         server_id=1, process_name=1, process_id=1, session_config_id=1, schema_file=None)

       
        