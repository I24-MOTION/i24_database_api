
GT_COLLECTION = "ground_truth_one"
RAW_COLLECTION = "raw_trajectories_one" # specify raw trajectories collection name that is used for reading
# RAW_COLLECTION = "test_collection"
STITCHED_COLLECTION = "stitched_trajectories"
RECONCILED_COLLECTION = "reconciled_trajectories"
DB_NAME = "trajectories"

# BYPASS_VALIDATION = False # False if enforcing schema


# Create indices upon instantiation of DBReader and DBWriter objects
INDICES = ["_id", "ID", "first_timestamp", "last_timestamp"]

