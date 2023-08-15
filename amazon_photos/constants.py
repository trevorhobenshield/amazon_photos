MAX_TRASH_BATCH = 50  # max nodes to batch into trash request
MAX_DOWNLOAD_BATCH = 1200  # max nodes to batch into zip file for download
MAX_NODES = 9999  # this is a real limit, not a large arbitrary number
MAX_LIMIT = 200  # max number of nodes to return in a single request
MAX_NODE_OFFSETS = [i * MAX_LIMIT for i in range(49)] + [MAX_NODES - MAX_LIMIT]
