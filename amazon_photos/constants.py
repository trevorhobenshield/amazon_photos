MAX_TRASH_BATCH = 50
MAX_NODES = 9999  # this is a real limit, not a large arbitrary number
MAX_LIMIT = 200
MAX_NODE_OFFSETS = [i * MAX_LIMIT for i in range(49)] + [MAX_NODES - MAX_LIMIT]
