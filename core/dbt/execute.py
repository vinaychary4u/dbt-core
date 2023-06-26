all_nodes = {}  # type: ignore
threa_id_sqls = {}  # type: ignore


def add_node(node_id: str, thread_id: str):
    all_nodes[node_id] = thread_id
    threa_id_sqls[thread_id] = []


def add_execution(thread_id: str, sql: str):
    if thread_id not in threa_id_sqls:
        add_node(f"main thread {thread_id}", thread_id)
    threa_id_sqls[thread_id].append(sql)


def get_all_sqls():
    # return all sqls with their node id as prefix, and sorted by node id
    ret = []
    for node_id in sorted(all_nodes.keys()):
        for sql in threa_id_sqls[all_nodes[node_id]]:
            ret.append(f"**{node_id}**:\n{sql} \n")
    return ret
