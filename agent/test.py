import couchdb
import docker
import json
import time
import redis

from utils import run_cmd
import params
from params import WSK_CLI, COUCH_LINK


# param = "-p couch_link {} -p db_name eg".format(COUCH_LINK)
# response = str(run_cmd('{} action invoke eg -b {} | grep -v "ok:"'.format(WSK_CLI, param)))
# doc = json.loads(response)

# if len(doc["annotations"]) == 6:
#     init_time = doc["annotations"][5]["value"]
# else:
#     init_time = 0
# wait_time = doc["annotations"][1]["value"]
# duration = doc["duration"]

# print("init_time: {}, wait_time: {}, duration: {}".format(init_time, wait_time, duration))

# couch = couchdb.Server(params.COUCH_LINK)

# couch_activations = couch["whisk_distributed_activations"]
# couch_whisks = couch["whisk_distributed_whisks"]
# couch_subjects = couch["whisk_distributed_subjects"]

# for function in couch_whisks:
#     print(function)

# print(couch_whisks["guest/ir"])

# request_id = "9297b1e0406b4e4797b1e0406bee4774"
# print(couch_activations["guest/{}".format(request_id)])
# print(couch["ul"]["file"])
# for row in couch_activations.view(
#     "_all_docs", 
#     keys=["guest/87edad323b194cceadad323b194cceb3"], 
#     include_docs=True
# ):
#     print(row.doc["annotations"][3]["value"])

# db_list = ["ip", "vp", "ir", "dv", "alu", "knn", "gd", "ms", "dh", "eg"]
# for db_name in db_list:
#     # del couch[db_name]
#     for doc in couch[db_name]:
#         print(couch[db_name][doc])

# t = couch["vp"].get_attachment("result", "processed_hi_chitanda_eru.mp4")
# with open("/tmp/processed_hi_chitanda_eru.mp4", "wb") as out:
#     out.write(t.read())

# Connect to redis pool
# def get_redis(
#     redis_host, 
#     redis_port, 
#     redis_password
# ):
#     pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
#     redis_client = redis.Redis(connection_pool=pool)

#     return redis_client

# # Retrieve resource utilization metrics for each invoker from Redis
# def retrieve_invoker_dict(
#     redis_client,
#     metric_list
# ):
#     p = redis_client.pipeline(transaction=False)
#     invoker_dict = {}
#     num_invokers = int(run_cmd('cat ../ansible/environments/distributed/hosts | grep "invoker" | grep -v "\[invokers\]" | wc -l'))

#     for i in range(num_invokers):
#         invoker_name = "invoker{}".format(i)
#         p.hgetall(invoker_name)

#     p_result = p.execute()

#     for i, metric_dict in enumerate(p_result):
#         invoker_name = "invoker{}".format(i)
#         invoker_dict[invoker_name] = {}
#         for metric in metric_list:
#             invoker_dict[invoker_name][metric] = float(metric_dict[metric])

#     print(invoker_dict)


# if __name__ == "__main__":
#     redis_host = run_cmd('cat ../ansible/environments/distributed/hosts | grep -A 1 "\[edge\]" | grep -v "\[edge\]" | awk {}'.format("{'print $1'}"))
#     redis_port = 6379
#     redis_password = "openwhisk"

#     redis_client = get_redis(redis_host, redis_port, redis_password)
#     redis_client.flushall()
#     retrieve_invoker_dict(
#         redis_client,
#         [
#             # Available memory slots
#             "available_memory",
#             # CPU
#             "cpu_user",
#             "cpu_nice",
#             "cpu_kernel",
#             "cpu_idle",
#             "cpu_iowait",
#             "cpu_irq",
#             "cpu_softirq",
#             "cpu_steal",
#             "cpu_ctx_switches",
#             "cpu_load_avg",
#             # Memory
#             "memory_free",
#             "memory_buffers",
#             "memory_cached",
#             # Disk
#             "disk_read_count",
#             "disk_read_merged_count",
#             "disk_read_time",
#             "disk_write_count",
#             "disk_write_merged_count",
#             "disk_write_time",
#             # Network
#             "net_byte_recv",
#             "net_byte_sent"
#         ]
#     )
