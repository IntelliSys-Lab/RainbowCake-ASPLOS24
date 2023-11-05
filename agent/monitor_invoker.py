import socket
import time
import redis
import psutil
import docker

import params
from cmd import run_cmd


# Connect to redis pool
def get_redis_client(
    redis_host, 
    redis_port, 
    redis_password
):
    pool = redis.ConnectionPool(host=redis_host, port=redis_port, password=redis_password, decode_responses=True)
    redis_client = redis.Redis(connection_pool=pool)

    return redis_client

# Connect to docker
def get_docker_client():
    return docker.from_env()

# Get invoker name
def get_invoker_name():
    invoker_hostname = socket.gethostname()
    invoker_ip = socket.gethostbyname(invoker_hostname)
    invoker_name = run_cmd('cat ../ansible/environments/distributed/hosts | grep "invoker" | grep "{}" | awk {}'.format("={}$".format(invoker_ip), "{'print $1'}"))

    return invoker_name

# Get invoker dict
def get_invoker_dict(docker_client):
    invoker_dict = {}

    # CPU metrics
    cpu_times = psutil.cpu_times()
    cpu_stats = psutil.cpu_stats()
    cpu_freq = psutil.cpu_freq()
    cpu_percent = psutil.cpu_percent()
    cpu_count = psutil.cpu_count()
    load_avg = psutil.getloadavg()

    invoker_dict["cpu_total"] = cpu_times.user + cpu_times.system
    invoker_dict["cpu_user"] = cpu_times.user
    invoker_dict["cpu_nice"] = cpu_times.nice
    invoker_dict["cpu_kernel"] = cpu_times.system
    invoker_dict["cpu_idle"] = cpu_times.idle
    invoker_dict["cpu_iowait"] = cpu_times.iowait
    invoker_dict["cpu_irq"] = cpu_times.irq
    invoker_dict["cpu_softirq"] = cpu_times.softirq
    invoker_dict["cpu_steal"] = cpu_times.steal
    invoker_dict["cpu_ctx_switches"] = cpu_stats.ctx_switches
    invoker_dict["cpu_load_avg"] = load_avg[0]

    # Memory metrics
    virtual_memory = psutil.virtual_memory()
    memory_percent = virtual_memory.percent
    swap_memory = psutil.swap_memory()

    invoker_dict["memory_total"] = virtual_memory.total
    invoker_dict["memory_free"] = virtual_memory.free
    invoker_dict["memory_buffers"] = virtual_memory.buffers
    invoker_dict["memory_cached"] = virtual_memory.cached

    # Disk metrics
    disk_partitions = psutil.disk_partitions()
    disk_usage = psutil.disk_usage('/')
    disk_io_counters = psutil.disk_io_counters(perdisk=False)

    invoker_dict["disk_read_count"] = disk_io_counters.read_count
    invoker_dict["disk_read_merged_count"] = disk_io_counters.read_merged_count
    invoker_dict["disk_read_time"] = disk_io_counters.read_time
    invoker_dict["disk_write_count"] = disk_io_counters.write_count
    invoker_dict["disk_write_merged_count"] = disk_io_counters.write_merged_count
    invoker_dict["disk_write_time"] = disk_io_counters.write_time

    # Network metrics
    net_io_counters = psutil.net_io_counters()
    net_connections = psutil.net_connections()
    net_if_addrs = psutil.net_if_addrs()
    net_if_stats = psutil.net_if_stats()

    invoker_dict["net_byte_recv"] = net_io_counters.bytes_recv
    invoker_dict["net_byte_sent"] = net_io_counters.bytes_sent

    # Alive containers for each function
    invoker_dict["n_alive_container_dl"] = 0
    invoker_dict["n_alive_container_eg"] = 0
    invoker_dict["n_alive_container_ip"] = 0
    invoker_dict["n_alive_container_vp"] = 0
    invoker_dict["n_alive_container_ir"] = 0
    invoker_dict["n_alive_container_knn"] = 0
    invoker_dict["n_alive_container_alu"] = 0
    invoker_dict["n_alive_container_ms"] = 0
    invoker_dict["n_alive_container_gd"] = 0
    invoker_dict["n_alive_container_dv"] = 0

    for container in docker_client.api.containers(filters={"name": "guest"}):
        for name in container["Names"]:
            if "guest_dl" in name:
                invoker_dict["n_alive_container_dl"] = invoker_dict["n_alive_container_dl"] + 1
            elif "guest_eg" in name:
                invoker_dict["n_alive_container_eg"] = invoker_dict["n_alive_container_eg"] + 1
            elif "guest_ip" in name:
                invoker_dict["n_alive_container_ip"] = invoker_dict["n_alive_container_ip"] + 1
            elif "guest_vp" in name:
                invoker_dict["n_alive_container_vp"] = invoker_dict["n_alive_container_vp"] + 1
            elif "guest_ir" in name:
                invoker_dict["n_alive_container_ir"] = invoker_dict["n_alive_container_ir"] + 1
            elif "guest_knn" in name:
                invoker_dict["n_alive_container_knn"] = invoker_dict["n_alive_container_knn"] + 1
            elif "guest_alu" in name:
                invoker_dict["n_alive_container_alu"] = invoker_dict["n_alive_container_alu"] + 1
            elif "guest_ms" in name:
                invoker_dict["n_alive_container_ms"] = invoker_dict["n_alive_container_ms"] + 1
            elif "guest_gd" in name:
                invoker_dict["n_alive_container_gd"] = invoker_dict["n_alive_container_gd"] + 1
            elif "guest_dv" in name:
                invoker_dict["n_alive_container_dv"] = invoker_dict["n_alive_container_dv"] + 1

    # CPU and memory utilization
    invoker_dict["memory_util"] = memory_percent
    invoker_dict["cpu_util"] = cpu_percent

    return invoker_dict

# Calulate delta version of two invoker dicts
def calculate_delta_invoker_dict(old_invoker_dict, new_invoker_dict):
    delta_invoker_dict = {}

    # CPU metrics
    delta_invoker_dict["cpu_total"] = new_invoker_dict["cpu_total"] - old_invoker_dict["cpu_total"]
    delta_invoker_dict["cpu_user"] = new_invoker_dict["cpu_user"] - old_invoker_dict["cpu_user"]
    delta_invoker_dict["cpu_nice"] = new_invoker_dict["cpu_nice"] - old_invoker_dict["cpu_nice"]
    delta_invoker_dict["cpu_kernel"] = new_invoker_dict["cpu_kernel"] - old_invoker_dict["cpu_kernel"]
    delta_invoker_dict["cpu_idle"] = new_invoker_dict["cpu_idle"] - old_invoker_dict["cpu_idle"]
    delta_invoker_dict["cpu_iowait"] = new_invoker_dict["cpu_iowait"] - old_invoker_dict["cpu_iowait"]
    delta_invoker_dict["cpu_irq"] = new_invoker_dict["cpu_irq"] - old_invoker_dict["cpu_irq"]
    delta_invoker_dict["cpu_softirq"] = new_invoker_dict["cpu_softirq"] - old_invoker_dict["cpu_softirq"]
    delta_invoker_dict["cpu_steal"] = new_invoker_dict["cpu_steal"] - old_invoker_dict["cpu_steal"]
    delta_invoker_dict["cpu_ctx_switches"] = new_invoker_dict["cpu_ctx_switches"] - old_invoker_dict["cpu_ctx_switches"]
    delta_invoker_dict["cpu_load_avg"] = new_invoker_dict["cpu_load_avg"]

    # Memory metrics
    delta_invoker_dict["memory_total"] = new_invoker_dict["memory_total"]
    delta_invoker_dict["memory_free"] = new_invoker_dict["memory_free"]
    delta_invoker_dict["memory_buffers"] = new_invoker_dict["memory_buffers"]
    delta_invoker_dict["memory_cached"] = new_invoker_dict["memory_cached"]

    # Disk metrics
    delta_invoker_dict["disk_read_count"] = new_invoker_dict["disk_read_count"] - old_invoker_dict["disk_read_count"]
    delta_invoker_dict["disk_read_merged_count"] = new_invoker_dict["disk_read_merged_count"] - old_invoker_dict["disk_read_merged_count"]
    delta_invoker_dict["disk_read_time"] = new_invoker_dict["disk_read_time"] - old_invoker_dict["disk_read_time"]
    delta_invoker_dict["disk_write_count"] = new_invoker_dict["disk_write_count"] - old_invoker_dict["disk_write_count"]
    delta_invoker_dict["disk_write_merged_count"] = new_invoker_dict["disk_write_merged_count"] - old_invoker_dict["disk_write_merged_count"]
    delta_invoker_dict["disk_write_time"] = new_invoker_dict["disk_write_time"] - old_invoker_dict["disk_write_time"]

    # Network metrics
    delta_invoker_dict["net_byte_recv"] = new_invoker_dict["net_byte_recv"] - old_invoker_dict["net_byte_recv"]
    delta_invoker_dict["net_byte_sent"] = new_invoker_dict["net_byte_sent"] - old_invoker_dict["net_byte_sent"]

    # Alive containers for each function
    delta_invoker_dict["n_alive_container_dl"] = new_invoker_dict["n_alive_container_dl"]
    delta_invoker_dict["n_alive_container_eg"] = new_invoker_dict["n_alive_container_eg"]
    delta_invoker_dict["n_alive_container_ip"] = new_invoker_dict["n_alive_container_ip"]
    delta_invoker_dict["n_alive_container_vp"] = new_invoker_dict["n_alive_container_vp"]
    delta_invoker_dict["n_alive_container_ir"] = new_invoker_dict["n_alive_container_ir"]
    delta_invoker_dict["n_alive_container_knn"] = new_invoker_dict["n_alive_container_knn"]
    delta_invoker_dict["n_alive_container_alu"] = new_invoker_dict["n_alive_container_alu"]
    delta_invoker_dict["n_alive_container_ms"] = new_invoker_dict["n_alive_container_ms"]
    delta_invoker_dict["n_alive_container_gd"] = new_invoker_dict["n_alive_container_gd"]
    delta_invoker_dict["n_alive_container_dv"] = new_invoker_dict["n_alive_container_dv"]

    # CPU and memory utilization
    delta_invoker_dict["memory_util"] = new_invoker_dict["memory_util"]
    delta_invoker_dict["cpu_util"] = new_invoker_dict["cpu_util"]

    return delta_invoker_dict

# Monitoring
def monitor(interval):
    redis_host = run_cmd('cat ../ansible/environments/distributed/hosts | grep -A 1 "\[edge\]" | grep -v "\[edge\]" | awk {}'.format("{'print $1'}"))
    redis_port = 6379
    redis_password = "openwhisk"

    redis_client = get_redis_client(redis_host, redis_port, redis_password)
    docker_client = get_docker_client()
    invoker_name = get_invoker_name()

    old_invoker_dict = get_invoker_dict(docker_client)

    while True:
        # psutil collection overhead: at most 0.01 sec
        time.sleep(interval) 

        # Periodically get new invoker dict
        new_invoker_dict = get_invoker_dict(docker_client)

        # Calculate delta
        delta_invoker_dict = calculate_delta_invoker_dict(
            old_invoker_dict=old_invoker_dict,
            new_invoker_dict=new_invoker_dict
        )

        # Write into Redis
        redis_client.hmset(invoker_name, delta_invoker_dict)
        old_invoker_dict = new_invoker_dict


if __name__ == "__main__":
    # Start monitoring
    monitor(params.MONITOR_INTERVAL)
