#!/usr/bin/env python3
#
# Copyright (c) 2020. Hitachi Vantara Corporation. All rights reserved.
#
# The copyright to the computer software herein is the property of
# Hitachi Vantara Corporation. The software may be used and/or copied only
# with the written permission of Hitachi Vantara Corporation or in accordance
# with the terms and conditions stipulated in the agreement/contract
# under which the software has been supplied.

from subprocess import Popen, PIPE
from multiprocessing import Process, Manager, Lock
import csv
import time
import re
from functools import reduce
import datetime
import os
import yaml
import netifaces

NODE_RUN_PATH = "/var/tmp/node_stat"
NODE_CONFIG_YML_FILE = "node_config.yml"
COLLECT_CONFIG_YML_FILE = "collect_config.yml"

hostname = ""
config = {}
node_config = {}

def cal_average(list):
    return (reduce(lambda x, y: x + y, list))/len(list)


def run_command(cmd):
    print(f"running cmd : {cmd}")
    out = Popen(cmd, shell=True, encoding=False, stdout=PIPE, stderr=PIPE)
    stdout, stderr = out.communicate()
    if stderr:
        print(f"Stderr: {stderr.decode('utf-8').strip()}")
        return False
    # print(f"Stderr: {stdout.decode('utf-8').strip()}")
    return stdout.decode('utf-8').strip()


def create_log_file(data, log_file):
    try:
        with open(log_file, 'w') as fl:
            fl.write(str(data))
    except (IOError, FileNotFoundError):
        print(f"Failed to wrrite data file: {log_file}")
        print(data)


def monitor_cpu(interval_in_sec=1, timeout_in_sec=1, dict={}, lock=None, log=None):
    cmd = "sar {} {}|grep Average".format(interval_in_sec, timeout_in_sec)
    print('****before running cpu command')
    out = run_command(cmd)
    print('****after running cpu command')
    filename = hostname + config["FILE_POST_CPU_INFO"]
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line)
            f_handler.close()
    except IOError as ex:
        print("Collect CPU info failed")
        return
    if log:
        create_log_file(out, log)
    print("CPU info collected, save to %s" % filename)


def monitor_memory(interval_in_sec=1, timeout_in_sec=1, dict={}, lock=None, log=None):
    cmd = "sar -r {} {}|grep Average".format(interval_in_sec, timeout_in_sec)
    out = run_command(cmd)
    filename = hostname + config["FILE_POST_MEMORY_INFO"]
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line)
            f_handler.close()
    except IOError as ex:
        print("Collect memory info failed")
        return
    if log:
        create_log_file(out, log)
    print("Memory info collected, save to %s" % filename)


def monitor_network(interval_in_sec=1, timeout_in_sec=1, NIC=['ALL'], dict={}, lock=None, log=None):
    cmd = "sar -n DEV {} {}|grep Average".format(interval_in_sec, timeout_in_sec)
    out = run_command(cmd).splitlines()
    filename = hostname + config["FILE_POST_NETWORK_INFO"]
    try:
         with open(filename, 'w') as f_handler:
             for line in out:
                f_handler.write(line + "\n")
             f_handler.close()
    except IOError as ex:
        print("Collect network info failed")
        return
    if log:
        create_log_file(out, log)
    print("Network info collected, save to %s" % filename)


def monitor_disk_io_all_part(interval_in_sec=1, timeout_in_sec=1, dict={}, lock=None, log=None):
    cmd = "sar -dp {} {}|grep Average".format(interval_in_sec, timeout_in_sec)
    out = run_command(cmd)
    filename = hostname + config["FILE_POST_DISK_IO_INFO"]
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line)
            f_handler.close()
    except IOError as ex:
        print("Collect disk io info failed")
        return
    if log:
        create_log_file(out, log)
    print("Disk io info collected, save to %s" % filename)


def monitor_diskIO_block(interval_in_sec=1, timeout_in_sec=1, dict={}, lock=None, log=None):
    cmd = "sar -b {} {}|grep Average".format(interval_in_sec, timeout_in_sec)
    out = run_command(cmd)
    filename = hostname + config["FILE_POST_DISK_BLOCK_INFO"]
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line)
            f_handler.close()
    except IOError as ex:
        print("Collect disk block info failed")
        return
    if log:
        create_log_file(out, log)
    print("Disk disk info collected, save to %s" % filename)


def monitor_kube_top(interval_in_sec=1, timeout_in_sec=1, dict={}, lock=None, log=None):
    print('#####in monitor kube top process')
    sample_interval=15
    cmd = "kubectl top pods -n hiota"
    out = []
    for _, (k, v) in enumerate(config.items()):
        print(k + ": " + v)
    filename = hostname + config["FILE_POST_KUBE_TOP_INFO"]

    for i in range(int(timeout_in_sec/sample_interval)):
        _out = run_command(cmd)
        if not _out:
            out.extend([0,0,0])
        else:
            out.extend(_out.splitlines())
        time.sleep(sample_interval)
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line + "\n")
            f_handler.close()
    except IOError as ex:
        print("Collect disk block info failed")
        return
    if log:
        create_log_file(out, log)
    print("Kubectl top info collected, save to %s" % filename)


def monitor_iostat(interval_in_sec=1, timeout_in_sec=1, dict={}, lock=None, log=None):
    cmd = f"iostat -dNmzx {interval_in_sec} {timeout_in_sec}"
    out = run_command(cmd).splitlines()
    filename = hostname + config["FILE_POST_IOSTAT_INFO"]
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line + "\n")
            f_handler.close()
    except IOError as ex:
        print("Collect iostat info failed")
        return
    if log:
        create_log_file(out, log)
    print("Disk iostat info collected, save to %s" % filename)


def monitor_containers(containers_postfixs, log=None):
    cmd = "docker ps"
    out = run_command(cmd).splitlines()
    container_names = containers_postfixs.split(",")
    container_ids = []
    for line in out:
        container_id = line.split()[0]
        for container_name in container_names:
            if ("k8s_" + container_name) in line:
                if container_id not in container_ids:
                    container_ids.append(container_id)
    if len(container_ids) == 0:
        return

    out = []
    for container_id in container_ids:
        cmd = "docker stats %s --no-stream" % container_id
        _out = run_command(cmd)
        if _out:
            out.extend(_out.splitlines())
    filename = hostname + config["FILE_POST_DOCKER_STATS_INFO"]
    try:
        with open(filename, 'w') as f_handler:
            for line in out:
                f_handler.write(line + "\n")
            f_handler.close()
    except IOError as ex:
        print("Collect docker stats info failed")
        return
    if log:
        create_log_file(out, log)
    print("Docker stats info collected, save to %s" % filename)


def collect_dispatch_data():
    # Just in case to create, may exist already, but no harm
    cmd_line = "./dispatch_latency/couch_query_postdispatch.sh %s %s" % (node_settings["IPS"].split(",")[0], node_settings["COUCH_PW"])
    cmd_line += " > %s%s" % (node_settings["HOSTS"].split(",")[0], config["FILE_POST_POSTDISPATCH_LATENCY"])
    proc = Popen(cmd_line, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    while proc.poll() is None:
        time.sleep(0.1)

    cmd_line = "./dispatch_latency/couch_query_predispatch.sh %s %s" % (node_settings["IPS"].split(",")[0], node_settings["COUCH_PW"])
    cmd_line += " > %s%s" % (node_settings["HOSTS"].split(",")[0], config["FILE_POST_PREDISPATCH_LATENCY"])
    proc = Popen(cmd_line, stdin=PIPE, stdout=PIPE, stderr=PIPE, shell=True)
    while proc.poll() is None:
        time.sleep(0.1)


def main():
    global hostname, config, node_settings
    path = NODE_RUN_PATH
    hostname = os.uname()[1]
    os.chdir(path)
    os.system('rm *.log')

    try:
        with open(COLLECT_CONFIG_YML_FILE, "r") as file:
            config = yaml.safe_load(file)
            file.close()
    except IOError as ex:
        print("Open file %s failed" % COLLECT_CONFIG_YML_FILE)
        return

    try:
        with open(NODE_CONFIG_YML_FILE, "r") as file:
            node_settings = yaml.safe_load(file)
            file.close()
    except IOError as ex:
        print("Open file %s failed" % NODE_CONFIG_YML_FILE)
        return

    start_time = time.time()
    date = datetime.datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    data_type = "json_minio"
    datasize = "medium"
    target_dir = "/home/bench/performance/{0}/{1}/{2}/".format(date, data_type, datasize)
    # load time in seconds
    load_time_in_sec = 3600
    interval_in_sec = 1

    hostnames = node_settings["HOSTS"].split(",")
    NIC = []
    ip = ""
    ips = node_settings["IPS"].split(",")
    for i in range(len(hostnames)):
        if hostname == hostnames[i]:
            ip = ips[i]
    ifs = netifaces.interfaces()
    match = False
    for nic in ifs:
        addrs = netifaces.ifaddresses(nic)[netifaces.AF_INET]
        for addr in addrs:
            for _, (k, v) in enumerate(addr.items()):
                if k == "addr" and v == ip:
                    NIC.append(nic)
                    print("interfae: %s, ip: %s" % (nic, ip))
                    match = True
                    break
            if match is True:
                break
        if match is True:
            break

    max_thread_time = load_time_in_sec

    lock = Lock()

    try:
        os.makedirs(target_dir, exist_ok=True)
    except:
        target_dir = os.getcwd()
    csv_file = target_dir + 'Performace.csv'
    print(f"CVSFILE PATH : {csv_file}")

    manager = Manager()
    shared_dict = manager.dict()

    containers = Process(target=monitor_containers, args=(node_settings["CONTAINERS"], target_dir + 'docker_stats'))
    kube_top = Process(target=monitor_kube_top, args=(interval_in_sec, load_time_in_sec, shared_dict, lock, target_dir +'kube_top'))
    cpu = Process(target=monitor_cpu, args=(interval_in_sec, load_time_in_sec, shared_dict, lock, target_dir +'cpu'))
    mem = Process(target=monitor_memory, args=(interval_in_sec, load_time_in_sec, shared_dict, lock, target_dir +'mem'))
    network = Process(target=monitor_network, args=(interval_in_sec, load_time_in_sec, NIC, shared_dict, lock, target_dir +'network'))
    diskIO_block = Process(target=monitor_diskIO_block, args=(interval_in_sec, load_time_in_sec, shared_dict, lock, target_dir +'diskIO_block'))
    disk_io_all_part = Process(target=monitor_disk_io_all_part, args=(interval_in_sec, load_time_in_sec, shared_dict, lock, target_dir +'disk_io_all_part'))
    iostat = Process(target=monitor_iostat, args=(interval_in_sec, load_time_in_sec, shared_dict, lock, target_dir +'iostat'))

    if hostname == node_settings["HOSTS"].split(",")[0]:
        print('****starting kube top')
        kube_top.start()
    cpu.start()
    mem.start()
    network.start()
    diskIO_block.start()
    disk_io_all_part.start()
    iostat.start()
    containers.start()

    if hostname == node_settings["HOSTS"].split(",")[0]:
        print('****joining kube top')
        kube_top.join(timeout=max_thread_time)
    cpu.join(timeout=max_thread_time)
    mem.join(timeout=max_thread_time)
    network.join(timeout=max_thread_time)
    diskIO_block.join(timeout=max_thread_time)
    disk_io_all_part.join(timeout=max_thread_time)
    iostat.join(timeout=max_thread_time)
    containers.join(timeout=max_thread_time)

    if hostname == node_settings["HOSTS"].split(",")[0]:
        collect_dispatch_data()

    print(f"Script took {time.time()-start_time} seconds to finish")

if __name__ == "__main__":
    main()
