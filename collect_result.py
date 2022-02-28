#!/usr/bin/env python3
#
# Copyright (c) 2020. Hitachi Vantara Corporation. All rights reserved.
#
# The copyright to the computer software herein is the property of
# Hitachi Vantara Corporation. The software may be used and/or copied only
# with the written permission of Hitachi Vantara Corporation or in accordance
# with the terms and conditions stipulated in the agreement/contract
# under which the software has been supplied.

from pexpect import pxssh
import getpass
import ipaddress
import os
import time
import warnings
import paramiko
import pexpect
import platform
import re
import shutil
import socket
import subprocess
from subprocess import *
import sys
import getopt
import yaml
import parse_output

SSH_KEY_PATH = '~/.ssh'
SSH_KEY_FILE = 'id_rsa'
node_proc = []

NODE_RUN_PATH = "/var/tmp/node_stat"
FILE_POST_CPU_INFO = "_cpu_info.log"
FILE_POST_DISK_BLOCK_INFO = "_disk_block_info.log"
FILE_POST_DISK_IO_INFO = "_disk_io_info.log"
FILE_POST_IOSTAT_INFO = "_iostat_info.log"
FILE_POST_MEMORY_INFO = "_memory_info.log"
FILE_POST_NETWORK_INFO = "_network_info.log"
FILE_POST_DOCKER_STATS_INFO = "_docker_stats.log"
NODE_CONFIG_YML_FILE = "node_config.yml"
COLLECT_CONFIG_YML_FILE = "collect_config.yml"

config = {}
hostnames = []
ips = []
hosts = {}
node_settings = {}


def check_ssh_conn(key, server, username):
    """copy ssh key to remote host."""
    # here is workaround to the known issue
    # https://github.com/paramiko/paramiko/issues/1369
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        try:
            client = paramiko.SSHClient()
            client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            client.connect(server, username=username, key_filename=key)
            return True
        except Exception as e:
            print('failed to connect server {} because of error: [{}].'.format(
                server, str(e)))
            return False


def get_remote_hostname(ip, user):
    """get hostname of remote server."""
    cmd = 'hostname'
    try:
        s = pxssh.pxssh(options={
            "StrictHostKeyChecking": "no",
            "UserKnownHostsFile": "/dev/null"})
        s.login(ip, user)
        s.sendline(cmd)
        s.prompt()
        res = s.before.decode("utf-8")
        s.logout()
        return res.splitlines()[1]
    except pxssh.ExceptionPxssh as e:
        print("failed to ssh into {} with user: {}.".format(ip, user))
        print(e)
        return None


def get_ssh_key():
    """get ssh pub key file name. generate it if not exists."""
    path = os.path.expanduser(SSH_KEY_PATH)
    private_key = os.path.join(path, SSH_KEY_FILE)
    pub_key = private_key + '.pub'
    if not os.path.exists(private_key) or not os.path.exists(pub_key):
        gen_ssh_key()
    return pub_key


def push_ssh_key(key, host, username, password):
    """copy ssh key to remote host."""
    cmd = '/usr/bin/ssh-copy-id -i %s %s@%s' % (key, username, host)
    child = pexpect.spawn(cmd)
    flag = False
    try:
        while True:
            index = child.expect(
                ['continue connecting \(yes/no\)',
                 '\'s password:',
                 pexpect.EOF],
                timeout=15)
            if index == 0:
                child.sendline('yes')
            elif index == 1:
                child.sendline(password)
            elif index == 2:
                if b'key(s) you wanted were added.\r\n' in child.before:
                    flag = True
                elif b'Permission denied' in child.before:
                    print(RED_FAIL, 'Permission denied on %s' % host)
                break
    except Exception as e:
        print(e)
        return False
    finally:
        child.close()
    return flag


def gen_ssh_key():
    """generate ssh key."""
    path = os.path.expanduser(SSH_KEY_PATH)
    os.makedirs(path, exist_ok=True)
    full_name = os.path.join(path, SSH_KEY_FILE)
    cmd = 'ssh-keygen -b 2048 -t rsa -f {} -q -N ""'.format(full_name)
    subprocess.call(cmd, shell=True)


def nodes_init(docker_stats_str=""):
    """ set nodes number and get node IP / hostname """
    global hostnames, ips, node_settings
    global ips
    hosts_str = ""
    ips_str = ""
    new_hosts_str = ""
    new_ips_str = ""
    couch_pw = ""
    need_update = True

    if os.path.exists(NODE_CONFIG_YML_FILE):
        with open(NODE_CONFIG_YML_FILE, "r") as file:
            node_settings = yaml.safe_load(file)
            file.close()

    if len(node_settings) > 0:
        hosts_str = node_settings["HOSTS"]
        ips_str = node_settings["IPS"]
        new_hosts_str = hosts_str
        new_ips_str = ips_str
        docker_str = node_settings["CONTAINERS"]
        hostnames = hosts_str.split(",")
        couch_pw = node_settings["COUCH_PW"]
        ips = ips_str.split(",")
        print("Couch PW: %s" % couch_pw)
        for i in range(len(ips)):
            print("node %d, host %s, ip %s" % (i + 1, hostnames[i], ips[i]))
        while True:
            reply = str(input("Do you want to change? (y/n)")).lower().strip()
            if reply == 'y':
                need_update = True
                break
            if reply == 'n':
                need_update = False
                break;

    if need_update is True:
        new_hosts_str = ""
        new_ips_str = ""
        hostnames = []
        ips = []
        os.system('clear')
        nodes_num = int(input("Enter number of nodes: "))
        key = get_ssh_key()
        if not key:
            input('Failed to get ssh key. press any key to continue...')
            return False
        node_num = 1
        while node_num <= nodes_num:
            while 1:
                os.system('clear')
                ip = input("input ip address for node {}: ".format(node_num))
                try:
                    ipaddress.ip_address(ip)
                except Exception:
                    input("invalid ip address: %s. press any key to continue..." % ip)
                    continue

                if not check_ssh_conn(key, ip, 'root'):
                    print("ssh key is not set.")
                    print('Enter password to set ssh key...')
                    password = getpass.getpass("input root password [%s]: " % ip)
                    if not push_ssh_key(key, ip, 'root', password):
                        input('Failed to set ssh key. press any key to re-try...')
                        continue
                    else:
                        print('ssh key is set.')

                hostname = get_remote_hostname(ip, 'root')
                hostnames.append(hostname)
                ips.append(ip)
                new_hosts_str += hostname
                new_ips_str += ip
                if node_num != nodes_num:
                    new_hosts_str += ","
                    new_ips_str += ","
                break
            node_num = node_num + 1
        node_settings["COUCH_PW"] = input("input password for couchdb: ")
    node_settings["HOSTS"] = new_hosts_str
    node_settings["IPS"] = new_ips_str
    node_settings["CONTAINERS"] = docker_stats_str

    for i in range(len(ips)):
        hosts[ips[i]] = hostnames[i]
        os.system("rm -rf ./%s" % hostnames[i])

    try:
        with open(NODE_CONFIG_YML_FILE, 'w') as f_handler:
            yaml.dump(node_settings, f_handler, default_flow_style=False)
            f_handler.close()
            return True
    except IOError as ex:
        return False


def collect_docker_ps(container_name):
    for cnm in container_name:
        cmd = "ansible-playbook -i hosts.ini playbook.yml -e docker_task=%s" % cnm
        subprocess.call(cmd, shell=True)


def ssh_and_cmd(ip, cmd, block=True, stdout_collect=True, stderr_collect=True):
    str_cmd = "ssh root@%s %s" % (ip, cmd)
    print(str_cmd)
    print(block)
    try:
        proc = Popen(str_cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        #print(proc.stderr.readlines())
        if (block == True):
            while (proc.poll() == None):
                time.sleep(0.1)
        return True, proc
    except subprocess.CalledProcessError as err:
        print("ssh root@%s %s failed" % (ip, cmd))
        return False, proc


def node_run(ip):
    global config
    path = config["NODE_RUN_PATH"]
    """
    str_cmd = "rm -rf %s" % path
    result, proc = ssh_and_cmd(ip, str_cmd)
    if result is False:
        print("mkdir in %s failed" % ip)
        return False, proc
    """
    str_cmd = "mkdir %s" % path
    print(str_cmd)
    result, proc = ssh_and_cmd(ip, str_cmd)
    if result is False:
        print("mkdir in %s failed" % ip)
        return False, proc

    str_cmd = "sync %s" % path
    print(str_cmd)
    result, proc = ssh_and_cmd(ip, str_cmd)
    if result is False:
        print("sync in %s failed" % ip)
        return False, proc

    str_cmd = "scp collect_node.py node_config.yml collect_config.yml root@%s:%s/" % (ip, path)
    print(str_cmd)
    try:
        proc = Popen(str_cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    except subprocess.CalledProcessError as err:
        error = proc.stderr.readlines()
        print(error)
        return False, proc

    str_cmd = "scp -pr dispatch_latency root@%s:%s/" % (ip, path)
    print(str_cmd)
    try:
        proc = Popen(str_cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
    except subprocess.CalledProcessError as err:
        error = proc.stderr.readlines()
        print(error)
        return False, proc

    str_cmd = "sync %s" % path
    print(str_cmd)
    result, proc = ssh_and_cmd(ip, str_cmd, block=False)
    if result is False:
        print("sync in %s failed" % ip)
        return False, proc

    str_cmd = "python3 %s/collect_node.py" % path
    print(str_cmd)
    result, proc = ssh_and_cmd(ip, str_cmd, block=False)
    if result is False:
        print("python3 collect_node.py at %s failed" % ip)
        return False, proc
    return True, proc


def copy_result_to_local(ip, hostname):
    global config
    path = config["NODE_RUN_PATH"]

    str_cmd = "sync %s" % path
    result, proc = ssh_and_cmd(ip, str_cmd)
    if result is False:
        print("sync in %s failed" % ip)
        return False, proc

    cmd = "rm -rf ./%s" % hostname
    print(cmd)
    #os.rmdir("./%s" % hostname)
    os.system(cmd)
    cmd = "mkdir -p ./%s" % hostname
    print(cmd)
    os.mkdir("./%s" % hostname)

    str_cmd = "ls %s/*log" % path
    result, proc = ssh_and_cmd(ip, str_cmd)
    if result is False:
        return
    files = []
    while (1):
        line = proc.stdout.readline()
        print(line)
        if (not line):
            break;
        line = line.decode().rstrip('\n')
        if path in line:
            files.append(line)
    for line in files:
        str_cmd = "scp root@%s:%s ./%s/" % (ip, line, hostname)
        print(str_cmd)
        time.sleep(0.1)
        proc = Popen(str_cmd.split(), stdin=PIPE, stdout=PIPE, stderr=PIPE)
        print(proc.stderr.readlines())
        while proc.poll() is None:
            time.sleep(0.1)


def main(argv):
    os.system('clear')
    global config
    with open(COLLECT_CONFIG_YML_FILE, "r") as file:
        config = yaml.safe_load(file)

    try:
        opts, args = getopt.getopt(argv, "hd:", ["containers="])
    except getopt.GetoptError:
        print("collect_result.py -d container_names(separated by comma)")
        sys.exit(2)
    containers = ''
    for opt, arg in opts:
        if opt == '-h':
            print("collect_result.py -d container_names(separated by comma)")
        elif opt in ("-d", "--containers"):
            containers = arg
    if len(containers) == 0:
        print("Err: Container name is expected")
        sys.exit(2)

    if nodes_init(containers) is False:
        print("nodes init failed\n")
        return
    container_names = containers.split(",")
    print(container_names)
    print("Clear stale couchDB dispatch data, may take some time depends on data volume")
    str_cmd = "%s/dispatch_latency/couch_create_view.sh %s %s" % (
        config["NODE_RUN_PATH"], ips[0], node_settings["COUCH_PW"])
    print(str_cmd)
    ssh_and_cmd(ips[0], str_cmd)
    str_cmd = "%s/dispatch_latency/couch_delete.sh %s %s" % (config["NODE_RUN_PATH"], ips[0], node_settings["COUCH_PW"])
    ssh_and_cmd(ips[0], str_cmd)
    print(str_cmd)

    procs = {}
    results = []
    for ip in ips:
        result, proc = node_run(ip)
        #print(result)
        if result is False:
            print("node_run failed in %s" % ip)
        else:
            results.append(result)
            procs[ip] = proc
    print("****************")
    print(procs)
    while 1:
        all_done = True
        for _, (ip, proc) in enumerate(procs.items()):
            if proc.poll() is None:
                all_done = False
                print("%s is running" % ip)
                break
        if all_done is False:
            time.sleep(5)
        else:
            break

    for _, (ip, proc) in enumerate(procs.items()):
        copy_result_to_local(ip, hosts[ip])
    
    cmd = "rm -rf performance_report.csv final_performance_report.csv"
    print(cmd)
    os.system(cmd)
    parse_output.parsing_result(hostnames)
    #python3 parse_output1.py 
    #execfile('parse_output1.py')
    #os.system('parse_output1.py')

if __name__ == "__main__":
    main(sys.argv[1:])
