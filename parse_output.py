#!/usr/bin/env python3
#
# Copyright (c) 2020. Hitachi Vantara Corporation. All rights reserved.
#
# The copyright to the computer software herein is the property of
# Hitachi Vantara Corporation. The software may be used and/or copied only
# with the written permission of Hitachi Vantara Corporation or in accordance
# with the terms and conditions stipulated in the agreement/contract
# under which the software has been supplied.

import os
from functools import reduce
import re, csv

def cal_average(list):
    return (reduce(lambda x, y: x + y, list))/len(list)

def convert_to_gb(_list):
    temp_list = []
    for value in _list:
        MB_KB = list(filter(lambda x: x.isdigit() == True or x.isalpha(), re.split(r'(\d+)', value.strip())))
        _value = int(MB_KB[0]) if len(MB_KB) == 2 else float(".".join(MB_KB[:-1]))
        _size = MB_KB[-1]
        if _size == 'MB':
            f_value = _value / 1024
        elif _size == 'KB':
            f_value = _value / (1024 * 1024)
        else:
            f_value = _value
        temp_list.append(f_value)

    return temp_list

def process_cpu(out=None, host=None, dict={}, lock=None):
    Feild = ['HOST', 'CPU', '%user', '%nice', '%system', '%iowait', '%steal', '%idle']
    if not 'cpu' in dict.keys():
        dict['cpu'] = {}
    
    if os.path.isfile(out):
        with open(out, 'r') as fl:
            _out = (fl.readlines()[0]).split()[1:]
        
        dict['cpu'][host] = {'Feild': Feild, 'data': [
            {
                Feild[0]: host,
                Feild[1]: _out[0],
                Feild[2]: _out[1],
                Feild[3]: _out[2],
                Feild[4]: _out[3],
                Feild[5]: _out[4],
                Feild[6]: _out[5],
                Feild[7]: _out[6]
            }]}
    else:
        print("Nothing to do for CPU info")
    print("Processing DONE for cpu")

def process_memory(out=None, host=None, dict={}):
    Feild = ['HOST', 'kbmemfree', 'kbmemused', '%memused', 'kbbuffers', 'kbcached', 'kbcommit',
             '%commit', 'kbactive', 'kbinact', 'kbdirty']
    if not 'memory' in dict.keys():
        dict['memory'] = {}
    if os.path.isfile(out):
        with open(out, 'r') as fl:
            _out = (fl.readlines()[0]).split()[1:]

        dict['memory'][host] = {'Feild': Feild, 'data': [{
            Feild[0]: host,
            Feild[1]: _out[0],
            Feild[2]: _out[1],
            Feild[3]: _out[2],
            Feild[4]: _out[3],
            Feild[5]: _out[4],
            Feild[6]: _out[5],
            Feild[7]: _out[6],
            Feild[8]: _out[7],
            Feild[9]: _out[9],
            Feild[10]: _out[9]
        }]}
    else:
        print("Nothing to do for CPU info")
    print("Processing DONE for Memory")

def process_network(out=None, host=[], dict={}):
    Feild = ["HOST", 'IFACE', 'rxpck/s', 'txpck/s', 'rxkB/s', 'txkB/s', 'rxcmp/s', 'txcmp/s', 'rxmcst/s']
    if not 'network' in dict.keys():
        dict['network'] = {}
    if os.path.isfile(out):
        with open(out, 'r') as fl:
            ll = lambda x: {
                Feild[0]: host,
                Feild[1]: x[0],
                Feild[2]: x[1],
                Feild[3]: x[2],
                Feild[4]: x[3],
                Feild[5]: x[4],
                Feild[6]: x[5],
                Feild[7]: x[6],
                Feild[8]: x[7]}
            temp = [ll(line.split()[1:]) for line in fl.readlines()[1:] if
                    not line.split()[1].startswith('cali')]
            dict['network'][host] = {'Feild': Feild, 'data': temp}
    else:
        print("Nothing to do for network")
    print("Processing DONE for Network")

def process_disk_io_all_part(out=None, host=None, dict={}):
    # Stats i/o per disk partition
    Feild = ["HOST", 'DEV', 'tps', 'rd_sec/s', 'wr_sec/s', 'avgrq-sz', 'avgqu-sz', 'await', 'svctm', '%util']
    if not 'disk_io_all_part' in dict.keys():
        dict['disk_io_all_part'] = {}

    print("Processing Data for disk_io_all_part")
    if os.path.isfile(out):
        with open(out, 'r') as fl:
            ll = lambda x: {
                Feild[0]: host,
                Feild[1]: x[0],
                Feild[2]: x[1],
                Feild[3]: x[2],
                Feild[4]: x[3],
                Feild[5]: x[4],
                Feild[6]: x[5],
                Feild[7]: x[6],
                Feild[8]: x[7],
                Feild[9]: x[8]}

            temp = [ll(line.split()[1:]) for line in fl.readlines()[1:]]
            dict['disk_io_all_part'][host] = {'Feild': Feild, 'data': temp}
            print("Processing DONE for disk_io_all_part")
    else:
        print("Nothing to do for disk_io_all_part info")

def process_diskIO_block(out=None, host=None, dict={}):
    # Starting Report I/O and transfer rate statistics for all block device
    Feild = ["HOST", 'tps', 'rtps', 'wtps', 'bread/s', 'bwrtn/s']
    if not 'diskIO_block' in dict.keys():
        dict['diskIO_block'] = {}

    if os.path.isfile(out):
        with open(out, 'r') as fl:
            _out = (fl.readlines()[0]).split()[1:]
            dict['diskIO_block'][host] = {'Feild': Feild, 'data': [{
                Feild[0]: host,
                Feild[1]: _out[0],
                Feild[2]: _out[1],
                Feild[3]: _out[2],
                Feild[4]: _out[3],
                Feild[5]: _out[4]
            }]}
            print("Processing DONE for diskIO_block")
    else:
        print("Nothing to do for diskIO_block info")

def process_kube_top(out=None, host=None, dict={}):
    data = {}
    if not 'kube_top' in dict.keys():
        dict['kube_top'] = {}

    Feild = ["HOST", 'Pod Name', 'Cpu(core)', 'Memory(Mib)']
    if os.path.isfile(out):
        print("Processing data for kubectl top")
        with open(out, 'r') as fl:
            for line in fl.readlines():
                if line.startswith('NAME'):
                    continue
                _line = line.strip().split()
                if not data.get(_line[0]):
                    data[_line[0]] = {'cpu' : [], 'memory' : [], "HOST": "" }
                data[_line[0]]['cpu'].append(int(list(filter(lambda x: x.isdigit() == True, re.split(r'(\d+)', _line[1].strip())))[0]))
                data[_line[0]]['memory'].append(int(list(filter(lambda x: x.isdigit() == True, re.split(r'(\d+)', _line[2].strip())))[0]))

        csv_input = [{"HOST": host, 'Pod Name': key, 'Cpu(core)': round(cal_average(data[key]['cpu']), 3),
                      'Memory(Mib)': round(cal_average(data[key]['memory']), 3)} for key in data]

        print(f"Kube top csv_input {csv_input}")
        dict['kube_top'][host] = {'Feild': Feild, 'data': csv_input}

        print("Processing DONE for kubectl top")
    else:
        print("Nothing to do for kube top info")

def process_iostat(out=None, host=None, dict={}):
    Feild = ["HOST", 'Device', 'tps', 'MB_read/s', 'MB_wrtn/s', 'await', 'r_await', 'w_await', '%util']
    iostat_dict = {}
    if not 'iostat' in dict.keys():
        dict['iostat'] = {}

    print("Processing Data for iostat")
    if os.path.isfile(out):
        with open(out, 'r') as fl:
            for line in fl.readlines():
                if line.startswith('Linux') or line.startswith('Device:') or line.strip() == "":
                    continue
                _line = line.split()
                # print(_line)
                tps = float(_line[3]) + float(_line[4])
                if not iostat_dict.get(_line[0]):
                    iostat_dict[_line[0]] = {'tps': [], 'MB_read_s': [], 'MB_wrtn_s': [], 'await': [], 'r_await': [],
                                             'w_await': [], '%util': [], "HOST": ""}
                iostat_dict[_line[0]]['tps'].append(tps)
                iostat_dict[_line[0]]['MB_read_s'].append(float(_line[5]))
                iostat_dict[_line[0]]['MB_wrtn_s'].append(float(_line[6]))
                iostat_dict[_line[0]]['await'].append(float(_line[9]))
                iostat_dict[_line[0]]['r_await'].append(float(_line[10]))
                iostat_dict[_line[0]]['w_await'].append(float(_line[11]))
                iostat_dict[_line[0]]['%util'].append(float(_line[13]))

        # ['Device:', 'tps', 'MB_read/s','MB_wrtn/s','await','r_await','w_await','%util']
        csv_input = [
            {
                'HOST': host,
                'Device': key,
                'tps': round(cal_average(iostat_dict[key]['tps']), 3),
                'MB_read/s': round(cal_average(iostat_dict[key]['MB_read_s']), 3),
                'MB_wrtn/s': round(cal_average(iostat_dict[key]['MB_wrtn_s']), 3),
                'await': round(cal_average(iostat_dict[key]['await']), 3),
                'r_await': round(cal_average(iostat_dict[key]['r_await']), 3),
                'w_await': round(cal_average(iostat_dict[key]['w_await']), 3),
                '%util': round(cal_average(iostat_dict[key]['%util']), 3)}
            for key in iostat_dict]

        dict['iostat'][host] = {'Feild': Feild, 'data': csv_input}
        print("Processing DONE for iostat")
    else:
        print("Nothing to do for kube top info")

def process_docker_stat(out=None, host=None, dict={}):
    feild = ["HOST", 'DockerName', '%Average_CPU', '%CPU_usage(min|Max)', '%Average_Memory', '%MemoryUsage(min|Max)',
             'gb_block_read', 'gb_block_read(min|Max)', 'gb_block_wrt', 'gb_block_wrt(min|Max)']

    if not 'docker_stat' in dict.keys():
        dict['docker_stat'] = {}

    if os.path.isfile(out):

        docker_info = {}
        with open(out, 'r') as fl:
            for i in set(fl.readlines()):
                l = i.split()
                if "NAME" in l:
                    continue
                if not docker_info.get(l[1]):
                    docker_info[l[1]] = {'cpu': [], 'mem': [], 'storage_r': [], 'storage_w': [], "HOST": ""}
                docker_info[l[1]]['cpu'].append(float(l[2].split("%")[0]))
                docker_info[l[1]]['mem'].append(float(l[6].split("%")[0]))
                docker_info[l[1]]['storage_r'].append(l[10])
                docker_info[l[1]]['storage_w'].append(l[12])

        sv_input = []
        for key in docker_info:
            cpu = sorted(docker_info[key]['cpu'])
            mem = sorted(docker_info[key]['mem'])
            block_read = sorted(convert_to_gb(docker_info[key]['storage_r']))
            block_wrt = sorted(convert_to_gb(docker_info[key]['storage_w']))
            sv_input.append({'HOST': host, 'DockerName': key, '%Average_CPU': cal_average(cpu),
                             '%CPU_usage(min|Max)': f'{cpu[0]}|{cpu[-1]}',
                             '%Average_Memory': cal_average(mem),
                             '%MemoryUsage(min|Max)': f"{mem[0]}|{mem[-1]}",
                             'gb_block_read': cal_average(block_read),
                             'gb_block_read(min|Max)': f"{block_read[0]}|{block_read[-1]}",
                             'gb_block_wrt': cal_average(block_wrt),
                             'gb_block_wrt(min|Max)': f"{block_wrt[0]}|{block_wrt[-1]}"})

        dict['docker_stat'][host] = {'Feild': feild, 'data': sv_input}
    else:
        print("Nothing to do for kube top info")


def parsing_result(hostsStr):
    out_dict = {}
    log_dir = os.getcwd()
    hosts = hostsStr
    cvs_name = f"{log_dir}/performance_report.csv"
    final_csv = f"{log_dir}/final_performance_report.csv"
    #hosts = ["vmdk"]

    for host in hosts:
        os.chdir(log_dir)
        if os.path.isdir(host):
            os.chdir(f"{log_dir}/{host}")

            process_cpu(f"{host}_cpu_info.log", host, out_dict)
            process_memory(f"{host}_memory_info.log", host, out_dict)
            process_network(f"{host}_network_info.log", host, out_dict)
            process_disk_io_all_part(f"{host}_disk_io_info.log", host, out_dict)
            process_diskIO_block(f"{host}_disk_block_info.log", host, out_dict)
            process_kube_top(f"{host}_kube_top_info.log", host, out_dict)
            process_iostat(f"{host}_iostat_info.log", host, out_dict)
            process_docker_stat(f"{host}_docker_stats.log", host, out_dict)
        else:
            print(f"Directory {host} doesnot exists")

            # Parsing data to cvs
            #print(out_dict, type(out_dict), out_dict.keys(), out_dict['cpu']['data'])
    print(out_dict)
    print("Creating the CSV file")
    with open(cvs_name, 'a+', newline="") as fl:
        for key in out_dict:
            print("key",key)
            #hosts = out_dict[key].keys()
            for hostname in hosts:
                if out_dict[key].get(hostname, None):
                    header_names = out_dict[key][hostname]['Feild']
                    break
            writer = csv.DictWriter(fl, fieldnames=header_names)
            writer.writeheader()
            for host in out_dict[key]:
                #print("host", host)
                #writer = csv.DictWriter(fl, fieldnames=out_dict[key][host]['Feild'])
                ## writing headers (field names)
                #writer.writeheader()
                ## writing data rows
                #l = lambda i : host if i == 0 else " "
                #host_row = [{out_dict[key][host]['Feild'][i] : l(i) for i in range(0, len(out_dict[key][host]['Feild']))}]
                #writer.writerows(host_row)
                writer.writerows(out_dict[key][host]['data'])

    with open(cvs_name, 'r+', newline="") as f2:
        spamreader = csv.reader(f2, delimiter=',', quotechar='|')
        key = iter(list(out_dict.keys()))
        try:
            for row in spamreader:
                with open(final_csv, 'a+', newline='') as f3:
                    spamwriter = csv.writer(f3, delimiter=' ', quoting=csv.QUOTE_MINIMAL)
                    if 'HOST' in ', '.join(row):
                        print("yes")
                        spamwriter.writerow(next(key))
                    print(row)
                    spamwriter.writerow(','.join(row))
        except Exception as e:
            pass
