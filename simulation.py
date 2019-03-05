#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Intakes a csv file of simulation data, creates a working CSV file, and uses
the data to simulate"""

import random
import urllib2
import logging
import argparse
import csv

class Queue:
    def __init__(self):
        self.items = []

    def is_empty(self):
        return self.items == []

    def enqueue(self, item):
        self.items.insert(0,item)

    def dequeue(self):
        return self.items.pop()

    def size(self):
        return len(self.items)

class Server_req:
    def __init__(self, ppm):
        self.page_rate = ppm
        self.current_task = None
        self.time_remaining = 0

    def tick(self):
        if self.current_task != None:
            self.time_remaining = self.time_remaining - 1
            if self.time_remaining <= 0:
                self.current_task = None

    def busy(self):
        if self.current_task != None:
            return True
        else:
            return False

    def start_next(self, new_task):
        self.current_task = new_task
        self.time_remaining = new_task.get_seconds() * 60/self.page_rate

class Request:
    def __init__(self, time, seconds):
        self.timestamp = time
        self.seconds = seconds

    def get_stamp(self):
        return self.timestamp

    def get_seconds(self):
        return self.seconds

    def wait_time(self, current_time):
        return current_time - self.timestamp
    
def simulateOneServer(sim_file):
    """simulateOneServer Function - intakes a list of server request data and
    simulates requests through a server.
    Args:
        sim_file (list): A list of objects to be simulated
    Output: Average wait time and tasks remaining
    Example:
        simulateOneServer(new_csv_file)
        >>>Average Wait 5003.00 secs   2 tasks remaining.
    """
    cur_req = 0
    a_wait_time = 0
    server_tsk_remain = 0
    a_waiting_lst = []
    with open(sim_file, mode = 'r') as outfile:
        b_reader = csv.reader(outfile)
        for row in b_reader:
            #print row
            if row[0] == cur_req:
                a_wait_time, server_tsk_remain = simulation(cur_req, row[1])
            else:
                cur_req = row[0]
            a_waiting_lst.append(a_wait_time)
    outfile.close
    total_wait = sum(a_waiting_lst) / len(a_waiting_lst)
    print("Average Wait %6.2f secs %3d tasks remaining."
        %(total_wait, server_tsk_remain))
    return

def simulateManyServers(sim_file, server_amt):
    """simulateOneServer Function - intakes a list of server request data and
    simulates requests through a server.
    Args:
        sim_file (list): A list of objects to be simulated
        server_amt (int): How many servers to simulate
    Output: Average wait time and tasks remaining
    Example:
        simulateOneServer(new_csv_file)
        >>>Average Wait 5003.00 secs   2 tasks remaining.
    """
    cur_req = 0
    req_ct = 0
    a_wait_time = 0
    server_tsk_remain = 0
    a_waiting_lst = []
    svrs_queue = Queue()
    svrs_dict = {}
    total_svr_wait = 0
    for serv in range(server_amt):
        svrs_queue.enqueue(serv)
        svrs_dict.update({serv: (0,0,0)})
    with open(sim_file, mode = 'r') as outfile:
        b_reader = csv.reader(outfile)
        for row in b_reader:
            if row[0] == cur_req:
                a_wait_time, server_tsk_remain = simulation(cur_req, row[1])
                req_ct += 1
            else:
                cur_req = row[0]
            a_waiting_lst.append(a_wait_time)
            last_svr = svrs_queue.dequeue()
            svrs_queue.enqueue(last_svr)
            svrs_dict.update({last_svr: (a_wait_time,req_ct, server_tsk_remain)})
    outfile.close
    for row in svrs_dict:
        total_svr_wait += svrs_dict[row][0]
    total_svr_wait = total_svr_wait / len(svrs_dict)
    total_wait = sum(a_waiting_lst) / len(a_waiting_lst)
    print("Average Many Wait %6.2f secs %3d tasks remaining."
        %(total_svr_wait, server_tsk_remain))
    return

def simulation( num_seconds,req_num):
    """simulation Function - intakes an object and begins to build a queue
    which is then processed to simulate server requests
    Args:
        num_seconds (int): An int which represents the queue num
        req_num (int): An int that represents how long the row takes to process
    Output: 99, 2
    Example:
        simulation(cur_req, row[1])
        >>>(99, 2)
    """
    num_seconds = int(num_seconds)
    req_num = int(req_num)
    server = Server_req(num_seconds)
    server_queue = Queue()
    waiting_times = 0
    for current_second in range(req_num):
        task = Request(current_second, num_seconds)
        server_queue.enqueue(task)
        if (not server.busy()) and (not server_queue.is_empty()):
            next_task = server_queue.dequeue()
            waiting_times = next_task.wait_time(num_seconds)
            server.start_next(next_task)
        server.tick()
    return (waiting_times, server_queue.size())
    return (average_wait, server_queue.size())
  
def main():
    """Main Function - Takes a URL passed from the command line
    and creates a CSV file which is used to simulate server requests.
    Args: --csvFile
    Output: True or False 
    Example:
        $ python simulation.py

        >>>Sequential Search on average took  0.0000615 seconds to run 500
        records 0.0002298 to run 1000 records,
        and  0.0005938 to run 2500 records.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--File", help = "Explicit location of the csv file"
                        "you'd like to parse", type = str)
    parser.add_argument("--servers", help = "How many servers to use in sim"
                        , type = int)
    args = parser.parse_args()
    url = args.File
    server_amt = args.servers
    #'http://s3.amazonaws.com/cuny-is211-spring2015/requests.csv'
    new_csv_file = 'ServerTraffic.csv'
    req = urllib2.Request(url)
    response = urllib2.urlopen(req)
    a_file = csv.reader(response)
    a_list_time = []
    a_list_time = ([row[0],row[2]] for row in a_file)
    with open(new_csv_file, mode = 'w') as outfile:
        writer = csv.writer(outfile)
        for row in a_list_time:
            writer.writerow(row)
    outfile.close
    if server_amt > 1:
        simulateManyServers(new_csv_file, server_amt)
    else:
        simulateOneServer(new_csv_file)

main()
