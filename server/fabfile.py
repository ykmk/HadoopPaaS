#coding: utf-8
from fabric.api import run,env,sudo

#env.hosts = ["157.82.3.143"]
env.user = 'root'
env.key_filename = '/home/id_rsa'

def sample():
    run("stop-all.sh")
    run("start-all.sh")
    run("hadoop jar /home/hadoop/hadoop-2.6.0/share/hadoop/mapreduce/hadoop-mapreduce-examples-2.6.0.jar pi 100 100")


def create_cluster(master,slave_list):
    pass

def make_lxc(ip):
    pass


def make_master(ip, slave_list):
    pass

def make_slave(ip):
    pass

def destroy_cluster(master,slave_list):
    pass





