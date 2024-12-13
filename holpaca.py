#!/bin/python3
import subprocess

def setupController(ip: str):
    ssh = subprocess.Popen(f'ssh {ip} docker pull vaavaav/cachelib-holpaca:alpha && docker run --rm --name cachelib-holpaca -p {ip}:6000:6000 vaavaav/cachelib-holpaca:alpha controller', shell=False, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    print(ssh.stdout.realines())

setupController('192.168.112.124')