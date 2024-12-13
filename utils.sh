#!/bin/bash

#This script contains utility functions. Mounting and unmounting filesystems on PMEM and NVME devices,
#limiting memory usage, cleaning OS heap and page cache, etc.


WORKSPACE="${HOME}"


# Limit memory usage
# --$1: size (e.g., 32G)
# NOTE: a suffix (K, M, G) can be to indicate values in kilo, mega or gigabytes.
# NOTE: -1 sets to unlimited.
function limit-mem {
    echo "utils::limit-mem to $1"

    local GROUP_NAME="limitmem"
    local CONFIG="group ${GROUP_NAME} { memory { memory.limit_in_bytes = -1\; } }"
    local RULES="*:db_bench  memory ${GROUP_NAME}"
    
    # Create this files if they do not exist
    sudo touch /etc/cgrules.conf
    sudo touch /etc/cgconfig.conf

    # If cgrulesengd is not running or if the files do not 
    # have the correct configuration, create the configuration 
    # files and start cgrulesengd.
    if [[ ! $(pgrep cgrulesengd) || 
          ! $(grep "$(echo $RULES)" /etc/cgrules.conf) ||  
          ! $(grep "$(echo $CONFIG)" /etc/cgconfig.conf) ]]; 
    then
        sudo pkill cgrulesengd
        sudo cgclear -l /etc/cgconfig.conf > /dev/null
        
        sudo rm -f /etc/cgconfig.conf 
        sudo rm -f /etc/cgrules.conf
        sudo rm -f /etc/cgred.conf

        sudo cp /usr/share/doc/cgroup-tools/examples/cgred.conf /etc/
        sudo sh -c "echo $CONFIG > /etc/cgconfig.conf"
        sudo sh -c "echo $RULES > /etc/cgrules.conf"
        
        sudo cgconfigparser -l /etc/cgconfig.conf && \
        sudo cgrulesengd -vvv
    fi 

    sudo pkill db_bench
    sudo sh -c "echo $1 > /sys/fs/cgroup/memory/${GROUP_NAME}/memory.limit_in_bytes"
}

# Clean OS heap (clean page cache and swap)
function clean-heap {
    echo "utils::clean-heap"
    free -m
    sudo sync; sudo sh -c 'echo 3 > /proc/sys/vm/drop_caches'
    sudo swapoff -a
    sudo swapon -a
    free -m
}


"$@"