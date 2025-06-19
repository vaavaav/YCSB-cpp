#!/usr/bin/env python3

import json
import os
import matplotlib.pyplot as plt
import sys
import re

def truncate_to_shortest(lists):
    min_len = min(len(l) for l in lists)
    return [l[:min_len] for l in lists]

plt.rcParams['font.size'] = 30

class Metric:
    def __init__(self, name: str, label: str, unit: str, values : list):
        self.name = name
        self.label = label
        self.unit = unit
        self.values = values

    def __str__(self):
        result = [f"Metric: {self.name} ({self.label})"]
        result.append(f"  unit: {self.unit}")
        result.append(f"  values: {self.values}")
        return "\n".join(result)

    def __repr__(self):
        return self.__str__()

class Measurements:
    def __init__(self, names: list):
        self.names = names
        self.data = [[] for _ in range(len(names))]
        self.globalData = []
    def add(self, value, thread: int = None):
        if thread is None:
            self.globalData.append(value)
        elif thread < len(self.data):
            self.data[thread].append(value)
        else:
            raise ValueError(f"Thread {thread} out of range for {len(self.data)} threads")
    def addAll(self, values: list, thread: int = None):
        if thread is None:
            self.globalData = values
        elif thread < len(self.data):
            self.data[thread] = values
        else:
            raise ValueError(f"Thread {thread} out of range for {len(self.data)} threads")

    def __str__(self):
        result = ["Measurements:"]
        for name, values in zip(self.names, self.data):
            result.append(f"  {name}: {values}")
        result.append(f"  global: {self.globalData}")
        return "\n".join(result)

    def __repr__(self):
        return self.__str__()


def getHitRatio(files: [str], runs: int, threads: int, names: list):
    measurements = Measurements(names)
    # THREADS
    for i in range(threads):
        misses = []
        hits = []
        for f in files:
            if h := re.findall(rf'\[T-{i}\]:.*READ-PASSED: Count=(\d+)', f):
                hits.append([int(x) for x in h])
            else:
                hits.append(0)
            if m := re.findall(rf'\[T-{i}\]:.*READ-FAILED: Count=(\d+)', f):
                misses.append([int(x) for x in m])
            else:
                misses.append(0)
        # Flatten the lists
        hits = [sum(values) for values in zip(*hits)]
        misses = [sum(values) for values in zip(*misses)]
        previousHits = 0
        previousMisses = 0
        for h,m in zip(hits, misses):
            h = h - previousHits
            m = m - previousMisses
            measurements.add(0 if (m+h) == 0 else h / (m + h), i)
            previousHits += h
            previousMisses += m
    # GLOBAL
    totalHits = []
    totalMisses = []
    for f in files:
        if h := re.findall(r'\[GLOBAL\]:.*READ-PASSED: Count=(\d+)', f):
            totalHits.append([int(x) for x in h])
        if m := re.findall(r'\[GLOBAL\]:.*READ-FAILED: Count=(\d+)', f):
            totalMisses.append([int(x) for x in m])
    # Flatten the lists
    totalHits = [sum(values) for values in zip(*totalHits)]
    totalMisses = [sum(values) for values in zip(*totalMisses)]
    previousHits = 0
    previousMisses = 0
    for h,m in zip(totalHits, totalMisses):
        h = h - previousHits
        m = m - previousMisses
        measurements.add(0 if (m+h) == 0 else h / (m + h))
        previousHits += h
        previousMisses += m
    return Metric('Hit Ratio', 'Hit Ratio', '', measurements)

def getThroughput(files: [str], runs: int, threads: int, names: list):
    measurements = Measurements(names)
    # THREADS
    for i in range(threads):
        throughput = []
        for f in files:
            if t := re.findall(rf'\[T-{i}\]: (\d+)', f):
                throughput.append([int(x) for x in t])
        # Flatten the lists
        throughput = [sum(values) / len(values) for values in zip(*throughput)]
        previousThroughput = 0
        for t in throughput:
            t = t - previousThroughput
            measurements.add(t, i)
            previousThroughput += t
    # GLOBAL
    totalThroughput = []
    for f in files:
        if t := re.findall(r'\[GLOBAL\]: (\d+)', f):
            totalThroughput.append([int(x) for x in t])
    # Flatten the lists
    totalThroughput = [sum(values) / len(values) for values in zip(*totalThroughput)]
    previousThroughput = 0
    for t in totalThroughput:
        t = t - previousThroughput
        measurements.add(t)
        previousThroughput += t
    return Metric('Throughput', 'Throughput', 'ops/s', measurements)

def getMemory(files: [str], runs: int, threads: int, names: list):
    measurements = Measurements(names)
    totalMemory = []
    # THREADS
    for i in range(threads):
        memory = []
        for f in files:
            if m := re.findall(rf'\[T-{i}\]:.*\(.*?\/ (\d+)', f):
                memory.append([int(x) for x in m])
        # Flatten the lists
        memory = [int(sum(values) / len(values)) for values in zip(*memory)]
        totalMemory.append(memory)
        measurements.addAll(memory, i)
    # GLOBAL
    totalMemory = [sum(values) for values in zip(*totalMemory)]
    measurements.addAll(totalMemory)
    return Metric('Memory', 'Memory', 'MB', measurements)

def plot(metrics_by_setup: dict, outputDir: str):
    # Assume all setups have same threads/names
    setup_names = list(metrics_by_setup.keys())
    example_metric = next(iter(metrics_by_setup.values()))
    thread_labels = example_metric.values.names
    num_threads = len(thread_labels)

    # 1. Globally, all setups in the same plot
    plt.figure(figsize=(20, 10))
    for setup_name, metric in metrics_by_setup.items():
        plt.plot(metric.values.globalData, label=setup_name)
    plt.title(f"{example_metric.label} (Global Comparison)")
    plt.xlabel("Time")
    plt.ylabel(f"{example_metric.label} {example_metric.unit}")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(f"{outputDir}/{example_metric.name.replace(' ', '_').lower()}_global_comparison.png")
    plt.close()

    # 2. One per setup, each with threads as vertical subplots
    for setup_name, metric in metrics_by_setup.items():
        fig, axes = plt.subplots(num_threads, 1, figsize=(20, 5 * num_threads), sharex=True)
        if num_threads == 1:
            axes = [axes]
        for i in range(num_threads):
            axes[i].plot(metric.values.data[i])
            axes[i].set_title(f"{setup_name} - Thread {i} ({thread_labels[i]})")
            axes[i].set_ylabel(f"{example_metric.unit}")
            axes[i].grid(True)
        axes[-1].set_xlabel("Time")
        fig.suptitle(f"{example_metric.label} - {setup_name} (Threads)")
        plt.tight_layout()
        plt.savefig(f"{outputDir}/{example_metric.name.replace(' ', '_').lower()}_{setup_name.replace(' ', '_').lower()}_threads.png")
        plt.close()

    # 3. One per setup: stackplot of all threads
    for setup_name, metric in metrics_by_setup.items():
        thread_data_lists = metric.values.data
        time_steps = range(len(thread_data_lists[0]))  # assumes aligned
        min_len = min(len(time_steps), *(len(data) for data in thread_data_lists))
        time_steps = time_steps[:min_len]
        thread_data_lists = [data[:min_len] for data in thread_data_lists]

        plt.figure(figsize=(20, 10))
        plt.stackplot(time_steps, *thread_data_lists, labels=[f"Thread {i}" for i in range(len(thread_data_lists))])
        plt.title(f"{example_metric.label} - {setup_name} (Threads Stackplot)")
        plt.xlabel("Time")
        plt.ylabel(f"{example_metric.unit}")
        plt.legend(loc='upper left')
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(f"{outputDir}/{example_metric.name.replace(' ', '_').lower()}_{setup_name.replace(' ', '_').lower()}_stacked_threads.png")
        plt.close()
   



if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python3 PlotMotivation.py <resultsDir>")
        sys.exit(1)

    resultsDir = os.path.abspath(sys.argv[1])

    metrics = {
            'Hit Ratio': getHitRatio,
            'Throughput': getThroughput,
            'Memory': getMemory
            }

    setups = {}
    names = {}
    threads = {}
    runs = {}
    with open(f'{resultsDir}/setups.json', 'r') as f:
        setups = json.load(f)

    for setup in setups:
        runs[setup] = len(os.listdir(f'{resultsDir}/{setup}'))
        threads[setup] = setups[setup]['threadcount']
        names[setup] = []
        for i in range(threads[setup]):
            if rd := setups[setup].get(f'requestdistribution.{i}', setups[setup].get('requestdistribution', None)):
                if rd == 'zipfian':
                    if zc := setups[setup].get(f'zipfian_const.{i}', setups[setup].get('zipfian_const', None)):
                        names[setup].append(f'Zipfian ({zc})')
                    else:
                        names[setup].append('Zipfian (?)') 
                elif rd == 'uniform':
                    names[setup].append('Uniform')
                else:
                    names[setup].append(rd)
            else:
                names[setup].append("?")

    results = {}

    for metric, getMetric in metrics.items():
        results[metric] = {}
        for setup in setups:
            results[metric][setup] = []
            files = []
            for run in range(runs[setup]):
                with open(f'{resultsDir}/{setup}/{run+1}/ycsb.txt', 'r') as f:
                    files.append(f.read())
            results[metric][setup] = getMetric(files, runs[setup], threads[setup], names[setup])
        plot(results[metric], resultsDir)
