#!/usr/bin/env python3

import io
from math import e
import os
import sys
import json
import re
from cycler import Cycler, cycler
import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from itertools import dropwhile

from pyparsing import line
from requests import get

plt.rcParams['font.family'] = ['NewsGotT', 'Iosevka']
plt.rcParams['font.size'] = 30
plt.rcParams['xtick.major.pad'] = 10

latencies = ['Min', 'Max', 'Avg', '90', '99', '99.9', '99.99']
metricsConfig = {
    'hit-ratio': {
        'label':
        'Hit Ratio',
        'name':
        'hit ratio',
        'yTicksFormatter':
        lambda tick, _: f'{round(tick, 2)}'.rstrip('0').rstrip('.'),
    },
    'throughput': {
        'label':
        'Throughput (Kops/s)',
        'name':
        'throughput',
        'yTicksFormatter':
        lambda tick, _: f'{round(tick/1000, 1)}'.rstrip('0').rstrip('.'),
    },
#    'memory': {
#        'label':
#        'Pool Size (GB)',
#        'name':
#        'pool size',
#        'yTicksFormatter':
#        lambda tick, _: '{:.2f}'.format(tick / 1_000_000_000).rstrip('0').
#        rstrip('.'),
#    },
#    'relative-memory': {
#        'label':
#        'Relative Pool Size',
#        'name':
#        'relative pool size',
#        'yTicksFormatter':
#        lambda tick, _: f'{round(tick * 100, 0)}'.rstrip('0').rstrip('.') +
#        '%',
#    },
    'cpu': {
        'label':
        'CPU Usage',
        'name':
        'cpu usage',
        'yTicksFormatter':
        lambda tick, _: f'{round(tick, 0)}'.rstrip('0').rstrip('.') + '%'
    }
}
latDict = {
    'Avg': 'average',
    'Min': 'minimum',
    'Max': 'maximum',
    '90': '90th percentile',
    '99': '99th percentile',
    '99.9': '99.9th percentile',
    '99.99': '99.99th percentile'
}
for l in latencies:
    metricsConfig[f'latency-{l.lower()}'] = {
        'label':
        f"{ 'P' + l if l.isnumeric() else l} lookup latency (ms)",
        'name':
        f"{latDict[l]} lookup latency",
        'yTicksFormatter':
        lambda tick, _: '{:.1f}'.format(tick / 1_000).rstrip('0').rstrip('.'),
    }


def getHitRatio(profilePath: str, runs: int, threads: int, totalExecutionTime):
    hits = [[] for _ in range(threads)]  # last one is the global
    reads = [[] for _ in range(threads)]
    files = [open(f'{profilePath}/{run}/ycsb.txt', 'r') for run in range(runs)]
    for lineFiles in zip(*files):
        for i in range(threads):
            r = 0
            h = 0
            for line in lineFiles:
                if result := re.search(rf'worker-{i} {{([^{{}}]*)}}', line):
                    if readSuccess := re.search(
                            r'READ-PASSED: Count=(\d+)',
                            #if readSuccess := re.search(r'READ: Count=(\d+)',
                            result.group(1)):
                        h += int(readSuccess.group(1))
                    #    r += int(readSuccess.group(1))
                    if rs := re.search(r'READ: Count=(\d+)', result.group(1)):
                        r += int(rs.group(1))
                        #if ms := re.search(r'READ-FAILED: Count=(\d+)',
                        #                   result.group(1)):
                    #    r += int(ms.group(1))
            hits[i].append(h / runs)
            reads[i].append(r / runs)
    for file in files:
        file.close()
    hits = [[f - i for i, f in zip(hits[i][:], hits[i][1:])]
            for i in range(threads)]
    reads = [[f - i for i, f in zip(reads[i][:], reads[i][1:])]
             for i in range(threads)]
    results = [[h / r if r > 0 else 0 for h, r in zip(hits[i], reads[i])]
               for i in range(threads)]
    results.append([
        h / r if r > 0 else 0 for h, r in zip([np.sum(x) for x in zip(
            *hits)], [np.sum(x) for x in zip(*reads)])
    ])  # global
    for i in range(threads + 1):
        results[i] = results[i][1:totalExecutionTime]
    return {'perPool': results[:-1], 'overall': results[-1]}


def getMemory(profilePath: str, runs: int, threads: int, totalExecutionTime):
    results = [[] for _ in range(threads + 1)]
    files = [open(f'{profilePath}/{run}/mem.txt', 'r') for run in range(runs)]
    maxCacheSizes = 0
    if result := re.search(rf'cacheSize: (\d+)', next(zip(*files))[0]):
        maxCacheSizes = (int(result.group(1))
                         )  # assuming all runs have the same cache size
    for lineFiles in zip(*files):
        gMemory = 0
        for i in range(threads):
            memory = 0
            for line in lineFiles:
                if result := re.search(rf'pool-{i} {{[^{{}}]*usedMem=(\d+)',
                                       line):
                    memory += int(result.group(1))
            results[i].append(memory / runs)
            gMemory += memory
        results[-1].append(gMemory / runs)
    for file in files:
        file.close()
    for i in range(threads + 1):
        results[i] = results[i][-totalExecutionTime:]
    return {
        'memory': {
            'perPool': results[:-1],
            'overall': results[-1]
        },
        'relative-memory': {
            'perPool':
            [[m / maxCacheSizes for m in results[i]] for i in range(threads)],
            'overall': [m / maxCacheSizes for m in results[-1]]
        }
    }


def getThroughput(profilePath: str, runs: int, threads: int,
                  totalExecutionTime):
    results = [[] for _ in range(threads + 1)]  # last one is the global
    files = [open(f'{profilePath}/{run}/ycsb.txt', 'r') for run in range(runs)]
    for lineFiles in zip(*files):
        gThroughput = 0
        for i in range(threads):
            throughput = 0
            for line in lineFiles:
                if result := re.search(rf'worker-{i} {{ (\d+) operations',
                                       line):
                    throughput += int(result.group(1))
            results[i].append(throughput / runs)
            gThroughput += throughput
        results[-1].append(gThroughput / runs)
    for file in files:
        file.close()
    for i in range(threads + 1):
        results[i] = results[i][:totalExecutionTime]
    results = [[f - i for i, f in zip(results[i], results[i][1:])]
               for i in range(threads + 1)]
    return {'perPool': results[:-1], 'overall': results[-1]}


def getLookupLatencies(profilePath: str, runs: int, threads: int,
                       totalExecutionTime, phases):
    results = {l: [[] for _ in range(threads + 1)] for l in latencies}
    files = [open(f'{profilePath}/{run}/ycsb.txt', 'r') for run in range(runs)]
    for lineFiles in zip(*files):
        for l in latencies:
            latency = 0
            for line in lineFiles:
                if result := re.search(
                        rf'global {{[^{{}}]*READ:[^\[\]]*{l}=(\d+(?:\.\d+)?)',
                        line):
                    latency += float(result.group(1))
            results[l][threads].append(latency / runs)
            for i in range(threads):
                latency = 0
                for line in lineFiles:
                    if result := re.search(
                            rf'worker-{i} {{[^{{}}]*READ:[^\[\]]*{l}=(\d+(?:\.\d+)?)',
                            line):
                        latency += float(result.group(1))
                results[l][i].append(latency / runs)
    for file in files:
        file.close()
    for l in latencies:
        for i in range(threads):
            results[l][i][phases[threads + i]:totalExecutionTime] = [
                0.0 for _ in range(totalExecutionTime - phases[threads + i])
            ]
            results[l][i] = results[l][i][:totalExecutionTime]
    return {
        f'latency-{l.lower()}': {
            'perPool': results[l][:-1],
            'overall': results[l][-1]
        }
        for l in latencies
    }


def getCPUUsage(
    profilePath: str,
    runs: int,
    threads: int,
    totalExecutionTime: int,
):
    results = []
    for run in range(runs):
        with open(f'{profilePath}/{run}/dstat.csv', 'r') as csvfile:
            data = csvfile.read().replace('|', ' ')
            cols = [
                'io.read', 'io.write', 'cpu.usr', 'cpu.sys', 'cpu.idl',
                'cpu.wai', 'cpu.stl', 'dsk.read', 'dsk.write', 'pag.in',
                'pag.out', 'mem.used', 'mem.free', 'mem.buff', 'mem.cach',
                'net.recv', 'net.send'
            ]
            df = pd.read_csv(io.StringIO(data),
                             delim_whitespace=True,
                             skiprows=2,
                             header=None,
                             names=cols)
            results.append(df['cpu.usr'].values[:totalExecutionTime])
    results = np.array(results).mean(axis=0)
    return {'overall': results}


def plotGlobalMetric(results, outputPath: str, phases, metricsId: str,
                     metricName: str, metricLabel: str, implementationNames,
                     yTicksFormatter):
    df = pd.DataFrame().from_dict(
        {
            implementationNames[k]: v
            for k, v in results.items()
        },
        orient='index').transpose()
    df.fillna(0, inplace=True)
    plt.figure(figsize=(15, 7))
    plt.xlim(0, phases[-1])
    plt.gca().set_prop_cycle(
        cycler('color', [
            'red', 'green', 'blue', 'orange', 'purple', 'brown', 'pink',
            'gray', 'black'
        ]))
    plt.ylim(0, df.values.max())
    plt.plot(df.index, df.values, linewidth=2.0)
    plt.xlabel('Time (s)', fontdict={'weight': 'bold'}, fontsize=28)
    plt.yticks(np.arange(0, df.values.max() * 1.09, df.values.max() / 10))
    plt.xticks(phases)
    plt.gca().yaxis.set_major_formatter(yTicksFormatter)
    for p in phases:
        plt.axvline(p, color='gray', linestyle='--', linewidth=0.25)
    plt.xticks(phases)
    plt.ylabel(metricLabel, fontdict={'weight': 'bold'})
    plt.legend(implementationNames.values())
    plt.title(f'Overall {metricName}', fontsize=30, weight='bold', y=1)
    phases_label = iter(['Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò', 'Ó'])
    for p in phases:
        plt.annotate(next(phases_label),
                     xy=(p, 0),
                     xytext=(0, -1.6),
                     textcoords='offset fontsize',
                     ha='center',
                     va='center',
                     font='D050000L',
                     size=30)
    plt.grid(True, linestyle='--', linewidth=0.25)
    plt.tight_layout()
    plt.savefig(f'{outputPath}/overall-{metricsId}.pdf',
                bbox_inches='tight',
                format='pdf',
                pad_inches=0)
    plt.savefig(f'{outputPath}/overall-{metricsId}.png',
                bbox_inches='tight',
                transparent=True,
                pad_inches=0)
    plt.close()


def plotMetricPerOtherMetric(implementationName: str, metric, metricId,
                             metricName: str, metricLabel: str, otherMetric,
                             otherMetricId: str, otherMetricName: str,
                             otherMetricLabel: str, threads: int,
                             outputPath: str, distributions, ticksFormatter,
                             otherMetricTicksFormatter):
    plt.figure(figsize=(15, 7))
    plt.xlabel(otherMetricLabel, fontdict={'weight': 'bold'})
    plt.ylabel(metricLabel, fontdict={'weight': 'bold'})
    plt.gca().yaxis.set_major_formatter(ticksFormatter)
    plt.gca().xaxis.set_major_formatter(otherMetricTicksFormatter)
    plt.title(f'{implementationName}\'s {metricName} per {otherMetricName}',
              fontsize=30,
              weight='bold',
              y=1.05)
    plt.grid(True, linestyle='--', linewidth=0.25)
    for i in range(threads):
        df = pd.DataFrame(metric[i], index=otherMetric[i]).sort_index()
        plt.plot(df.index,
                 df.values,
                 label=f'Tenant {i+1}: {distributions[i]}',
                 linewidth=2.0)
    plt.legend(loc='upper left')
    plt.savefig(f'{outputPath}/{metricId}-per-{otherMetricId}.pdf',
                bbox_inches='tight',
                format='pdf',
                pad_inches=0)
    plt.savefig(f'{outputPath}/{metricId}-per-{otherMetricId}.png',
                bbox_inches='tight',
                transparent=True,
                pad_inches=0)
    plt.close()


def plotHitRatioWith(implementationName: str,
                     hitRatio,
                     hitRatioMax,
                     otherMetricId: str,
                     otherMetricName: str,
                     otherMetricLabel: str,
                     otherMetric,
                     otherMetricMax,
                     threads: int,
                     outputPath: str,
                     phases,
                     totalExecutionTime,
                     distributions,
                     otherMetricYTicksFormatter,
                     qos=None):
    fig, ax = plt.subplots(threads, 1, sharex=True)
    fig.subplots_adjust(hspace=0.2, wspace=0.1, bottom=0.07, top=0.9)
    fig.set_size_inches(28, 14)
    fig.suptitle(
        t=f'{implementationName}\'s per-tenant hit ratio and {otherMetricName}',
        fontsize=40,
        weight='bold',
        y=0.94)
    ometric = pd.DataFrame(otherMetric).transpose()
    ometric.fillna(0, inplace=True)
    hr = pd.DataFrame([hitRatio[i] for i in range(threads)]).transpose()
    fig.text(0.08,
             0.5,
             otherMetricLabel,
             va='center',
             rotation='vertical',
             weight='bold')
    fig.text(0.94,
             0.5,
             'Hit Ratio',
             va='center',
             rotation='vertical',
             ha='center',
             weight='bold')
    colors = plt.rcParams['axes.prop_cycle'].by_key()['color'][:threads]
    for i in range(threads):
        ax[i].set_ylim(0, otherMetricMax[i])
        ax[i].set_xlim(0, totalExecutionTime)
        ax[i].fill_between(ometric.index,
                           0,
                           ometric.values.T[i],
                           alpha=0.25,
                           color=colors[i])
        ax[i].margins(0)
        ax[i].set_yticks(
            np.arange(0, otherMetricMax[i] * 1.25, otherMetricMax[i] / 4))
        ax[i].yaxis.set_major_formatter(otherMetricYTicksFormatter)
        for p in phases:
            ax[i].axvline(p, color='gray', linestyle='--', linewidth=0.25)
        ax[i].set_xticks(phases)
        ax2 = ax[i].twinx()
        ax2.plot(hr.index, hr[i], color=colors[i], linewidth=3.0)
        ax2.plot([], [], ' ', label=f'Tenant {i+1}: {distributions[i]}')
        for p in (np.arange(0, hitRatioMax[i] * 1.25, hitRatioMax[i] / 4)):
            ax2.axhline(p, color='gray', linestyle='--', linewidth=0.25)
        ax2.set_ylim(0, hitRatioMax[i])
        ax2.set_xlim(0, totalExecutionTime)
        ax2.margins(0)
        ax2.locator_params(axis='y', nbins=4)
        ax2.set_yticks(np.arange(0, hitRatioMax[i] * 1.25, hitRatioMax[i] / 4))
        ax2.yaxis.set_major_formatter(
            metricsConfig['hit-ratio']['yTicksFormatter'])
        # this needs to be in axis2 to make the text appear on top of the lines
        for pi, pf in zip(phases, phases[1:]):
            text = ometric.loc[pi + 1:pf - 1, i].mean()
            text = otherMetricYTicksFormatter(text, 0)
            ax2.text((pi + pf) / 2, 0.825 * hitRatioMax[i], text, ha='center')
        if qos is not None and i in qos and qos[i] <= hitRatioMax[i]:
            ax2.axhline(qos[i], color='red', linestyle='--', linewidth=1.5)
            t = ax2.annotate(f'QoS: {round(qos[i],2)}'.rstrip('0').rstrip('.'),
                             xy=(totalExecutionTime, qos[i]),
                             xytext=(-1.75, 0.2),
                             textcoords='offset fontsize',
                             weight='bold',
                             color='red',
                             va='center',
                             ha='right')
            t.set_bbox(dict(facecolor='white', alpha=0.9, linewidth=0))
        ax2.legend(handlelength=0,
                   handletextpad=0,
                   loc='lower left',
                   fancybox=True)
    phases_label = iter(['Ê', 'Ë', 'Ì', 'Í', 'Î', 'Ï', 'Ð', 'Ñ', 'Ò', 'Ó'])
    ax[threads - 1].set_xlabel('Time (s)', fontdict={'weight': 'bold'})
    for p in phases:
        ax[threads - 1].annotate(next(phases_label),
                                 xy=(p, 0.018),
                                 xycoords=('data', 'subfigure fraction'),
                                 ha='center',
                                 va='center',
                                 font='D050000L',
                                 size=40)
    fig.savefig(f'{outputPath}/hit-ratio-with-{otherMetricId}.pdf',
                format='pdf',
                bbox_inches='tight',
                pad_inches=0.0)
    fig.savefig(f'{outputPath}/hit-ratio-with-{otherMetricId}.png',
                bbox_inches='tight',
                transparent=True,
                pad_inches=0.0)
    plt.close()


def plot(metrics, threads, phases, distributions, totalExecutionTime,
         outputPath, resultsDirs, names, qos):
    global metricsConfig
    globalMetrics = {
        metric: {
            impl: results[metric]['overall']
            for impl, results in metrics.items()
        }
        for metric in metricsConfig
    }
    maxMetricsBetweenImplementationsPerPool = {}
    for metric, results in globalMetrics.items():
        if (all(['perPool' in r[metric] for _, r in metrics.items()])):
            maxMetricsBetweenImplementationsPerPool[metric] = [
                max([max(r[metric]['perPool'][i]) for _, r in metrics.items()])
                for i in range(threads)
            ]
        plotGlobalMetric(
            results,
            outputPath,
            phases,
            metric,
            metricsConfig[metric]['name'],
            metricsConfig[metric]['label'],
            names,
            yTicksFormatter=metricsConfig[metric]['yTicksFormatter'])
    for implementation, ms in metrics.items():
        outputPath = resultsDirs[implementation]
        for metric, results in ms.items():
            if metric != 'hit-ratio' and 'perPool' in results:
                plotHitRatioWith(
                    names[implementation],
                    ms['hit-ratio']['perPool'],
                    maxMetricsBetweenImplementationsPerPool['hit-ratio'],
                    metric,
                    metricsConfig[metric]['name'],
                    metricsConfig[metric]['label'],
                    results['perPool'],
                    maxMetricsBetweenImplementationsPerPool[metric],
                    threads,
                    outputPath,
                    phases,
                    totalExecutionTime,
                    distributions,
                    otherMetricYTicksFormatter=metricsConfig[metric]
                    ['yTicksFormatter'],
                    qos=qos[implementation])
        #plot throughput per hit ratio
        plotMetricPerOtherMetric(
            names[implementation], ms['throughput']['perPool'], 'throughput',
            metricsConfig['throughput']['name'],
            metricsConfig['throughput']['label'], ms['hit-ratio']['perPool'],
            'hit-ratio', metricsConfig['hit-ratio']['name'],
            metricsConfig['hit-ratio']['label'], threads, outputPath,
            distributions, metricsConfig['throughput']['yTicksFormatter'],
            metricsConfig['hit-ratio']['yTicksFormatter'])


def main(argv, argc):
    global config, latencies
    workspace = os.getcwd()
    profile_dir = argv[1]
    with open(f'{workspace}/{profile_dir}/config.json') as f:
        config = json.load(f)
        phases = config['phases']
        metrics = {}
        qos = {}
        names = {i: config['setups'][i]['name'] for i in config['setups']}
        threads = int(config['ycsb']['threadcount'])
        runs = int(config['runs'])
        totalExecutionTime = phases[-1]
        distributions = []
        for i in range(threads):
            if f'requestdistribution.{i}' in config['ycsb']:
                distributions.append(
                    f'Zipf ({config["ycsb"][f"zipfian_const.{i}"]})'
                    if config['ycsb'][f'requestdistribution.{i}'] ==
                    'zipfian' else 'Uniform')
            elif 'requestdistribution' in config['ycsb']:
                distributions.append(
                    f'Zipf ({config["ycsb"]["zipfian_const"]})'
                    if config['ycsb'][f'requestdistribution'] ==
                    'zipfian' else 'Uniform')
            else:
                exit('No request distribution found')
        outputDirs = {}
        outputRoot = argv[2] if argc > 2 else argv[1]
        for implementation in config['setups']:
            resultsDir = f'{argv[1]}/{config["setups"][implementation]["resultsDir"]}'
            if argc > 2:
                outputDirs[
                    implementation] = f'{outputRoot}/{config["setups"][implementation]["resultsDir"]}'
                os.makedirs(outputDirs[implementation], exist_ok=True
                            )  # create the directories if they don't not exist
            else:
                outputDirs[implementation] = resultsDir

            if qosImpl := config['setups'][implementation].get('qos'):
                qos[implementation] = {
                    int(p): 1 - float(q)
                    for p, q in qosImpl.items()
                }
            else:
                qos[implementation] = None
            try:
                print(resultsDir)
                hr = getHitRatio(resultsDir, runs, threads, totalExecutionTime)
             #   mem = getMemory(resultsDir, runs, threads, totalExecutionTime)
                iops = getThroughput(resultsDir, runs, threads,
                                     totalExecutionTime)
                lats = getLookupLatencies(resultsDir, runs, threads,
                                          totalExecutionTime, phases)
                cpu = getCPUUsage(resultsDir, runs, threads,
                                  totalExecutionTime)
            except FileNotFoundError:
                print(
                    f'Some results not found\n Check if {implementation} is still running'
                )
                continue
            metrics[implementation] = {
                'hit-ratio': hr,
                'throughput': iops,
                'cpu': cpu,
           #     **mem,
                **lats
            }
            #print(f'Real Cache Size: {max(mem["memory"]["overall"])}')
        plot(metrics, threads, phases, distributions, totalExecutionTime,
             outputRoot, outputDirs, names, qos)


if __name__ == "__main__":
    main(sys.argv, len(sys.argv))
