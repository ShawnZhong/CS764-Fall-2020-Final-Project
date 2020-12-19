#!/usr/bin/env python3

from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt
import numpy as np

RESULTS_DIR = Path("results")


def parse_val(s):
    if s.isdigit():
        return int(s)
    try:
        return float(s)
    except ValueError:
        return s


def parse_kv_str(kv_str):
    metrics = [m.split("=") for m in kv_str.split(",")]
    return {k.strip(): parse_val(v) for k, v in metrics}


def read_results(results_dir):
    for result_path in sorted(results_dir.iterdir()):
        if not result_path.is_dir():
            continue

        job_name = result_path.name
        result_txt = result_path / "result.txt"

        if not result_txt.exists():
            print(f"WARNING: {result_txt} does not exist")
            continue

        with open(result_txt) as f:
            summary = f.readline()
            exp_result = parse_kv_str(summary[10:])
            exp_config = parse_kv_str(job_name)

            yield {**exp_config, **exp_result}


def group_by(res, keys):
    keys = keys or []
    d = defaultdict(list)
    for item in res:
        k = tuple(item[k] for k in keys)
        d[k].append(item)
    return d.items()


def plot(
    title_func,
    data_func,
    results_dir,
    subplot_func=None,
    groupby_keys=None,
    label_func=None,
    xlabel=None,
    ylabel=None,
    figname="plot",
    figsize=(16, 10),
    subplot_size=(1, 1),
    x_log_base=None,
    y_log_base=None,
):
    res = list(read_results(results_dir))

    plt.figure(figsize=figsize)

    for key, items in group_by(res, groupby_keys):
        data = dict(sorted(data_func(items).items()))
        print(key, " ".join(f"{e:.2f}" for e in data.values()), np.var(list(data.values())))

        subplot_id = subplot_func(items) if subplot_func else 1
        plt.subplot(*subplot_size, subplot_id)
        plt.plot(
            data.keys(),
            data.values(),
            label=label_func(items) if label_func else None,
            marker="o",
        )
        if x_log_base:
            plt.xscale("log", basex=x_log_base)
        if y_log_base:
            plt.yscale("log", basey=y_log_base)
        plt.xlabel(xlabel)
        # only plot the y label for the very left subplot
        if (subplot_id - 1) % subplot_size[1] == 0:
            plt.ylabel(ylabel)
        plt.title(title_func(items))

    if label_func:
        # # move to the last subplot in the first row to plot the legend
        # plt.subplot(*subplot_size, subplot_size[1])
        # plt.legend(loc="upper left", bbox_to_anchor=(1.05, 1))
        plt.subplot(*subplot_size, subplot_size[0] * subplot_size[1])
        plt.legend()

    plt.savefig(RESULTS_DIR / f"{figname}.png", bbox_inches='tight')


def compute_y(e):
    return e["time_index"] / e["txn_cnt"] / e["THREAD_CNT"] * 10 ** 6

def subplot_by_index_struct(items):
    return ["IDX_BTREE", "IDX_HASH"].index(items[0]["INDEX_STRUCT"]) + 1


def plot_scalability_1():
    def subplot_func(items):
        item = items[0]
        subplot_map = {
            ("IDX_HASH", "TPCC"): 1,
            ("IDX_BTREE", "TPCC"): 2,
            ("IDX_HASH", "YCSB"): 3,
            ("IDX_BTREE", "YCSB"): 4,
        }
        return subplot_map[(item["INDEX_STRUCT"]), item["WORKLOAD"]]

    plot(
        results_dir=RESULTS_DIR / "scalability",
        figname="scalability-1",
        figsize=(10, 10),
        subplot_size=(2, 2),
        x_log_base=2,
        xlabel="Number of Threads",
        ylabel="Average Index Time per Transaction (ms)",
        groupby_keys=["CC_ALG", "INDEX_STRUCT", "WORKLOAD"],
        label_func=lambda items: items[0]["CC_ALG"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['INDEX_STRUCT']}",
        data_func=lambda items: {e["THREAD_CNT"]: compute_y(e) for e in items},
        subplot_func=subplot_func,
    )


def plot_scalability_2():
    def subplot_func(items):
        item = items[0]
        col_label = ["DL_DETECT", "NO_WAIT", "HEKATON", "SILO", "TICTOC"]
        row_label = ["TPCC", "YCSB"]

        row_idx = row_label.index(item["WORKLOAD"])
        col_idx = col_label.index(item["CC_ALG"])

        return row_idx * len(col_label) + col_idx + 1

    plot(
        results_dir=RESULTS_DIR / "scalability",
        figname="scalability-2",
        figsize=(20, 8),
        subplot_size=(2, 5),
        x_log_base=2,
        groupby_keys=["CC_ALG", "INDEX_STRUCT", "WORKLOAD"],
        label_func=lambda items: items[0]["INDEX_STRUCT"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['CC_ALG']}",
        data_func=lambda items: {e["THREAD_CNT"]: compute_y(e) for e in items},
        subplot_func=subplot_func,
    )


def plot_rw():
    plot(
        results_dir=RESULTS_DIR / "rw",
        figname="rw",
        figsize=(8, 4),
        subplot_size=(1, 2),
        xlabel="Read Percentage",
        ylabel="Average Index Time per Transaction (ms)",
        groupby_keys=["WORKLOAD", "INDEX_STRUCT"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['INDEX_STRUCT']}",
        data_func=lambda items: {e["READ_PERC"]: compute_y(e) for e in items},
        subplot_func=subplot_by_index_struct,
    )

def plot_fanout():
    plot(
        results_dir=RESULTS_DIR / "fanout",
        figname="fanout",
        figsize=(7, 5),
        x_log_base=2,
        xlabel="B-Tree Order",
        ylabel="Average Index Time per Transaction (ms)",
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['INDEX_STRUCT']}",
        data_func=lambda items: {e["BTREE_ORDER"]: compute_y(e) for e in items},
    )


def plot_hotset():
    plot(
        results_dir=RESULTS_DIR / "hotset",
        figname="hotspot",
        figsize=(10, 5),
        subplot_size=(1, 2),
        xlabel="Hotspot Percentage",
        ylabel="Average Index Time per Transaction (ms)",
        groupby_keys=["WORKLOAD", "INDEX_STRUCT"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['INDEX_STRUCT']}",
        data_func=lambda items: {e["ZIPF_THETA"]: compute_y(e) for e in items},
        subplot_func=subplot_by_index_struct,
    )

def plot_contention():
    plot(
        results_dir=RESULTS_DIR / "contention",
        figname="contention",
        figsize=(10, 5),
        subplot_size=(1, 2),
        xlabel="Number of Warehouse",
        ylabel="Average Index Time per Transaction (ms)",
        groupby_keys=["WORKLOAD", "INDEX_STRUCT"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['INDEX_STRUCT']}",
        data_func=lambda items: {e["NUM_WH"]: compute_y(e) for e in items},
        subplot_func=subplot_by_index_struct,
    )

def main():
    plot_scalability_1()
    plot_scalability_2()
    plot_rw()
    plot_fanout()
    plot_hotset()
    plot_contention()
    pass


if __name__ == "__main__":
    main()
