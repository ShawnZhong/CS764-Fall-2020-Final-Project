from pathlib import Path
from collections import defaultdict

import matplotlib.pyplot as plt

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
    d = defaultdict(list)
    for item in res:
        k = tuple(item[k] for k in keys)
        d[k].append(item)
    return d.items()


def plot(
    results_dir,
    groupby_keys,
    label_func,
    title_func,
    data_func,
    subplot_index_func,
    xlabel=None,
    ylabel=None,
    figname="plot",
    figsize=(16, 10),
    subplot_size=(2, 2),
    x_log_scale=False,
):
    res = list(read_results(results_dir))

    plt.figure(figsize=figsize)

    for key, items in group_by(res, groupby_keys):
        data = dict(sorted(data_func(items).items()))
        print(key, " ".join(f"{e:.1f}" for e in data.values()))

        plt.subplot(*subplot_size, subplot_index_func(items))
        plt.plot(data.keys(), data.values(), label=label_func(items), marker="o")
        if x_log_scale:
            plt.xscale("log", basex=2)
        plt.xlabel(xlabel)
        plt.ylabel(ylabel)
        plt.title(title_func(items))

    # move to the last subplot in the first row to plot the legend
    plt.subplot(*subplot_size, subplot_size[1])
    plt.legend(loc="upper left", bbox_to_anchor=(1.05, 1))
    plt.savefig(results_dir / f"{figname}.png")


def plot_scalability_1():
    def subplot_index_func(items):
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
        figsize=(16, 4),
        subplot_size=(1, 4),
        xlabel="Number of Threads",
        ylabel="Average Time per Transaction (ms)",
        groupby_keys=["CC_ALG", "INDEX_STRUCT", "WORKLOAD"],
        label_func=lambda items: items[0]["CC_ALG"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['INDEX_STRUCT']}",
        data_func=lambda items: {
            e["CORE_CNT"]: e["time_index"] / e["txn_cnt"] * 10 ** 6 for e in items
        },
        subplot_index_func=subplot_index_func,
    )


def plot_scalability_2():
    def subplot_index_func(items):
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
        groupby_keys=["CC_ALG", "INDEX_STRUCT", "WORKLOAD"],
        label_func=lambda items: items[0]["INDEX_STRUCT"],
        title_func=lambda items: f"{items[0]['WORKLOAD']} {items[0]['CC_ALG']}",
        data_func=lambda items: {
            e["CORE_CNT"]: e["time_index"] / e["txn_cnt"] * 10 ** 6 for e in items
        },
        subplot_index_func=subplot_index_func,
    )


def main():
    plot_scalability_1()
    plot_scalability_2()
    pass


if __name__ == "__main__":
    main()
