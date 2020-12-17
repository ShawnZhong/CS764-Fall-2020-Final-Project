import itertools
import operator

from pathlib import Path

import matplotlib.pyplot as plt


def parse(field, t=float):
    _, field = field[:-1].split("=")
    return t(field)


def read_result(results_dir):
    for result_path in sorted(results_dir.iterdir()):
        job_name = result_path.name
        result_txt = result_path / "result.txt"

        if not result_txt.exists():
            print(f"{job_name} does not exist")
            continue

        with open(result_txt) as f:
            summary = f.readline()
            (
                _,
                txn_cnt,
                abort_cnt,
                run_time,
                time_wait,
                time_ts_alloc,
                time_man,
                time_index,
                time_abort,
                time_cleanup,
                latency,
                deadlock_cnt,
                cycle_detect,
                dl_detect_time,
                dl_wait_time,
                time_query,
                *_,
            ) = summary.split()

            workload, alg, index_type, num_threads, hotset_perc = job_name.split(",")

            yield workload, alg, index_type, int(num_threads), parse(txn_cnt) / parse(time_index), hotset_perc


def main(results_dir):
    res = sorted(read_result(results_dir))

    grouped_res = {
        key: list(items)
        for key, items in itertools.groupby(res, lambda item: item[:3])
    }

    plt.figure(figsize=(16, 10))

    for i, (key, items) in enumerate(grouped_res.items()):
        workload, alg, index_type = key

        hotset_perc_lst = [e[5] for e in items]
        run_time_lst = [e[4] for e in items]
        print(hotset_perc_lst)
        print(run_time_lst)

        d = {hotset_perc_lst[i]:run_time_lst[i] for i in range(len(hotset_perc_lst))}

        sorted_hotset_perc_lst = []
        sorted_run_time_lst = []

        for key in sorted(d.keys()):
          print("append", key)
          sorted_hotset_perc_lst.append(key)
          sorted_run_time_lst.append(d[key])

        print(sorted_hotset_perc_lst)
        print(sorted_run_time_lst)
        
        label = " ".join(key)

        index = {
            ("IDX_HASH", "TPCC"): 1,
            ("IDX_BTREE", "TPCC"): 2,
            ("IDX_HASH", "YCSB"): 3,
            ("IDX_BTREE", "YCSB"): 4,
        }

        plt.subplot(2, 2, index[(index_type, workload)])

        plt.plot(sorted_hotset_perc_lst, sorted_run_time_lst, label=alg, marker='o')
        # plt.xscale("log", basex=2)
        plt.xlabel("Hotset Percentage")
        plt.ylabel("Throughput (txn/sec)")
        plt.legend()
        plt.title(f"{workload} {index_type}")

    plt.savefig(results_dir / "4_7_hotset_perc_plot.png")


if __name__ == "__main__":
    results_dir = Path("4_7_results")
    main(results_dir)