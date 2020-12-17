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

            workload, alg, index_type, num_threads, fanout = job_name.split(",")

            yield workload, alg, index_type, int(num_threads), parse(txn_cnt) / parse(time_index), fanout


def main(results_dir):
    res = sorted(read_result(results_dir))

    grouped_res = {
        key: list(items)
        for key, items in itertools.groupby(res, lambda item: item[:3])
    }

    plt.figure(figsize=(16, 10))

    for i, (key, items) in enumerate(grouped_res.items()):
        workload, alg, index_type = key

        fanout_lst = [e[5] for e in items]
        print(fanout_lst)
        run_time_lst = [e[4] for e in items]
        label = " ".join(key)

        index = {
            ("IDX_HASH", "TPCC"): 1,
            ("IDX_BTREE", "TPCC"): 2,
            ("IDX_HASH", "YCSB"): 3,
            ("IDX_BTREE", "YCSB"): 4,
        }

        plt.subplot(2, 2, index[(index_type, workload)])

        plt.plot(fanout_lst, run_time_lst, label=alg, marker='o')
        # plt.xscale("log", basex=2)
        plt.xlabel("Number of fanout")
        plt.ylabel("Throughput (txn/sec)")
        plt.legend()
        plt.title(f"{workload} {index_type}")

    plt.savefig(results_dir / "4_2_fanout_plot.png")


if __name__ == "__main__":
    results_dir = Path("4_2_results")
    main(results_dir)