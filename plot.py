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

            workload, alg, index_type, num_threads = job_name.split(",")

            yield workload, alg, index_type, int(num_threads), parse(txn_cnt) / parse(time_index)


def main(results_dir):
    res = sorted(read_result(results_dir))

    grouped_res = {
        key: list(items)
        for key, items in itertools.groupby(res, lambda item: item[:3])
    }

    plt.figure(figsize=(8, 10))
    # plt.subplots_adjust(right=0.7)

    for i, (key, items) in enumerate(grouped_res.items()):
        num_threads_lst = [e[3] for e in items]
        run_time_lst = [e[4] for e in items]
        label = " ".join(key)

        if key[2] == "IDX_HASH":
            plt.subplot(2, 1, 1)
        else:
            plt.subplot(2, 1, 2)
        plt.plot(num_threads_lst, run_time_lst, label=key[1])
        plt.xscale("log", basex=2)
        plt.legend()
        # plt.legend(bbox_to_anchor=(1.04,1), loc="upper left")
        # plt.title(label)

    plt.savefig(results_dir / "plot.png")


if __name__ == "__main__":
    results_dir = Path("results")
    main(results_dir)