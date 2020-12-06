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

            *group_name, index_type, num_threads = job_name.split("_")

            group_name = "_".join(group_name)
            run_time = parse(run_time)
            time_index = parse(time_index)
            num_threads = int(num_threads)

            yield group_name, index_type, num_threads, run_time


def main(results_dir):
    res = sorted(read_result(results_dir))

    grouped_res = {
        " ".join(key).replace("_", " "): list(items)
        for key, items in itertools.groupby(res, lambda item: item[:2])
    }

    plt.figure(figsize=(25, 20))

    for i, (label, items) in enumerate(grouped_res.items()):
        num_threads_lst = [e[2] for e in items]
        run_time_lst = [e[3] for e in items]

        plt.subplot(4, 5, i + 1)
        plt.plot(num_threads_lst, run_time_lst)  # label=label)
        plt.xscale("log", basex=2)
        plt.title(label)

    plt.savefig(results_dir / "plot.png")


if __name__ == "__main__":
    results_dir = Path("results_high_cont")
    main(results_dir)