#!/usr/bin/env python3

import os
import os.path
import re
import subprocess as sp

from pathlib import Path

CFG_STD = "config-std.h"
CFG_CURR = "config.h"
RESULTS_DIR = Path("results")


def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, "w")
    f.write(s)
    f.close()


def execute(cmd, out_path, err_path):
    p = sp.Popen(cmd, stdout=sp.PIPE, stderr=sp.PIPE, shell=True)
    stdout, stderr = p.communicate()
    out_str, err_str = stdout.decode(), stderr.decode()

    with open(out_path, "w") as fout, open(err_path, "w") as ferr:
        print(out_str, file=fout)
        print(err_str, file=ferr)

    return p.returncode, out_str, err_str


def test_compile(name, job, result_dir):
    os.system("cp " + CFG_STD + " " + CFG_CURR)
    for param, value in job.items():
        pattern = r"\#define\s" + re.escape(param) + r".*"
        replacement = "#define " + param + " " + str(value)
        replace(CFG_CURR, pattern, replacement)

    ret, _, _ = execute(
        "make -j",
        out_path=result_dir / "compile.out",
        err_path=result_dir / "compile.err",
    )

    if ret != 0:
        print(f"ERROR in compiling job {name}")
    else:
        print(f"PASS compile\t {name}")


def test_run(name, job, result_dir):
    cmd = f"./rundb -o {result_dir / 'result.txt'}"
    _, stdout, _ = execute(
        cmd, out_path=result_dir / "run.out", err_path=result_dir / "run.err"
    )

    if "PASS" in stdout:
        print(f"PASS execution\t {name}")
    else:
        print(f"FAILED execution. cmd = {cmd}")


def main():
    algs = ["DL_DETECT", "NO_WAIT", "HEKATON", "SILO", "TICTOC"]
    indices = ["IDX_BTREE", "IDX_HASH"]
    num_threads_lst = [2 ** n for n in range(1, 8)]
    # workloads = ["YCSB", "TPCC"]

    # num_threads_lst = range(1, 1000, 50)
    # indices = ["IDX_HASH"]
    # algs = ["NO_WAIT"]
    workloads = ["YCSB"]

    jobs = {
        f"{workload},{alg},{index},{num_threads}": {
            "WORKLOAD": workload,
            "CORE_CNT": num_threads,
            "CC_ALG": alg,
            "INDEX_STRUCT": index,
        }
        for workload in workloads
        for alg in algs
        for index in indices
        for num_threads in num_threads_lst
    }

    for name, job in jobs.items():
        result_dir = RESULTS_DIR / name
        os.makedirs(result_dir, exist_ok=True)

        test_compile(name, job, result_dir)
        test_run(name, job, result_dir)


if __name__ == "__main__":
    main()
