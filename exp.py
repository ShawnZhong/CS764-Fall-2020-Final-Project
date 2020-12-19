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


def test_compile(job_name, job, result_dir):
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
        print(f"ERROR in compiling job {job_name}")
    else:
        print(f"PASS compile\t {job_name}")


def test_run(job_name, job, result_dir):
    _, stdout, _ = execute(
        f"./rundb -o {result_dir / 'result.txt'}",
        out_path=result_dir / "run.out",
        err_path=result_dir / "run.err",
    )

    if "PASS" in stdout:
        print(f"PASS execution\t {job_name}")
    else:
        print(f"FAILED execution. {job_name}")


def get_job_name(job):
    return ",".join(f"{k}={v}" for k, v in job.items())


def run_exp(exp_name, jobs):
    for job in jobs:
        job_name = get_job_name(job)
        result_dir = RESULTS_DIR / exp_name / job_name
        if result_dir.exists():
            print(f"WARNING skip\t {job_name}")
        else:
            os.makedirs(result_dir)

            test_compile(job_name, job, result_dir)
            test_run(job_name, job, result_dir)

scalability_exp = [
    {
        "WORKLOAD": workload,
        "THREAD_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
    }
    for workload in ["YCSB", "TPCC"]
    for alg in ["DL_DETECT", "NO_WAIT", "HEKATON", "SILO", "TICTOC"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    # for num_threads in [2 ** i for i in range(0, 8)]
    for num_threads in [2 ** i for i in range(0, 6)]
]

fanout_exp = [
    {
        "WORKLOAD": workload,
        "THREAD_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "BTREE_ORDER": fanout,
    }
    for workload in ["TPCC"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE"]
    for num_threads in [1]
    for fanout in [2**i for i in range(2, 15)]
]

contention_exp = [
    {
        "WORKLOAD": workload,
        "THREAD_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "NUM_WH": num_wh,
    }
    for workload in ["TPCC"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [1]
    for num_wh in [i for i in range(1, 21)]
]

rw_exp = [
    {
        "WORKLOAD": workload,
        "THREAD_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "READ_PERC": round(read_perc, 1),
        "WRITE_PERC": round(1 - read_perc, 1),
    }
    for workload in ["YCSB"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [1]
    for read_perc in [0.1 * i for i in range(11)]
]

hotset_exp = [
    {
        "WORKLOAD": workload,
        "THREAD_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "ZIPF_THETA": zipf_theta,
    }
    for workload in ["YCSB"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [1]
    for zipf_theta in [i / 10 for i in range(10)]
]

latch_exp = [
    {
        "WORKLOAD": workload,
        "THREAD_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "ENABLE_LATCH": latch,
    }
    for workload in ["YCSB", "TPCC"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [1]
    for latch in ["true", "false"]
]


def main():
    # run_exp("scalability", scalability_exp)
    # run_exp("fanout", fanout_exp)
    # run_exp("contention", contention_exp)
    run_exp("rw", rw_exp)
    # run_exp("hotset", hotset_exp)
    # run_exp("latch", latch_exp)


if __name__ == "__main__":
    main()
