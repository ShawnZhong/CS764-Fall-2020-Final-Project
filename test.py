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

def run_exp(exp_name, jobs):
    for name, job in jobs.items():
        result_dir = results_dir / exp_name / name
        os.makedirs(result_dir, exist_ok=True)

        test_compile(name, job, result_dir)
        test_run(name, job, result_dir)

scalibility_exp = {
    f"{workload},{alg},{index},{num_threads}": {
        "WORKLOAD": workload,
        "CORE_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
    }
    for workload in ["YCSB"]
    for alg in ["DL_DETECT", "NO_WAIT", "HEKATON", "SILO", "TICTOC"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in list(range(5, 35, 5))
}

fanout_exp = {
    f"{workload},{alg},{index},{num_threads},{fanout}": {
        "WORKLOAD": workload,
        "CORE_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "BTREE_ORDER": fanout,
    }
    for workload in ["TPCC"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE"]
    for num_threads in [32]
    for fanout in [4, 8, 16, 32, 64, 128, 256]
}

contention_exp = {
    f"{workload},{alg},{index},{num_threads},{num_wh}": {
        "WORKLOAD": workload,
        "CORE_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "NUM_WH": num_wh,
    }
    for workload in ["TPCC"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [32]
    for num_wh in [i for i in range(1,21)]
}

rw_exp = {
    f"{workload},{alg},{index},{num_threads},{rw_ratio}": {
        "WORKLOAD": workload,
        "CORE_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "PERC_PAYMENT": rw_ratio,
    }
    for workload in ["TPCC"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [32]
    for rw_ratio in [i/10 for i in range(11)]
}

hotset_exp = {
    f"{workload},{alg},{index},{num_threads},{zipf_theta}": {
        "WORKLOAD": workload,
        "CORE_CNT": num_threads,
        "CC_ALG": alg,
        "INDEX_STRUCT": index,
        "ZIPF_THETA": zipf_theta,
    }
    for workload in ["YCSB"]
    for alg in ["NO_WAIT"]
    for index in ["IDX_BTREE", "IDX_HASH"]
    for num_threads in [32]
    for zipf_theta in [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 0.99]
}


def main():
    # run_exp("scalibility", scalibility_exp)
    # run_exp("fanout", fanout_exp)
    # run_exp("contention", contention_exp)
    # run_exp("rw", rw_exp)
    run_exp("hotset", hotset_exp)


if __name__ == "__main__":
    main()
