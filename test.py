import datetime
import os
import os.path
import re
import signal
import subprocess
import time

CFG_STD = "config-std.h"
CFG_CURR = "config.h"


def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, 'w')
    f.write(s)
    f.close()


def test_compile(name, job):
    os.system("cp " + CFG_STD + ' ' + CFG_CURR)
    for (param, value) in job.items():
        pattern = r"\#define\s" + re.escape(param) + r'.*'
        replacement = "#define " + param + ' ' + str(value)
        replace(CFG_CURR, pattern, replacement)
    ret = os.system("make -j > temp.out 2>&1")
    if ret != 0:
        print(f"ERROR in compiling job {name}")
    print(f"PASS Compile\t {name}")
    os.system('rm temp.out')


def test_run(name, job, app_flags=""):
    cmd = f"./rundb {app_flags}"
    start = datetime.datetime.now()
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    stdout, stderr = p.communicate()

    if "PASS" not in stdout.decode():
        print(f"FAILED execution. cmd = {cmd}")

    print(f"PASS execution\t {name}")


def main():
    algs = ['DL_DETECT', 'NO_WAIT', 'HEKATON', 'SILO', 'TICTOC']
    indices = ['IDX_BTREE', 'IDX_HASH']
    num_threads_lst = [2 ** n for n in range(1, 8)]
    workloads = ["YCSB", "TPCC"]

    jobs = {
        f"{workload}_{alg}_{index}_{num_threads}": {
            "WORKLOAD": workload,
            "CORE_CNT": num_threads,
            "CC_ALG": alg,
            "INDEX_STRUCT": index
        }
        for workload in workloads
        for alg in algs
        for index in indices
        for num_threads in num_threads_lst
    }

    for name, job in jobs.items():
        test_compile(name, job)
        test_run(name, job, app_flags=f"-o results/{name}.txt")


if __name__ == '__main__':
    main()
