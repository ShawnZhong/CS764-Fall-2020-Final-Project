import os, sys, re, os.path
import subprocess, datetime, time, signal

CFG_STD = "config-std.h"
CFG_CURR = "config-std.h"


def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, 'w')
    f.write(s)
    f.close()


def test_compile(job):
    os.system("cp " + CFG_STD + ' ' + CFG_CURR)
    for (param, value) in job.iteritems():
        pattern = r"\#define\s" + re.escape(param) + r'.*'
        replacement = "#define " + param + ' ' + str(value)
        replace(CFG_CURR, pattern, replacement)
    ret = os.system("make -j > temp.out 2>&1")
    if ret != 0:
        print(f"ERROR in compiling job={job}")
        exit(0)
    print(f"PASS Compile{job}")
    os.system('rm temp.out')


def test_run(job, app_flags=""):
    cmd = f"./rundb {app_flags}"
    start = datetime.datetime.now()
    process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    timeout = 10  # in second
    while process.poll() is None:
        time.sleep(1)
        now = datetime.datetime.now()
        if (now - start).seconds > timeout:
            os.kill(process.pid, signal.SIGKILL)
            os.waitpid(-1, os.WNOHANG)
            print("ERROR. Timeout cmd=%s" % cmd)
            exit(0)

    if "PASS" not in process.stdout.readlines():
        print("FAILED execution. cmd = {cmd}")
        exit(0)

    print(f"PASS execution. {job}")


def main():
    algs = ['DL_DETECT', 'NO_WAIT', 'HEKATON', 'SILO', 'TICTOC']
    indices = ['INDEX_BTREE', 'INDEX_HASH']
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

    for job in jobs:
        test_compile(job)
        test_run(job)


if __name__ == '__main__':
    main()
