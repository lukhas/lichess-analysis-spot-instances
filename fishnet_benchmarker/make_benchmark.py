import subprocess
import json
import datetime
import getpass

for i in range(5):
    try:
        print("Updating packages...")
        p = subprocess.run(["sudo", "apt", "update", "-y"], capture_output = True)
        out, err = p.stdout, p.stderr
        print("Installing pip...")
        p = subprocess.run(["sudo", "apt", "install", "-y", "python3-pip"], capture_output = True)
        out, err = p.stdout, p.stderr
        print("Updating fishnet...")
        p = subprocess.run(["pip3", "install", "--user", "fishnet"], capture_output = True)
        out, err = p.stdout, p.stderr
        break
    except:
        print("FAILED installation! Trying again.")
        print("out:", out)
        print("err:", err)

results = []

fishnet_key = "XXXXXXXXX"

n_cores = int(subprocess.run(["nproc"], capture_output = True).stdout.decode("utf8").strip())
for n_thread_per_process in [n_cores]:
    open("fishnet.ini", "w").write("""
[Fishnet]
enginedir = /home/ubuntu
cores = {n_cores}
threads-per-process = {n_thread_per_process}
userbacklog = 0s
systembacklog = 0s
endpoint = https://lichess.org/fishnet/
key = {key}

[Stockfish]
    """.strip().format(n_cores = n_cores, n_thread_per_process = n_thread_per_process, key = fishnet_key))

    print("Starting benchmark for {} threads per instance at {}. Should take 17 minutes.".format(n_thread_per_process, datetime.datetime.now().strftime("%H:%M:%S")))
    stdout = subprocess.run(["timeout", "1000", "python3", "-m", "fishnet"], capture_output = True).stdout.decode("utf8")
    if "ConfigError" in stdout:
        break

    result = {
        "n_cores": n_cores,
        "n_thread_per_process": n_thread_per_process,
        "bench_length": 1000,
        "n_nodes": stdout.split("crunched ")[-1].split(" nodes")[0]
    }
    print(result)
    results.append(result)

open("results.json", "w").write(json.dumps(results))