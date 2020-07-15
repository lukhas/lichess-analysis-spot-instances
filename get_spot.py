from glob import glob
import subprocess
import sys
import os
import shutil
import time
import random
import string
import requests
import json
from pprint import pprint
import itertools
import datetime
import os.path
from filelock import Timeout, FileLock
from matplotlib import pyplot as plt

REGION_BLACKLIST = set()
print("Blacklisted regions (too many preemptions): {}".format(REGION_BLACKLIST))

API_KEY = os.environ.get("GCLOUD_API_KEY")
if not API_KEY:
    print("Please set the GCLOUD_API_KEY environment variable.")
    sys.exit()

GCLOUD_CMD = shutil.which("gcloud")

# Creates a spot instance with the given name, instance type and region
def make_spot_instance(vm_name, instance_type, region):
    p = subprocess.run([
        GCLOUD_CMD, "compute", "instances", "create",
        "--machine-type", instance_type,
        "--zone", region,
        vm_name, "--image-project", "ubuntu-os-cloud", "--image-family", "ubuntu-2004-lts",
        "--subnet", "defaulteuwb1", "--preemptible"
    ], capture_output = True, encoding = "utf8")
    stdout, stderr = p.stdout, p.stderr
    out = stdout + stderr
    if "Created" in out:
        return True, out

    return False, out

# scp a file into a vm
def put_file(vm_name, region, local_fname, remote_fname):
    p = subprocess.run([
        GCLOUD_CMD, "compute", "scp", "--force-key-file-overwrite",
        local_fname, "ubuntu@" + vm_name + ":" + remote_fname, "--zone", region,
    ], capture_output = True, encoding = "utf8")
    stdout, stderr = p.stdout, p.stderr
    out = stdout + stderr
    if "ERROR" in out:
        return False, out
    
    return True, out

# scp a file from a vm
def get_file(vm_name, region, remote_fname, local_fname):
    p = subprocess.run([
        GCLOUD_CMD, "compute", "scp", "--force-key-file-overwrite",
        "ubuntu@" + vm_name + ":" + remote_fname, local_fname, "--zone", region,
    ], capture_output = True, encoding = "utf8")
    stdout, stderr = p.stdout, p.stderr
    out = stdout + stderr
    if "ERROR" in out:
        return False, out
    
    return True, out


# deletes a vm
def delete_spot_instance(vm_name):
    p = subprocess.run([
        GCLOUD_CMD, "-q", "compute", "instances", "delete",
        vm_name, "--zone", region
    ], capture_output = True, encoding = "utf8")

    stdout, stderr = p.stdout, p.stderr
    out = stdout + stderr

    if "Delete" in out:
        return True, out

    return False, out

# executes a command in a vm through ssh
def exec_ssh(vm_name, region, command):
    p = subprocess.run([
        GCLOUD_CMD, "compute", "ssh", "--force-key-file-overwrite",
        "ubuntu@" + vm_name, "--command", command, "--zone", region
    ], capture_output = True, encoding = "utf8")

    stdout, stderr = p.stdout, p.stderr
    out = stdout + stderr

    if "ERROR" in out:
        return False, stdout, stderr

    return True, stdout, stderr

# gets the price for a sku. if there isn't one, 9999 is returned
def get_sku_price(sku):
    try:
        tieredRates = sku["pricingInfo"][0]["pricingExpression"]["tieredRates"]
        if len(tieredRates) == 0:
            return 9999

        unitPrice = tieredRates[0]["unitPrice"]
        usageUnit = sku["pricingInfo"][0]["pricingExpression"]["usageUnit"]
        price = int(unitPrice["units"]) + int(unitPrice["nanos"]) / (1000 ** 3)
        return price
    except Exception as e:
        print("Exception with", sku, str(e))
        sys.exit(1)

# gets the data we're interested in from all the skus
def get_skus():
    services = requests.get("https://cloudbilling.googleapis.com/v1/services?key={}".format(API_KEY)).json()["services"]
    serviceId = [x for x in services if x["displayName"] == "Compute Engine"][0]["serviceId"]

    skus_data = {}
    skus = []
    
    r_skus = requests.get("https://cloudbilling.googleapis.com/v1/services/{}/skus?key={}".format(serviceId, API_KEY)).json()
    skus += r_skus["skus"]
    while "nextPageToken" in r_skus and r_skus["nextPageToken"].strip():
        r_skus = requests.get("https://cloudbilling.googleapis.com/v1/services/{}/skus?pageToken={}&key={}".format(serviceId, r_skus["nextPageToken"], API_KEY)).json()
        skus += r_skus["skus"]

    for sku in sorted(skus, key = lambda sku: get_sku_price(sku)):
        # Only preemptible compute instances
        if not (sku["category"]["usageType"] == "Preemptible" and sku["category"]["resourceFamily"] == "Compute"):
            continue

        # Also can't do anything with GPU
        if "GPU" in sku["description"]:
            continue

        if(len(sku["pricingInfo"][0]["pricingExpression"]["tieredRates"]) > 1):
            print("WARNING! Multiple tieredRates in ", sku["description"])
            for p in ["pricingInfo"][0]["pricingExpression"]["tieredRates"]:
                pprint(p)
                print()
                print()

        if(len(sku["pricingInfo"]) > 1):
            print("WARNING! Multiple pricingInfo in ", sku["description"])
            for p in ["pricingInfo"]:
                pprint(p)
                print()
                print()

        unitPrice = sku["pricingInfo"][0]["pricingExpression"]["tieredRates"][0]["unitPrice"]
        usageUnit = sku["pricingInfo"][0]["pricingExpression"]["usageUnit"]
        price = get_sku_price(sku)

        for region in sku["geoTaxonomy"]["regions"]:
            if region in REGION_BLACKLIST:
                continue
        
            if not region in skus_data:
                skus_data[region] = {}

            skus_data[region][sku["description"].split("running")[0].strip()] = {
                "price": price
            }

    return skus_data

# Returns the machine type we can benchmark and spin up.
# This includes specs, such as number of vCPUs and amount of ram
def get_defined_machine_types(return_all = False):
    skus = get_skus()
    machine_types = []

    for region_name, region in skus.items():
        ram_name  = "Preemptible Custom Instance Ram"
        core_name = "Preemptible Custom Instance Core"
        if ram_name not in skus[region_name].keys():
            continue

        if core_name not in skus[region_name].keys():
            continue

        for n_vcpu in [8, 16]:
            for n_quarter_ram_units in [4 * n_vcpu]:
                machine_types.append({
                    "instance-type": "n1-custom-{}-{}".format(n_vcpu, n_quarter_ram_units * 256),
                    "price": skus[region_name][core_name]["price"] * n_vcpu + skus[region_name][ram_name]["price"] * n_quarter_ram_units / 4,
                    "region": region_name
                })

    for region_name, region in skus.items():
        ram_name  = "Preemptible N2 Custom Instance Ram"
        core_name = "Preemptible N2 Custom Instance Core"
        if ram_name not in skus[region_name].keys():
            continue

        if core_name not in skus[region_name].keys():
            continue

        for n_vcpu in [8, 16]:
            for n_quarter_ram_units in [2 * n_vcpu]:
                machine_types.append({
                    "instance-type": "n2-custom-{}-{}".format(n_vcpu, n_quarter_ram_units * 256),
                    "price": skus[region_name][core_name]["price"] * n_vcpu + skus[region_name][ram_name]["price"] * n_quarter_ram_units / 4,
                    "region": region_name
                })

    for region_name, region in skus.items():
        ram_name  = "Preemptible E2 Instance Ram"
        core_name = "Preemptible E2 Instance Core"
        if ram_name not in skus[region_name].keys():
            continue

        if core_name not in skus[region_name].keys():
            continue

        for n_vcpu in [8, 16]:
            for n_quarter_ram_units in [2 * n_vcpu]:
                machine_types.append({
                    "instance-type": "e2-custom-{}-{}".format(n_vcpu, n_quarter_ram_units * 256),
                    "price": skus[region_name][core_name]["price"] * n_vcpu + skus[region_name][ram_name]["price"] * n_quarter_ram_units / 4,
                    "region": region_name
                })

    for region_name, region in skus.items():
        ram_name  = "Preemptible N2D AMD Custom Instance Ram"
        core_name = "Preemptible N2D AMD Custom Instance Core"
        if ram_name not in skus[region_name].keys():
            continue

        if core_name not in skus[region_name].keys():
            continue

        for n_vcpu in [8, 16]:
            for n_quarter_ram_units in [2 * n_vcpu]:
                machine_types.append({
                    "instance-type": "n2d-custom-{}-{}".format(n_vcpu, n_quarter_ram_units * 256),
                    "price": skus[region_name][core_name]["price"] * n_vcpu + skus[region_name][ram_name]["price"] * n_quarter_ram_units / 4,
                    "region": region_name
                })

    for region_name, region in skus.items():
        ram_name  = "Preemptible Compute optimized Ram"
        core_name = "Preemptible Compute optimized Core"
        if ram_name not in skus[region_name].keys():
            continue

        if core_name not in skus[region_name].keys():
            continue

        idx = 0
        for n_vcpu in [8, 16]:
            for n_quarter_ram_units in [16 * n_vcpu]:
                machine_types.append({
                    "instance-type": ["c2-standard-8", "c2-standard-16"][idx],
                    "price": skus[region_name][core_name]["price"] * n_vcpu + skus[region_name][ram_name]["price"] * n_quarter_ram_units / 4,
                    "region": region_name
                })
                idx += 1

    for region_name, region in skus.items():
        ram_name  = "Preemptible N1 Predefined Instance Core"
        core_name = "Preemptible N1 Predefined Instance Ram"
        if ram_name not in skus[region_name].keys():
            continue

        if core_name not in skus[region_name].keys():
            continue

        idx = 0
        for n_vcpu in [4, 8, 16]:
            for n_quarter_ram_units in [4 * n_vcpu]:
                machine_types.append({
                    "instance-type": ["n1-standard-4", "n1-standard-8", "n1-standard-16"][idx],
                    "price": skus[region_name][core_name]["price"] * n_vcpu + skus[region_name][ram_name]["price"] * n_quarter_ram_units / 4,
                    "region": region_name
                })
                idx += 1


    if not return_all:
        machine_types_grouped = itertools.groupby(sorted(machine_types, key = lambda t: t["instance-type"]), key = lambda t: t["instance-type"])
        return [min(list(x), key = lambda t: t["price"] if t["price"] > 0.000001 else 999999) for n, x in machine_types_grouped]

    return machine_types


if __name__ == "__main__":
    reply = input("""
What would you like to do?
{simulate, get_data, show_price_variation, list_running_workers, show_price_per_mnps, get_defined_machine_types, get_skus, bench}
    """.strip() + " > ").strip().lower()
    if reply == "simulate":
        pricing_data_files = sorted(glob("pricing_data/*.json"))
        pricing_data_files = [{
            "t": int(x.replace("\\", "/").split(".")[0].split("/")[1]),
            "data": json.load(open(x))
        } for x in pricing_data_files]
        start_t = pricing_data_files[0]["t"]
        end_t   = pricing_data_files[-1]["t"]

        lichess_costs = []
        dynascript_costs = []

        cur_t = start_t
        while cur_t <= end_t:
            print(min(pricing_data_files, key = lambda pdf: abs(pdf["t"] - cur_t))["t"])
            machine_types = min(pricing_data_files, key = lambda pdf: abs(pdf["t"] - cur_t))["data"]
            dollars_per_mnps = []

            for f in glob("fishnet_benchmarker/data/gcp/*.json"):
                json_data = json.loads(open(f).read())[0]
                n_nodes = int(json_data["n_nodes"].replace(" million", ""))
                seconds = json_data["bench_length"]

                instance_name = f.replace("\\", "/").split("/")[-1].split(".")[0]

                try:
                    matching = [x for x in machine_types if x["instance-type"] == instance_name]
                    for match in matching:
                        price = match["price"]
                        region = match["region"]
                        dollars_per_mnps.append({
                            "name": instance_name + "-" + region,
                            "cost_per_mnps": 1 / (n_nodes / seconds / price),
                            "price": price,
                            "mnps": n_nodes / seconds,
                        })
                except Exception as e:
                    continue

            dollars_per_mnps = sorted(dollars_per_mnps, key = lambda x: x["cost_per_mnps"])

            lichess_machine = [x for x in dollars_per_mnps if x["name"] == "n1-custom-8-8192-us-central1"][0]
            lichess_instance_mpns = lichess_machine["mnps"]

            print("one lichess instance (n1-custom-8-8192-us-central1) has {:.2f} mnps and costs ${:.4f}".format(lichess_instance_mpns, lichess_machine["price"]))
            print("the cheapest instance ({}) has {:.2f} mnps and costs ${:.4f}".format(dollars_per_mnps[0]["name"], dollars_per_mnps[0]["mnps"], dollars_per_mnps[0]["price"]))    

            n_lichess_instances = 1
            day_t = cur_t % (24 * 60 * 60)
            day_hour = day_t // (60 * 60)
            if day_hour <= 2 or day_hour >= 18:
                n_lichess_instances = 8

            print("t = {}; day_t = {}; day_hour = {}; n_lichess_instancesinstances = {}".format(cur_t, day_t, day_hour, n_lichess_instances))

            n_dynascript_instances = round(lichess_instance_mpns / dollars_per_mnps[0]["mnps"] * n_lichess_instances)
            print("we need {} cheapest instances to replace {} lichess instances".format(n_dynascript_instances, n_lichess_instances))

            lichess_costs.append((cur_t, lichess_machine["price"] * n_lichess_instances / 60))
            dynascript_costs.append((cur_t, dollars_per_mnps[0]["price"] * n_dynascript_instances / 60))
            cur_t += 60
        
        if True:
            plt.plot([x[0] / 3600 for x in lichess_costs], [x[1] for x in lichess_costs])
            plt.plot([x[0] / 3600 for x in dynascript_costs], [x[1] for x in dynascript_costs])
        else:
            plt.plot([x[0] / 3600 for x in lichess_costs], [sum([a[1] for a in lichess_costs[:idx]]) for idx, x in enumerate(lichess_costs)])
            plt.plot([x[0] / 3600 for x in dynascript_costs], [sum([a[1] for a in dynascript_costs[:idx]]) for idx, x in enumerate(dynascript_costs)])
        plt.legend(["lichess costs", "dynascript costs"])
        plt.show()
    elif reply == "get_data":
        while True:
            t = datetime.datetime.now().strftime("%H:%M")
            try:
                machine_types = get_defined_machine_types(return_all = True)
                open("pricing_data/" + str(int(time.time())) + ".json", "w").write(json.dumps(machine_types))
                print("[{}]: saved pricing data".format(t))
            except Exception as e:
                print("[{}]: exception ".format(t), str(e))

            time.sleep(60)
    elif reply == "bench":
        machine_types = get_defined_machine_types()
        vm_name = "fishnetbench-" + "".join([random.choice(string.ascii_lowercase) for x in range(20)])
        for machine_type in machine_types:
            if os.path.isfile("fishnet_benchmarker/data/gcp/" + machine_type["instance-type"] + ".json"):
                print(machine_type["instance-type"], "has already been benchmarked. Skipping it.")
                continue
            elif os.path.isfile("fishnet_benchmarker/data/gcp/" + machine_type["instance-type"] + ".lock"):
                print(machine_type["instance-type"], "is being benchmarked (or lock leaked). Skipping it.")
                continue
            else:
                print(machine_type["instance-type"], "hasn't been benchmarked yet. Doing it now and locking it.")

            lock = FileLock("fishnet_benchmarker/data/gcp/" + machine_type["instance-type"] + ".lock")
            with lock:
                print("Making spot instance `{}`...".format(vm_name))

                for letter in "abcdef":
                    region = machine_type["region"] + "-" + letter

                    success, out = make_spot_instance(vm_name, machine_type["instance-type"], region)
                    if not success:
                        if "does not exist in zone" not in out:
                            print("Failed! output:")
                            print(out)
                            sys.exit(1)
                        else:
                            continue
                    else:
                        break

                if not success:
                    "Unsuccessful in starting server."

                print("SCPing benchmark script on server")
                attempts = 0
                while True:
                    if attempts > 20:
                        print("Something is wrong. Bailing out!")
                        print(out)
                        sys.exit(1)

                    attempts += 1
                    success, out = put_file(vm_name, region, "fishnet_benchmarker/make_benchmark.py", "/home/ubuntu/make_benchmark.py")
                    if not success:
                        if "External IP" in out:
                            print("We have been interrupted!")
                            break
                    else:
                        break

                if "External IP" in out:
                    print("We have been interrupted!")
                    continue

                print("[{}]: running benchmark. Should take <15 minutes.".format(datetime.datetime.now().strftime("%H:%M")))
                success, stdout, stderr = exec_ssh(vm_name, region, "python3 make_benchmark.py")
                if not success:
                    if "External IP" in stdout + stderr or "unexpectedly closed" in stdout + stderr:
                        print("We have been interrupted!")
                        continue

                    print("FAILED!")
                    print(stdout)
                    print(stderr)

                print("Done. Getting result.json...")
                success, out = get_file(vm_name, region, "/home/ubuntu/results.json", "fishnet_benchmarker/data/gcp/" + machine_type["instance-type"] + ".json")
                if not success:
                    print("FAILED!")
                    print(out)

                print("Deleting spot instance {}".format(vm_name))
                success, out = delete_spot_instance(vm_name)
                if not success:
                    print("Failed! output:")
                    print(out)
                    sys.exit(1)
    elif reply == "get_skus":
        pprint(get_skus())
    elif reply == "get_defined_machine_types":
        for machine_type in get_defined_machine_types():
            print(json.dumps(machine_type, indent = 4))
    elif reply == "list_running_workers":
        print("Unimplemented")
    elif reply == "show_price_per_mnps":
        machine_types = get_defined_machine_types()
        dollars_per_mnps = []

        for f in glob("fishnet_benchmarker/data/gcp/*.json"):
            json_data = json.loads(open(f).read())[0]
            n_nodes = int(json_data["n_nodes"].replace(" million", ""))
            seconds = json_data["bench_length"]
            if seconds < 1000 and "n1" not in f:
                print("Skipping short test")
                continue

            instance_name = f.replace("\\", "/").split("/")[-1].split(".")[0]

            try:
                price = [x for x in machine_types if x["instance-type"] == instance_name][0]["price"]
                region = [x for x in machine_types if x["instance-type"] == instance_name][0]["region"]
            except:
                print("not available")
                continue

            print(instance_name, region, 1 / (n_nodes / seconds / price))
            dollars_per_mnps.append((instance_name.replace("-custom", "") + "-" + region, 1 / (n_nodes / seconds / price)))

        dollars_per_mnps = sorted(dollars_per_mnps, key = lambda x: x[1])
        plt.bar([x[0] for x in dollars_per_mnps], [x[1] for x in dollars_per_mnps])
        plt.show()
    elif reply == "show_price_variation":
        machine_types = get_defined_machine_types(return_all = True)
        chart = []

        machine_types_grouped = itertools.groupby(sorted(machine_types, key = lambda t: t["instance-type"]), key = lambda t: t["instance-type"])
        for machine_type, machines in machine_types_grouped:
            machines = sorted(machines, key = lambda x: x["price"])
            for machine in machines:
                print(machine_type, "in", machine["region"], "costs", "{:.5f}$".format(machine["price"]))

            name = (machine_type.replace("-custom", "").replace("-standard", ""))

            chart.append((name, machines[0]["price"] / machines[-1]["price"]))

        plt.bar([x[0] for x in chart], [x[1] for x in chart])
        plt.show()
    else:
        print("Unrecognized request `{}`. Quitting.".format(reply))