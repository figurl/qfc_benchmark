# %%

import os
import requests
import matplotlib.pyplot as plt

# %%

url = "https://neurosift.org/scratch/qfc_benchmark/test1.zarr/results.json"
# url = "https://neurosift.org/scratch/qfc_benchmark/test2.zarr/results.json"
response = requests.get(url)
results = response.json()

# %%

filt_sets = []
algs = []
compression_methods = []
target_residual_stdevs = []
for r in results:
    lowcut = r["lowcut"]
    highcut = r.get("highcut", None)
    k = f"{lowcut}-{highcut}" if highcut else f"{lowcut}"
    alg = r["alg"]
    compression_method = r["compression_method"]
    target_residual_stdev = r["target_residual_stdev"]
    if alg not in algs:
        algs.append(alg)
    if compression_method not in compression_methods:
        compression_methods.append(compression_method)
    if target_residual_stdev not in target_residual_stdevs:
        target_residual_stdevs.append(target_residual_stdev)
    if k not in filt_sets:
        filt_sets.append(k)

data_directory = "output1"
output_directory = "plots"

# Clear plots directory
if not os.path.exists(output_directory):
    os.makedirs(output_directory)
else:
    for file in os.listdir(output_directory):
        file_path = os.path.join(output_directory, file)
        if os.path.isfile(file_path):
            os.unlink(file_path)

# %%

def label_for_filter(filt_set):
    p = filt_set.split("-")
    lowcut = int(p[0])
    highcut = int(p[1]) if len(p) > 1 else None
    if highcut is not None:
        return f"Bandpass filter {lowcut}-{highcut} Hz"
    else:
        return f"Highpass filter {lowcut} Hz"

# %%

for r in results:
    if r["alg"] == "qwc":
        print(f'{r["alg"]}-{r["compression_method"]}: {r["residual_stdev"]}')

# %%

# Compression ratio vs residual stdev for QFC-zlib, QFC-zstd, QTC-zlib, QTC-zstd
for filt_set in filt_sets:
    p = filt_set.split("-")
    lowcut = int(p[0])
    highcut = int(p[1]) if len(p) > 1 else None
    plt.figure(figsize=(6, 4))
    for alg in algs:
        for compression in compression_methods:
            # Filter results for this algorithm and compression method
            filtered_results = [
                r
                for r in results
                if r["alg"] == alg
                and r["compression_method"] == compression
                and r["lowcut"] == lowcut
                and r.get("highcut", None) == highcut
            ]
            # Sort by residual_stdev to ensure proper line plotting
            filtered_results.sort(key=lambda x: x["residual_stdev"])

            if filtered_results:
                label = f"{alg}-{compression}"
                residual_stdevs = [r["residual_stdev"] for r in filtered_results]
                compression_ratios = [r["compression_ratio"] for r in filtered_results]
                plt.plot(residual_stdevs, compression_ratios, label=label, marker="o")
    plt.xlabel("Residual Stdev")
    plt.ylabel("Compression Ratio")
    plt.yscale("log")
    plt.legend()
    plt.title(f"{label_for_filter(filt_set)}")
    plt.show()

# %%

# Compression time vs residual stdev for QFC-zlib, QFC-zstd, QTC-zlib, QTC-zstd
plt.figure(figsize=(6, 4))
for filt_set in filt_sets:
    p = filt_set.split("-")
    lowcut = int(p[0])
    highcut = int(p[1]) if len(p) > 1 else None
    for alg in algs:
        for compression in compression_methods:
            # Filter results for this algorithm and compression method
            filtered_results = [
                r
                for r in results
                if r["alg"] == alg
                and r["compression_method"] == compression
                and r["residual_stdev"] > 0
                and r["lowcut"] == lowcut
                and r.get("highcut", None) == highcut
            ]
            # Sort by residual_stdev to ensure proper line plotting
            filtered_results.sort(key=lambda x: x["residual_stdev"])

            if filtered_results:
                label = f"{alg}-{compression}"
                residual_stdevs = [r["residual_stdev"] for r in filtered_results]
                compression_times = [
                    r["compression_time_sec"] for r in filtered_results
                ]
                plt.plot(residual_stdevs, compression_times, label=label, marker="o")

    plt.xlabel("Residual Stdev")
    plt.ylabel("Compression Time (s)")
    plt.legend()
    plt.title(f"{label_for_filter(filt_set)}")
    plt.show()
# %%

# Compression ratio vs compression time for QFC-zlib, QFC-zstd, QTC-zlib, QTC-zstd
plt.figure(figsize=(6, 4))
for filt_set in filt_sets:
    p = filt_set.split("-")
    lowcut = int(p[0])
    highcut = int(p[1]) if len(p) > 1 else None
    for alg in algs:
        for compression in compression_methods:
            # Filter results for this algorithm and compression method
            filtered_results = [
                r
                for r in results
                if r["alg"] == alg
                and r["compression_method"] == compression
                and r["residual_stdev"] > 0
                and r["lowcut"] == lowcut
                and r.get("highcut", None) == highcut
            ]
            # Sort by compression_ratio to ensure proper line plotting
            filtered_results.sort(key=lambda x: x["compression_ratio"])

            if filtered_results:
                label = f"{alg}-{compression}"
                compression_ratios = [r["compression_ratio"] for r in filtered_results]
                compression_times = [
                    r["compression_time_sec"] for r in filtered_results
                ]
                plt.plot(compression_ratios, compression_times, label=label, marker="o")

    plt.xlabel("Compression ratio")
    plt.ylabel("Compression Time (s)")
    plt.legend()
    plt.title(f"{label_for_filter(filt_set)}")
    plt.show()
# %%
