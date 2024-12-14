# %%

import os
import requests
import matplotlib.pyplot as plt

# %%

url = 'https://neurosift.org/scratch/qfc_benchmark/test1.zarr/results.json'
response = requests.get(url)
results = response.json()

# %%

algs = []
compression_methods = []
target_residual_stdevs = []
for r in results:
    alg = r["alg"]
    compression_method = r["compression_method"]
    target_residual_stdev = r["target_residual_stdev"]
    if alg not in algs:
        algs.append(alg)
    if compression_method not in compression_methods:
        compression_methods.append(compression_method)
    if target_residual_stdev not in target_residual_stdevs:
        target_residual_stdevs.append(target_residual_stdev)

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

# Compression ratio vs residual stdev for QFC-zlib, QFC-zstd, QTC-zlib, QTC-zstd
plt.figure(figsize=(6, 4))
for alg in algs:
    for compression in compression_methods:
        # Filter results for this algorithm and compression method
        filtered_results = [
            r
            for r in results
            if r["alg"] == alg and r["compression_method"] == compression
        ]
        # Sort by residual_stdev to ensure proper line plotting
        filtered_results.sort(key=lambda x: x["residual_stdev"])

        if filtered_results:
            label = f"{alg}-{compression}"
            residual_stdevs = [r["residual_stdev"] for r in filtered_results]
            compression_ratios = [r["compression_ratio"] for r in filtered_results]
            plt.plot(residual_stdevs, compression_ratios, label=label, marker="o")

# plt.yscale("log")
plt.xlabel("Residual Stdev")
plt.ylabel("Compression Ratio")
plt.legend()
plt.show()

# %%

# Compression time vs residual stdev for QFC-zlib, QFC-zstd, QTC-zlib, QTC-zstd
plt.figure(figsize=(6, 4))
for alg in algs:
    for compression in compression_methods:
        # Filter results for this algorithm and compression method
        filtered_results = [
            r
            for r in results
            if r["alg"] == alg
            and r["compression_method"] == compression
            and r["residual_stdev"] > 0
        ]
        # Sort by residual_stdev to ensure proper line plotting
        filtered_results.sort(key=lambda x: x["residual_stdev"])

        if filtered_results:
            label = f"{alg}-{compression}"
            residual_stdevs = [r["residual_stdev"] for r in filtered_results]
            compression_times = [r["compression_time_sec"] for r in filtered_results]
            plt.plot(residual_stdevs, compression_times, label=label, marker="o")

plt.xlabel("Residual Stdev")
plt.ylabel("Compression Time (s)")
plt.legend()
plt.show()
# %%

# Compression ratio vs compression time for QFC-zlib, QFC-zstd, QTC-zlib, QTC-zstd
plt.figure(figsize=(6, 4))
for alg in algs:
    for compression in compression_methods:
        # Filter results for this algorithm and compression method
        filtered_results = [
            r
            for r in results
            if r["alg"] == alg and r["compression_method"] == compression and r["residual_stdev"] > 0
        ]
        # Sort by compression_ratio to ensure proper line plotting
        filtered_results.sort(key=lambda x: x["compression_ratio"])

        if filtered_results:
            label = f"{alg}-{compression}"
            compression_ratios = [r["compression_ratio"] for r in filtered_results]
            compression_times = [r["compression_time_sec"] for r in filtered_results]
            plt.plot(compression_ratios, compression_times, label=label, marker="o")

plt.xlabel("Compression ratio")
plt.ylabel("Compression Time (s)")
plt.legend()
plt.show()
# %%
