from typing import List, Tuple
import time
import json
import numpy as np
import zarr
import os
import shutil
from scipy.signal import butter, lfilter
from qfc.codecs import QFCCodec, QTCCodec
from qfc import qfc_estimate_quant_scale_factor, qtc_estimate_quant_scale_factor
import spikeinterface.extractors as se
from typing import Any, cast
from numpy.typing import NDArray

"""
This script benchmarks the compression of electrophysiology data from NWB files. It includes:
- **Preprocessing**: Loads NWB data using `spikeinterface`, slices it by duration and channels.
- **Filtering**: Applies a bandpass filter to isolate desired frequency ranges.
- **Compression**: Compresses filtered data using QFC/QTC algorithms with zstd/zlib methods,
  testing various target residual standard deviations and compression levels.
- **Storage**: Uses Zarr for efficient storage and retrieval, supporting local and S3-based storage.
- **Metrics**: Evaluates compression time, ratio, and residual accuracy, saving results to JSON.

The script demonstrates a full pipeline, from raw data extraction to filtered compression,
and outputs benchmark results for further analysis.
"""

QFCCodec.register_codec()
QTCCodec.register_codec()


# Signal Processing
def apply_bandpass_filter(
    array: NDArray[Any], sampling_frequency: float, lowcut: float, highcut: float
) -> NDArray[Any]:
    """
    Apply a bandpass filter to a signal array.

    Args:
        array: Input signal array
        sampling_frequency: Sampling frequency in Hz
        lowcut: Lower cutoff frequency in Hz
        highcut: Upper cutoff frequency in Hz

    Returns:
        Filtered signal array
    """
    nyquist = 0.5 * sampling_frequency
    low = lowcut / nyquist
    high = highcut / nyquist
    b, a = butter(5, [low, high], btype="band")
    result = lfilter(b, a, array, axis=0)
    # lfilter can return either ndarray or tuple of ndarrays
    # In our case, we know it returns ndarray since we're filtering
    return cast(NDArray[Any], result).astype(array.dtype)


def load_nwb_recording(
    nwb_url: str,
    electrical_series_path: str,
    duration_sec: float,
    channel_ids: List[str],
) -> Any:
    """
    Load recording from an NWB file.

    Args:
        nwb_url: URL to the NWB file
        electrical_series_path: Path to electrical series in NWB file
        duration_sec: Duration to load in seconds
        channel_ids: List of channel IDs to include

    Returns:
        Recording object sliced to specified duration
    """
    recording_full = se.NwbRecordingExtractor(
        nwb_url,
        electrical_series_path=electrical_series_path,
        stream_mode="remfile",
    )
    R = recording_full.frame_slice(
        start_frame=0,
        end_frame=int(recording_full.get_sampling_frequency() * duration_sec),
    )
    return R.channel_slice(channel_ids=channel_ids)


def initiate_benchmark(
    *,
    zarr_path: str,
    nwb_url: str,
    electrical_series_path: str,
    duration_sec: float,
    channel_ids: List[str],
):
    """
    Initialize benchmark by loading traces from NWB file and saving to zarr archive.

    Args:
        zarr_path: Path to zarr archive
        nwb_url: URL to the NWB file
        electrical_series_path: Path to electrical series in NWB file
        duration_sec: Duration to load in seconds
        channel_ids: List of channel IDs to include
    """
    z = open_zarr(zarr_path, mode="a")  # create if doesn't exist
    if "raw" in z:
        print("Raw data already exists")
        return
    print("Loading traces from NWB file...")
    recording = load_nwb_recording(
        nwb_url=nwb_url,
        electrical_series_path=electrical_series_path,
        duration_sec=duration_sec,
        channel_ids=channel_ids,
    )
    X: np.ndarray = recording.get_traces()
    print("Saving traces to zarr")
    # create a zarr archive
    assert isinstance(z, zarr.Group)
    z.attrs["nwb_url"] = nwb_url
    z.attrs["electrical_series_path"] = electrical_series_path
    z.attrs["duration_sec"] = duration_sec
    z.attrs["channel_ids"] = channel_ids
    z.attrs["sampling_frequency"] = float(recording.get_sampling_frequency())
    print(f'Creating dataset "raw" with shape {X.shape}...')
    z.create_dataset(
        "raw", data=X, compressor=None, chunks=get_chunks_for_shape(X.shape)
    )


def open_zarr(uri: str, *, mode: str):
    """
    Open a zarr archive, supporting both local and remote (S3) storage.

    Args:
        uri: URI of zarr archive (local path or r2:// S3 path)
        mode: Mode to open zarr archive ('r' for read, 'w' for write, 'a' for append)

    Returns:
        Opened zarr archive
    """
    if uri.startswith("r2://"):
        import s3fs

        fs = s3fs.S3FileSystem(
            key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            config_kwargs={"max_pool_connections": 30},
        )
        pp = "/".join(uri.split("/")[2:])
        store = fs.get_mapper(pp)
        return zarr.open(store=store, mode=mode)
    else:
        return zarr.open(uri, mode=mode)


def get_filtered_path(lowcut: float, highcut: float) -> str:
    """
    Generate path for filtered data based on filter parameters.

    Args:
        lowcut: Lower cutoff frequency in Hz
        highcut: Upper cutoff frequency in Hz

    Returns:
        Path string for filtered data
    """
    return f"filtered_{lowcut}-{highcut}"


def do_filter(*, zarr_path: str, lowcut: float = 300, highcut: float = 6000):
    """
    Apply bandpass filter to raw data and save filtered result.

    Args:
        zarr_path: Path to zarr archive
        lowcut: Lower cutoff frequency in Hz
        highcut: Upper cutoff frequency in Hz
    """
    filtered_path = get_filtered_path(lowcut=lowcut, highcut=highcut)
    z = open_zarr(zarr_path, mode="r+")
    assert isinstance(z, zarr.Group)
    if filtered_path in z:
        print(f"Filtered data already exists: {filtered_path}")
        return
    print("Applying bandpass filter to data...")
    sampling_frequency: float = z.attrs["sampling_frequency"]
    raw_array = z["raw"]
    assert isinstance(raw_array, zarr.Array)
    X = raw_array[:]
    X_filtered = apply_bandpass_filter(
        X, sampling_frequency=sampling_frequency, lowcut=lowcut, highcut=highcut
    )
    print(f'Creating dataset "{filtered_path}" with shape {X_filtered.shape}...')
    z.create_dataset(
        filtered_path,
        data=X_filtered,
        compressor=None,
        chunks=get_chunks_for_shape(X_filtered.shape),
    )


def get_compressed_path(
    *,
    lowcut: float,
    highcut: float,
    alg: str,
    compression_method: str,
    target_residual_stdev: float,
    compression_level: int,
) -> str:
    """
    Generate path for compressed data based on compression parameters.

    Args:
        lowcut: Lower cutoff frequency in Hz
        highcut: Upper cutoff frequency in Hz
        alg: Compression algorithm ('qfc' or 'qtc')
        compression_method: Compression method ('zstd' or 'zlib')
        target_residual_stdev: Target residual standard deviation
        compression_level: Compression level

    Returns:
        Path string for compressed data
    """
    return f"compressed/{alg}/{compression_method}/{target_residual_stdev}/compressed_{lowcut}-{highcut}_{compression_level}"


def do_compress(
    *,
    filtered_data: np.ndarray,
    zarr_path: str,
    lowcut: float = 300,
    highcut: float = 6000,
    alg: str,
    compression_method: str,
    target_residual_stdev: float,
    segment_length_sec: float = 0.1,
    zstd_level: int = 3,
    zlib_level: int = 3,
    estimate_compression_time: bool = False,
    compute_compression_ratio: bool = False,
    compute_residual_stdev: bool = False,
):
    """
    Compress filtered data using specified algorithm and parameters.

    Args:
        filtered_data: Filtered data array
        zarr_path: Path to zarr archive
        lowcut: Lower cutoff frequency in Hz
        highcut: Upper cutoff frequency in Hz
        alg: Compression algorithm ('qfc' or 'qtc')
        compression_method: Compression method ('zstd' or 'zlib')
        target_residual_stdev: Target residual standard deviation
        segment_length_sec: Segment length in seconds (for QFC)
        zstd_level: Zstandard compression level
        zlib_level: Zlib compression level
        estimate_compression_time: Whether to estimate compression time
        compute_compression_ratio: Whether to compute compression ratio
        compute_residual_stdev: Whether to compute residual standard deviation
    """
    path = get_compressed_path(
        lowcut=lowcut,
        highcut=highcut,
        alg=alg,
        compression_method=compression_method,
        target_residual_stdev=target_residual_stdev,
        compression_level=(
            zstd_level
            if compression_method == "zstd"
            else zlib_level if compression_method == "zlib" else 0
        ),
    )
    z = open_zarr(zarr_path, mode="r+")
    assert isinstance(z, zarr.Group)
    if path in z:
        x = z[path]
        assert isinstance(x, zarr.Array)
        matches = True
        if alg == "qfc" and target_residual_stdev == 0:
            matches = False
        if alg == "qfc" and target_residual_stdev > 0:
            if x.attrs["segment_length_sec"] != segment_length_sec:
                matches = False
        if matches:
            print(f"Compressed data already exists: {path}")
            return x
        else:
            print(f"Compressed data exists but with different parameters: {path}")
            z.__delitem__(path)
    X = filtered_data
    sampling_frequency = z.attrs["sampling_frequency"]
    segment_length = int(segment_length_sec * sampling_frequency)
    if target_residual_stdev > 0:
        if alg == "qfc":
            quant_scale_factor = qfc_estimate_quant_scale_factor(
                X, target_residual_stdev=target_residual_stdev
            )
            codec = QFCCodec(
                quant_scale_factor=quant_scale_factor,
                dtype="int16",
                compression_method=compression_method,  # type: ignore
                segment_length=segment_length,
                zstd_level=zstd_level,
                zlib_level=zlib_level,
            )

        elif alg == "qtc":
            quant_scale_factor = qtc_estimate_quant_scale_factor(
                X, target_residual_stdev=target_residual_stdev
            )
            codec = QTCCodec(
                quant_scale_factor=quant_scale_factor,
                dtype="int16",
                compression_method=compression_method,  # type: ignore
                zstd_level=zstd_level,
                zlib_level=zlib_level,
            )
        else:
            raise ValueError(f"Invalid compression algorithm: {alg}")
    else:
        if compression_method == "zstd":
            from numcodecs import Blosc
            clevel = min(zstd_level, 9)  # note that numcodecs does not support levels > 9
            codec = Blosc(cname="zstd", clevel=clevel)
        elif compression_method == "zlib":
            from numcodecs import Zlib
            codec = Zlib(level=zlib_level)
        else:
            raise ValueError(f"Invalid compression method: {compression_method}")
    print(
        f"Compressing data; alg={alg}, compression_method={compression_method}, target_residual_stdev={target_residual_stdev}"
    )
    print(f'Creating dataset "{path}" with shape {X.shape}...')
    ds = z.create_dataset(
        path,
        data=X,
        compressor=codec,
        chunks=get_chunks_for_shape(X.shape),  # type: ignore
    )
    if estimate_compression_time:
        print("Estimating compression time...")
        elapsed_times = []
        for i in range(3):
            memory_store = zarr.MemoryStore()
            z_memory = zarr.open(store=memory_store, mode="w")
            assert isinstance(z_memory, zarr.Group)
            timer = time.time()
            z_memory.create_dataset(
                "tmp",
                data=X,
                compressor=codec,
                chunks=get_chunks_for_shape(X.shape),  # type: ignore
            )
            elapsed_times.append(time.time() - timer)
        elapsed = np.mean(elapsed_times)
        ds.attrs["compression_time_sec"] = elapsed
        print(f"Estimated compression time: {elapsed:.4f} sec")
    if compute_compression_ratio or compute_residual_stdev:
        print("Computing compression ratio...")
        memory_store = zarr.MemoryStore()
        z_memory = zarr.open(store=memory_store, mode="w")
        assert isinstance(z_memory, zarr.Group)
        z_memory.create_dataset(
            "tmp",
            data=X,
            compressor=codec,
            chunks=get_chunks_for_shape(X.shape),  # type: ignore
        )
        if compute_compression_ratio:
            X_total_bytes = X.size * X.itemsize
            compressed_bytes = compute_size_of_zarr_store_excluding_meta(memory_store)
            compression_ratio = X_total_bytes / compressed_bytes
            ds.attrs["compression_ratio"] = compression_ratio
            print(f"Compression ratio: {compression_ratio:.4f}")
        if compute_residual_stdev:
            print("Computing residual stdev...")
            X_compressed = z_memory["tmp"]
            assert isinstance(X_compressed, zarr.Array)
            residual = X - X_compressed[:]  # type: ignore
            residual_stdev = np.std(residual)
            ds.attrs["residual_stdev"] = residual_stdev
            print(f"Residual stdev: {residual_stdev:.4f}")

    if alg == "qfc" and target_residual_stdev > 0:
        ds.attrs["segment_length_sec"] = segment_length_sec
    ds.attrs["lowcut"] = lowcut
    ds.attrs["highcut"] = highcut
    ds.attrs["alg"] = alg
    ds.attrs["compression_method"] = compression_method
    ds.attrs["target_residual_stdev"] = target_residual_stdev
    if compression_method == "zstd":
        ds.attrs["zstd_level"] = zstd_level
    elif compression_method == "zlib":
        ds.attrs["zlib_level"] = zlib_level
    return ds


def compute_size_of_zarr_store_excluding_meta(store):
    """
    Compute total size of zarr store excluding metadata.

    Args:
        store: Zarr store object

    Returns:
        Total size in bytes
    """
    total_size = 0
    for key in store:
        if not key.startswith("."):  # don't include .zattrs, etc
            v = store[key]
            total_size += len(v)
    return total_size


def get_chunks_for_shape(shape: Tuple[int, ...]) -> Tuple[int, ...]:
    """
    Calculate optimal chunk shape for zarr array based on array shape.

    Args:
        shape: Shape of array as tuple of integers

    Returns:
        Chunk shape as tuple of integers

    Raises:
        Exception: If shape is not 2-dimensional
    """
    if len(shape) == 2:
        num_channels = shape[1]
        target_num_entries = 4_000_000
        num_timepoints_per_chunk = target_num_entries // num_channels
        if shape[0] <= num_timepoints_per_chunk:
            return (shape[0], num_channels)
        elif shape[0] <= num_timepoints_per_chunk * 2:
            num_timepoints_per_chunk = (shape[0] + 1) // 2
            return (num_timepoints_per_chunk, num_channels)
        else:
            return (num_timepoints_per_chunk, num_channels)
    else:
        raise Exception(f"Cannot get chunks for shape: {shape}")


def remove_zarr_store(zarr_path: str):
    """
    Remove zarr store, with safety checks for remote stores.

    Args:
        zarr_path: Path to zarr store (local path or r2:// S3 path)

    Raises:
        Exception: If attempting to remove non-scratch remote store
    """
    if zarr_path.startswith("r2://"):
        if not zarr_path.startswith("r2://neurosift/scratch/"):
            # to be safe, only allow removing zarr stores in the scratch directory
            raise Exception(f"Cannot remove zarr store: {zarr_path}")
        import s3fs

        fs = s3fs.S3FileSystem(
            key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            config_kwargs={"max_pool_connections": 30},
        )
        pp = "/".join(zarr_path.split("/")[2:])
        assert pp.startswith("neurosift/scratch/")
        if not fs.exists(pp):
            print(f"{pp} does not exist.")
            return
        ok = input(f"Are you sure you want to remove {pp}? (y/n): ")
        if ok != "y":
            raise Exception(f"User did not confirm removal of {pp}")
        fs.rm(pp, recursive=True)
    else:
        if os.path.exists(zarr_path):
            shutil.rmtree(zarr_path)


def save_results_to_store(results: List[dict], zarr_path: str):
    """
    Save results to the zarr store, either locally or in S3.

    Args:
        results: List of benchmark results
        zarr_path: Path to zarr store
    """
    if zarr_path.startswith("r2://"):
        import s3fs
        fs = s3fs.S3FileSystem(
            key=os.environ.get("AWS_ACCESS_KEY_ID"),
            secret=os.environ.get("AWS_SECRET_ACCESS_KEY"),
            endpoint_url=os.environ.get("S3_ENDPOINT_URL"),
            config_kwargs={"max_pool_connections": 30},
        )
        pp = "/".join(zarr_path.split("/")[2:])
        with fs.open(f"{pp}/results.json", 'w') as f:
            json.dump(results, f, indent=2)
    else:
        # For local storage, save in the zarr directory
        results_path = os.path.join(zarr_path, "results.json")
        with open(results_path, 'w') as f:
            json.dump(results, f, indent=2)


if __name__ == "__main__":
    # https://neurosift.app/?p=/nwb&url=https://api.dandiarchive.org/api/assets/c04f6b30-82bf-40e1-9210-34f0bcd8be24/download/&dandisetId=000409&dandisetVersion=draft
    nwb_url = "https://api.dandiarchive.org/api/assets/c04f6b30-82bf-40e1-9210-34f0bcd8be24/download/"
    electrical_series_path = "/acquisition/ElectricalSeriesAp"
    duration_sec = 6
    channel_ids = [f"AP{i}" for i in np.arange(100, 132).tolist()]

    zarr_path = "r2://neurosift/scratch/qfc_benchmark/test1.zarr"

    # uncomment if want to start from scratch
    # remove_zarr_store(zarr_path)

    initiate_benchmark(
        zarr_path=zarr_path,
        nwb_url=nwb_url,
        electrical_series_path=electrical_series_path,
        duration_sec=duration_sec,
        channel_ids=channel_ids,
    )

    do_filter(zarr_path=zarr_path, lowcut=300, highcut=6000)

    algs = ["qfc", "qtc"]
    compression_methods = ["zstd", "zlib"]
    target_residual_stdevs = [0, 1, 2, 3, 4, 5, 6, 7, 8]
    zlib_level = 6
    zstd_level = 15
    lowcut = 300
    highcut = 6000

    z = open_zarr(zarr_path, mode="r")
    filtered_path = get_filtered_path(lowcut=lowcut, highcut=highcut)
    if filtered_path not in z:
        raise Exception(f"Filtered data not found: {filtered_path}")
    A = z[filtered_path]
    assert isinstance(A, zarr.Array)
    X_filtered = A[:]

    results = []
    for alg in algs:
        for compression_method in compression_methods:
            for target_resid_stdev in target_residual_stdevs:
                ds = do_compress(
                    filtered_data=X_filtered,
                    zarr_path=zarr_path,
                    lowcut=lowcut,
                    highcut=highcut,
                    alg=alg,
                    compression_method=compression_method,
                    target_residual_stdev=target_resid_stdev,
                    estimate_compression_time=True,
                    compute_compression_ratio=True,
                    compute_residual_stdev=True,
                    zlib_level=zlib_level,
                    zstd_level=zstd_level,
                )
                compression_time_sec = ds.attrs.get("compression_time_sec", None)
                compression_ratio = ds.attrs.get("compression_ratio", None)
                resid_stdev = ds.attrs.get("target_residual_stdev", None)
                results.append(
                    {
                        "alg": alg,
                        "compression_method": compression_method,
                        "target_residual_stdev": resid_stdev,
                        "residual_stdev": resid_stdev,
                        "compression_ratio": compression_ratio,
                        "compression_time_sec": compression_time_sec,
                        "lowcut": lowcut,
                        "highcut": highcut,
                        "compression_level": zlib_level if compression_method == "zlib" else zstd_level,
                    }
                )

    # Save results to the zarr store
    save_results_to_store(results, zarr_path)
