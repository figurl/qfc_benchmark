/* eslint-disable @typescript-eslint/no-explicit-any */
import pako from "pako";
import dw from "discrete-wavelets";
import { _get_segment_ranges, concatenateFloat32Arrays, concatenateInt16Arrays, zstdDecompress } from "./qfc";

type QwcCompressionOpts = {
  compression_method: "zlib" | "zstd";
  dtype: "float32" | "int16";
  id: "qfc_qwc";
  pywt_wavelet: string,
  pywt_level: number,
  pywt_mode: string,
  quant_scale_factor: number;
  segment_length: number;
  zlib_level: number;
  zstd_level: number;
};

const isQwcCompressionOpts = (x: any): x is QwcCompressionOpts => {
  if (!x) return false;
  if (typeof x !== "object") return false;
  if (x.compression_method !== "zlib" && x.compression_method !== "zstd")
    return false;
  if (x.dtype !== "float32" && x.dtype !== "int16") return false;
  if (x.id !== "qfc_qwc") return false;
  if (typeof x.pywt_wavelet !== "string") return false;
  if (typeof x.pywt_level !== "number") return false;
  if (typeof x.pywt_mode !== "string") return false;
  if (typeof x.quant_scale_factor !== "number") return false;
  if (typeof x.segment_length !== "number") return false;
  if (typeof x.zlib_level !== "number") return false;
  if (typeof x.zstd_level !== "number") return false;
  return true;
};

export const qwcDecompress = async (
  buf: ArrayBuffer,
  shape: number[],
  compressor: QwcCompressionOpts,
): Promise<any> => {
  if (!isQwcCompressionOpts(compressor)) {
    console.warn(compressor);
    throw Error("Invalid qwc compressor");
  }

  const header = new Int32Array(buf, 0, 5);
  if (header[0] !== 9364182) {
    throw Error(`Invalid header[0]: ${header[0]}`);
  }
  if (header[1] !== 1) {
    throw Error(`Invalid header[1]: ${header[1]}`);
  }
  const num_samples = header[2];
  const num_channels = header[3];
  if (num_samples !== shape[0]) {
    throw Error(
      `Unexpected num samples in header. Expected ${shape[0]}, got ${num_samples}`,
    );
  }
  if (num_channels !== shape[1]) {
    throw Error(
      `Unexpected num channels in header. Expected ${shape[1]}, got ${num_channels}`,
    );
  }
  if (header[4] !== compressor.segment_length) {
    throw Error(
      `Unexpected segment length in header. Expected ${compressor.segment_length}, got ${header[4]}`,
    );
  }

  const decompressed_buf = await qwc_multi_segment_decompress({
    buf: buf.slice(4 * 5),
    dtype: compressor.dtype,
    pywt_wavelet: compressor.pywt_wavelet,
    num_channels,
    num_samples,
    segment_length: compressor.segment_length,
    quant_scale_factor: compressor.quant_scale_factor,
    compression_method: compressor.compression_method,
  });

  if (
    decompressed_buf.byteLength !==
    num_samples * num_channels * (compressor.dtype === "float32" ? 4 : 2)
  ) {
    console.warn("compressor", compressor);
    throw Error(
      `Unexpected decompressed buffer length. Expected ${num_samples * num_channels * (compressor.dtype === "float32" ? 4 : 2)}, got ${decompressed_buf.byteLength}`,
    );
  }

  return decompressed_buf;
};

const qwc_multi_segment_decompress = async (o: {
  buf: ArrayBuffer;
  dtype: "float32" | "int16";
  pywt_wavelet: string,
  num_channels: number;
  num_samples: number;
  segment_length: number;
  quant_scale_factor: number;
  compression_method: "zlib" | "zstd";
}): Promise<ArrayBuffer> => {
  const {
    buf,
    dtype,
    pywt_wavelet,
    num_channels,
    num_samples,
    segment_length,
    quant_scale_factor,
    compression_method,
  } = o;

  const { coeff_sizes_list, X } = _parse_qwc_compressed_bytes_multi(buf);

  let decompressedArray: Int16Array;
  if (compression_method === "zlib") {
    decompressedArray = new Int16Array(pako.inflate(X).buffer);
  } else if (compression_method === "zstd") {
    decompressedArray = new Int16Array(await zstdDecompress(X));
  } else {
    throw Error(`Unexpected compression method: ${compression_method}`);
  }

  return await qwc_multi_segment_inv_pre_compress({
    coeff_sizes_list,
    array: decompressedArray,
    pywt_wavelet,
    quant_scale_factor,
    segment_length,
    dtype,
    num_channels,
    num_samples,
  });
};

const qwc_multi_segment_inv_pre_compress = async (o: {
  coeff_sizes_list: number[][];
  array: Int16Array;
  pywt_wavelet: string,
  quant_scale_factor: number;
  segment_length: number;
  dtype: "int16" | "float32";
  num_samples: number;
  num_channels: number;
}): Promise<ArrayBuffer> => {
  const {
    coeff_sizes_list,
    array,
    pywt_wavelet,
    quant_scale_factor,
    segment_length,
    dtype,
    num_samples,
    num_channels,
  } = o;
  if (segment_length > 0 && segment_length < num_samples) {
    const numCoeffsList = coeff_sizes_list.map((coeff_sizes) =>
      coeff_sizes.reduce((a, b) => a + b, 0),
    );
    const segment_ranges = _get_segment_ranges(num_samples, segment_length);
    const coeff_ranges: number[][] = [];
    let offset = 0;
    for (let i = 0; i < numCoeffsList.length; i++) {
      coeff_ranges.push([offset, offset + numCoeffsList[i]]);
      offset += numCoeffsList[i];
    }
    const prepared_segments = await Promise.all(
      segment_ranges.map(
        async (segment_range, ii) =>
          await qwc_inv_pre_compress({
            coeff_sizes: coeff_sizes_list[ii],
            array: array.slice(
              coeff_ranges[ii][0] * num_channels,
              coeff_ranges[ii][1] * num_channels,
            ),
            pywt_wavelet,
            quant_scale_factor,
            dtype,
            num_samples: segment_range[1] - segment_range[0],
            num_channels,
          }),
      ),
    );
    if (dtype === "int16") {
      return concatenateInt16Arrays(prepared_segments as Int16Array[]);
    } else if (dtype === "float32") {
      return concatenateFloat32Arrays(prepared_segments as Float32Array[]);
    } else {
      throw Error(`Unexpected dtype: ${dtype}`);
    }
  } else {
    return await qwc_inv_pre_compress({
      coeff_sizes: coeff_sizes_list[0],
      array,
      pywt_wavelet,
      quant_scale_factor,
      dtype,
      num_samples,
      num_channels,
    });
  }
};

const _parse_qwc_compressed_bytes_multi = (compressed_bytes: ArrayBuffer): {
  coeff_sizes_list: number[][];
  X: ArrayBuffer;
} => {
  const view = new DataView(compressed_bytes);
  const num_segments = view.getInt32(0, true);
  const coeff_sizes_list: (number[])[] = [];
  let offset = 4;
  for (let i = 0; i < num_segments; i++) {
    const num_coeff_sizes = view.getInt32(offset, true);
    const coeff_sizes = new Int32Array(compressed_bytes, offset + 4, num_coeff_sizes);
    coeff_sizes_list.push(Array.from(coeff_sizes));
    offset += 4 + num_coeff_sizes * 4;
  }
  const X = compressed_bytes.slice(offset);
  return {coeff_sizes_list, X};
}

const qwc_inv_pre_compress = async (o: {
  coeff_sizes: number[];
  array: Int16Array;
  quant_scale_factor: number;
  pywt_wavelet: string,
  dtype: "int16" | "float32";
  num_samples: number;
  num_channels: number;
}): Promise<Int16Array | Float32Array> => {
  const { array, quant_scale_factor, dtype, num_samples, num_channels, pywt_wavelet, coeff_sizes } = o;

  const ret = new Float32Array(num_samples * num_channels);
  for (let iChannel = 0; iChannel < num_channels; iChannel++) {
    let index = 0;
    const segment_coeffs: number[][] = [];
    for (let j = 0; j < coeff_sizes.length; j++) {
      const coeff_size = coeff_sizes[j];
      const xx: number[] = [];
      for (let jj = 0; jj < coeff_size; jj++) {
        xx.push(array[index * num_channels + iChannel] * 1.0 / quant_scale_factor);
        index += 1;
      }
      segment_coeffs.push(xx);
    }
    const data = dw.waverec(segment_coeffs, pywt_wavelet as any);
    for (let iii = 0; iii < num_samples; iii++) {
      ret[iii * num_channels + iChannel] = data[iii];
    }
  }

  if (dtype === "int16") {
    const ret2 = new Int16Array(ret.length);
    for (let i = 0; i < ret.length; i++) {
      ret2[i] = Math.round(ret[i]);
    }
    return ret2;
  } else if (dtype === "float32") {
    return ret;
  } else {
    throw Error(`Unexpected dtype: ${dtype}`);
  }
};