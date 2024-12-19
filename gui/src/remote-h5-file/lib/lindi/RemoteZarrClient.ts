/* eslint-disable @typescript-eslint/no-explicit-any */
import { ZMetaDataZArray } from "./RemoteH5FileLindi";
import zarrDecodeChunkArray from "./zarrDecodeChunkArray";

export class RemoteZarrClient {
  #fileContentCache: {
    [key: string]: { content: any | undefined; found: boolean };
  } = {};
  #inProgressReads: { [key: string]: boolean } = {};
  constructor(
    private url: string
  ) {}
  async readJson(path: string, defaultVal?: any): Promise<{ [key: string]: any } | undefined> {
    const buf = await this.readBinary(path, { decodeArray: false, defaultVal: defaultVal ? new TextEncoder().encode(JSON.stringify(defaultVal)) : undefined });
    if (!buf) return undefined;
    const text = new TextDecoder().decode(buf);
    // replace NaN by "NaN" so that JSON.parse doesn't choke on it
    // text = text.replace(/NaN/g, '"___NaN___"'); // This is not ideal. See: https://stackoverflow.com/a/15228712
    // BUT we want to make sure we don't replace NaN within quoted strings
    // Here's an example where this matters: https://neurosift.app/?p=/nwb&dandisetId=000409&dandisetVersion=draft&url=https://api.dandiarchive.org/api/assets/54b277ce-2da7-4730-b86b-cfc8dbf9c6fd/download/
    //    raw/intervals/contrast_left
    let newText: string;
    if (text.includes("NaN")) {
      newText = "";
      let inString = false;
      let isEscaped = false;
      for (let i = 0; i < text.length; i++) {
        const c = text[i];
        if (c === '"' && !isEscaped) inString = !inString;
        if (!inString && c === "N" && text.slice(i, i + 3) === "NaN") {
          newText += '"___NaN___"';
          i += 2;
        } else {
          newText += c;
        }
        isEscaped = c === "\\" && !isEscaped;
      }
    } else {
      newText = text;
    }
    try {
      return JSON.parse(newText, (_key, value) => {
        if (value === "___NaN___") return NaN;
        return value;
      });
    } catch (e) {
      console.warn(text);
      throw Error("Failed to parse JSON for " + path + ": " + e);
    }
  }
  async readBinary(
    path: string,
    o: {
      decodeArray?: boolean;
      startByte?: number;
      endByte?: number;
      disableCache?: boolean;
      defaultVal?: ArrayBuffer;
    },
  ): Promise<any | undefined> {
    if (o.startByte !== undefined) {
      if (o.decodeArray)
        throw Error("Cannot decode array and read a slice at the same time");
      if (o.endByte === undefined)
        throw Error("If you specify startByte, you must also specify endByte");
    } else if (o.endByte !== undefined) {
      throw Error("If you specify endByte, you must also specify startByte");
    }
    if (
      o.endByte !== undefined &&
      o.startByte !== undefined &&
      o.endByte < o.startByte
    ) {
      throw Error(
        `endByte must be greater than or equal to startByte: ${o.startByte} ${o.endByte} for ${path}`,
      );
    }
    if (
      o.endByte !== undefined &&
      o.startByte !== undefined &&
      o.endByte === o.startByte
    ) {
      return new ArrayBuffer(0);
    }
    const kk =
      path +
      "|" +
      (o.decodeArray ? "decode" : "") +
      "|" +
      o.startByte +
      "|" +
      o.endByte;
    while (this.#inProgressReads[kk]) {
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
    this.#inProgressReads[kk] = true;
    try {
      if (path.startsWith("/")) path = path.slice(1);
      if (this.#fileContentCache[kk]) {
        if (this.#fileContentCache[kk].found) {
          return this.#fileContentCache[kk].content;
        }
        return undefined;
      }
      const path2 = path.startsWith("/") ? path.slice(1) : path;
      const readRemoteFile = async (url: string) => {
        const response = await fetch(url);
        if (!response.ok) {
          throw new Error(`Error loading data from url: ${url}`);
        }
        return await response.arrayBuffer();
      }
      let buf;
      try {
        buf = await readRemoteFile(this.url + "/" + path2);
      }
      catch (e) {
        if (o.defaultVal) {
          return o.defaultVal;
        }
        throw e;
      }
      if (o.startByte !== undefined) {
        buf = buf.slice(o.startByte, o.endByte);
      }
      if (o.decodeArray) {
        const parentPath = path.split("/").slice(0, -1).join("/");
        const zarray = (await this.readJson(parentPath + "/.zarray")) as
          | ZMetaDataZArray
          | undefined;
        if (!zarray) throw Error("Failed to read .zarray for " + path);
        try {
          buf = await zarrDecodeChunkArray(
            buf,
            zarray.dtype,
            zarray.compressor,
            zarray.filters,
            zarray.chunks,
          );
        } catch (e) {
          throw Error(`Failed to decode chunk array for ${path}: ${e}`);
        }
      }
      if (buf) {
        this.#fileContentCache[kk] = { content: buf, found: true };
      } else {
        this.#fileContentCache[kk] = { content: undefined, found: false };
      }
      return buf;
    } catch (e) {
      this.#fileContentCache[kk] = { content: undefined, found: false }; // important to do this so we don't keep trying to read the same file
      throw e;
    } finally {
      this.#inProgressReads[kk] = false;
    }
  }
}

export default RemoteZarrClient;
