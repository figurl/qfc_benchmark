import { useWindowDimensions } from "@fi-sci/misc";
import "./App.css";

import { FunctionComponent, useEffect, useMemo, useState } from "react";
import {
  ProvideDocumentWidth,
  useDocumentWidth,
} from "./Markdown/DocumentWidthContext";
import LazyPlotlyPlot from "./Plotly/LazyPlotlyPlot";
import { RemoteH5FileLindi } from "./remote-h5-file";
import RemoteZarrClient from "./remote-h5-file/lib/lindi/RemoteZarrClient";

function App() {
  const { width, height } = useWindowDimensions();
  const mainAreaWidth = Math.min(width - 30, 900);
  const offsetLeft = (width - mainAreaWidth) / 2;
  const [okayToViewSmallScreen, setOkayToViewSmallScreen] = useState(false);
  if (width < 800 && !okayToViewSmallScreen) {
    return <SmallScreenMessage onOkay={() => setOkayToViewSmallScreen(true)} />;
  }
  return (
    <div style={{ position: "absolute", width, height, overflow: "auto" }}>
      <div
        style={{ position: "absolute", left: offsetLeft, width: mainAreaWidth }}
      >
        <ProvideDocumentWidth width={mainAreaWidth}>
          <Document />
        </ProvideDocumentWidth>
      </div>
    </div>
  );
}

// eslint-disable-next-line @typescript-eslint/no-empty-object-type
type DocumentProps = {};

type CompressionMethod = "zlib" | "zstd";

type Selection = {
  alg: "qfc" | "qtc" | "db4" | "db8" | "haar";
  filtSet: string;
  targetResidualStdev: number;
  compressionMethod: CompressionMethod;
};

const defaultSelection: Selection = {
  alg: "qfc",
  filtSet: "300-6000",
  targetResidualStdev: 1,
  compressionMethod: "zlib",
};

const Document: FunctionComponent<DocumentProps> = () => {
  const results = useResults();
  const [selection, setSelection] = useState<Selection>(defaultSelection);
  const documentWidth = useDocumentWidth();
  if (!results) {
    return <div>Loading results...</div>;
  }
  return (
    <div>
      <h1>QFC Benchmark</h1>
      <AlgSelection
        results={results}
        selection={selection}
        setSelection={setSelection}
      />
      &nbsp;&nbsp;
      <FilterSelection
        results={results}
        selectedFiltSet={selection.filtSet}
        setSelectedFiltSet={(v) => setSelection({ ...selection, filtSet: v })}
      />
      &nbsp;&nbsp;
      <TargetResidualStdevSelection
        results={results}
        selectedTargetResidualStdev={selection.targetResidualStdev}
        setSelectedTargetResidualStdev={(v) =>
          setSelection({ ...selection, targetResidualStdev: v })
        }
      />
      &nbsp;&nbsp;
      <CompressionMethodSelection
        results={results}
        selectedCompressionMethod={selection.compressionMethod}
        setSelectedCompressionMethod={(v) =>
          setSelection({ ...selection, compressionMethod: v })
        }
      />
      <div style={{ position: "relative", width: "100%" }}>
        <div style={{ display: "flex" }}>
          <CompressionRatioVsResidualStdevPlot
            results={results}
            filtSet="300-6000"
            selection={selection}
            width={documentWidth / 2}
          />
          <CompressionRatioVsResidualStdevPlot
            results={results}
            filtSet="300"
            selection={selection}
            width={documentWidth / 2}
          />
        </div>
      </div>
      <TracePlot
        filtSet={selection.filtSet}
        targetResidualStdev={selection.targetResidualStdev}
        compressionMethod={selection.compressionMethod}
        alg={selection.alg}
        results={results}
        width={documentWidth}
      />
    </div>
  );
};

type FilterSelectionProps = {
  results: Result[];
  selectedFiltSet: string;
  setSelectedFiltSet: (filtSet: string) => void;
};

const FilterSelection: FunctionComponent<FilterSelectionProps> = ({
  results,
  selectedFiltSet,
  setSelectedFiltSet,
}) => {
  const { filtSets } = useX(results);
  return (
    <>
      Filter:&nbsp;
      <select
        value={selectedFiltSet}
        onChange={(e) => setSelectedFiltSet(e.target.value)}
      >
        {filtSets.map((filtSet) => (
          <option key={filtSet} value={filtSet}>
            {labelForFilt(filtSet)}
          </option>
        ))}
      </select>
    </>
  );
};

type TargetResidualStdevSelectionProps = {
  results: Result[];
  selectedTargetResidualStdev: number | undefined;
  setSelectedTargetResidualStdev: (targetResidualStdev: number) => void;
};

const TargetResidualStdevSelection: FunctionComponent<
  TargetResidualStdevSelectionProps
> = ({
  results,
  selectedTargetResidualStdev,
  setSelectedTargetResidualStdev,
}) => {
  const { targetResidualStdevs } = useX(results);
  useEffect(() => {
    if (!selectedTargetResidualStdev) return;
    if (!targetResidualStdevs.includes(selectedTargetResidualStdev)) {
      if (targetResidualStdevs.length) {
        setSelectedTargetResidualStdev(targetResidualStdevs[0]);
      }
    }
  }, [targetResidualStdevs, selectedTargetResidualStdev, setSelectedTargetResidualStdev]);
  return (
    <select
      style={{ width: 200 }}
      value={selectedTargetResidualStdev}
      onChange={(e) =>
        setSelectedTargetResidualStdev(parseFloat(e.target.value))
      }
    >
      {targetResidualStdevs.map((targetResidualStdev) => (
        <option key={targetResidualStdev} value={targetResidualStdev}>
          Target resid stdev: {targetResidualStdev}
        </option>
      ))}
    </select>
  );
};

type CompressionMethodSelectionProps = {
  results: Result[];
  selectedCompressionMethod: CompressionMethod;
  setSelectedCompressionMethod: (compressionMethod: CompressionMethod) => void;
};

const CompressionMethodSelection: FunctionComponent<
  CompressionMethodSelectionProps
> = ({ results, selectedCompressionMethod, setSelectedCompressionMethod }) => {
  const { compressionMethods } = useX(results);
  useEffect(() => {
    if (!compressionMethods.includes(selectedCompressionMethod)) {
      if (compressionMethods.length) {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        setSelectedCompressionMethod(compressionMethods[0] as any);
      }
    }
  }, [compressionMethods, selectedCompressionMethod, setSelectedCompressionMethod]);
  return (
    <select
      value={selectedCompressionMethod}
      onChange={(e) =>
        setSelectedCompressionMethod(e.target.value as CompressionMethod)
      }
    >
      {compressionMethods.map((compressionMethod) => (
        <option key={compressionMethod} value={compressionMethod}>
          {compressionMethod}
        </option>
      ))}
    </select>
  );
};

type CompressionRatioVsResidualStdevPlotProps = {
  results: Result[];
  filtSet: string;
  selection: Selection;
  width: number;
};

const CompressionRatioVsResidualStdevPlot: FunctionComponent<
  CompressionRatioVsResidualStdevPlotProps
> = ({ results, filtSet, selection, width }) => {
  const { algs, compressionMethods } = useX(results);

  const p = filtSet.split("-");
  const lowcut = parseInt(p[0]);
  const highcut = parseInt(p[1]) || null;
  const resultsFiltered = results.filter(
    (r) => r.lowcut === lowcut && r.highcut === highcut
  );
  const data = [];
  for (const alg of algs) {
    for (const compressionMethod of compressionMethods) {
      const filteredResults = resultsFiltered.filter(
        (r) => r.alg === alg && r.compression_method === compressionMethod
      );
      filteredResults.sort((a, b) => a.residual_stdev - b.residual_stdev);
      const selectedIndex =
        filtSet === selection.filtSet
          ? filteredResults.findIndex(
              (r) =>
                r.target_residual_stdev === selection.targetResidualStdev &&
                r.compression_method === selection.compressionMethod
            )
          : undefined;
      if (filteredResults.length) {
        const label = `${alg}-${compressionMethod}`;
        const residualStdevs = filteredResults.map((r) => r.residual_stdev);
        const compressionRatios = filteredResults.map(
          (r) => r.compression_ratio
        );
        data.push({
          x: residualStdevs,
          y: compressionRatios,
          mode: "lines+markers",
          type: "scatter",
          name: label,
          marker: {
            size: residualStdevs.map((x, i) => {
              if (i === selectedIndex) {
                return 20;
              } else {
                return 10;
              }
            }),
          },
        });
      }
    }
  }

  const layout = {
    width: width,
    height: 400,
    title: `${labelForFilt(filtSet)}`,
    xaxis: {
      title: "Residual Stdev",
    },
    yaxis: {
      title: "Compression Ratio",
    },
  };

  return <LazyPlotlyPlot data={data} layout={layout} />;
};

type AlgSelectionProps = {
  results: Result[];
  selection: Selection;
  setSelection: (selection: Selection) => void;
};

const AlgSelection: FunctionComponent<AlgSelectionProps> = ({
  results,
  selection,
  setSelection,
}) => {
  const { algs } = useX(results);
  return (
    <select
      value={selection.alg}
      onChange={(e) =>
        setSelection({ ...selection, alg: e.target.value as "qfc" | "qtc" | "db4" | "db8" | "haar" })
      }
    >
      {algs.map((alg) => (
        <option key={alg} value={alg}>
          {alg}
        </option>
      ))}
    </select>
  );
};

const labelForFilt = (filtSet: string) => {
  const p = filtSet.split("-");
  const lowcut = parseInt(p[0]);
  const highcut = parseInt(p[1]) || null;
  if (highcut !== null) {
    return `Bandpass filter ${lowcut}-${highcut} Hz`;
  } else {
    return `Highpass filter ${lowcut} Hz`;
  }
};

const useX = (results: Result[]) => {
  return useMemo(() => {
    const filtSets: string[] = [];
    const algs: string[] = [];
    const compressionMethods: string[] = [];
    const targetResidualStdevs: number[] = [];
    for (const r of results) {
      const lowcut = r.lowcut;
      const highcut = r.highcut;
      const k = highcut ? `${lowcut}-${highcut}` : `${lowcut}`;
      const alg = r.alg;
      const compressionMethod = r.compression_method;
      const targetResidualStdev = r.target_residual_stdev;
      if (!algs.includes(alg)) {
        algs.push(alg);
      }
      if (!compressionMethods.includes(compressionMethod)) {
        compressionMethods.push(compressionMethod);
      }
      if (!targetResidualStdevs.includes(targetResidualStdev)) {
        targetResidualStdevs.push(targetResidualStdev);
      }
      if (!filtSets.includes(k)) {
        filtSets.push(k);
      }
    }
    return { filtSets, algs, compressionMethods, targetResidualStdevs };
  }, [results]);
};

type Result = {
  dataset_path: string;
  alg: string;
  compression_method: string;
  target_residual_stdev: number;
  residual_stdev: number;
  compression_ratio: number;
  compression_time_sec: number;
  lowcut: number;
  highcut: number;
  compression_level: number;
};

const zarrUrl = "https://neurosift.org/scratch/qfc_benchmark/test1.zarr";

const useResults = () => {
  const url = `${zarrUrl}/results.json`;
  const [results, setResults] = useState<Result[] | undefined>(undefined);
  useEffect(() => {
    let canceled = false;
    fetch(url)
      .then((response) => response.json())
      .then((data) => {
        if (!canceled) {
          setResults(data);
        }
      });
    return () => {
      canceled = true;
    };
  }, []);
  return results;
};

type TracePlotProps = {
  filtSet: string;
  targetResidualStdev: number;
  compressionMethod: CompressionMethod;
  alg: string;
  results: Result[];
  width: number;
};

const TracePlot: FunctionComponent<TracePlotProps> = ({
  filtSet,
  targetResidualStdev,
  compressionMethod,
  alg,
  results,
  width,
}) => {
  const p = filtSet.split("-");
  const lowcut = parseInt(p[0]);
  const highcut = parseInt(p[1]) || null;
  const result = results.find((r) => {
    return (
      r.lowcut === lowcut &&
      r.highcut === highcut &&
      r.target_residual_stdev === targetResidualStdev &&
      r.compression_method === compressionMethod &&
      r.alg === alg
    );
  });
  console.log(
    "--- aaa",
    result,
    lowcut,
    highcut,
    targetResidualStdev,
    compressionMethod,
    alg
  );
  console.log('---- results', results);
  if (!result) {
    return <div>No results found for selected parameters</div>;
  }
  const compressedPath = result.dataset_path;
  const filteredPath = `filtered_${filtSet}`;
  return (
    <TracePlotChild
      compressedPath={compressedPath}
      filteredPath={filteredPath}
      width={width}
    />
  );
};

type TracePlotChildProps = {
  compressedPath: string;
  filteredPath: string;
  width: number;
};

const TracePlotChild: FunctionComponent<TracePlotChildProps> = ({
  compressedPath,
  filteredPath,
  width,
}) => {
  const z = useRemoteZarr(zarrUrl);
  const [compressedData, setCompressedData] = useState<number[] | undefined>(
    undefined
  );
  const [filteredData, setFilteredData] = useState<number[] | undefined>(
    undefined
  );
  useEffect(() => {
    const load = async () => {
      if (!z) return;
      const dsCompressed = await z.getDataset(compressedPath);
      if (!dsCompressed) {
        console.error(`Dataset not found: ${compressedPath}`);
        return;
      }
      const dsFiltered = await z.getDataset(filteredPath);
      if (!dsFiltered) {
        console.error(`Dataset not found: ${filteredPath}`);
        return;
      }
      const compressedData = await z.getDatasetData(compressedPath, {
        slice: [[0, 1000]],
      });
      if (!compressedData) {
        console.error(`Failed to get data for ${compressedPath}`);
        return;
      }
      const filteredData = await z.getDatasetData(filteredPath, {
        slice: [[0, 1000]],
      });
      if (!filteredData) {
        console.error(`Failed to get data for ${filteredPath}`);
        return;
      }
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      setCompressedData(compressedData as any as number[]);
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
      setFilteredData(filteredData as any as number[]);
    };
    load();
  }, [z, compressedPath, filteredPath]);
  if (!compressedData) {
    return <div>Loading data...</div>;
  }
  if (!filteredData) {
    return <div>Loading data...</div>;
  }
  const tt: number[] = [];
  const dd: number[] = [];
  const ch = 1;
  for (let ii = ch; ii < compressedData.length; ii += 32) {
    tt.push(ii / 32);
    dd.push(compressedData[ii]);
  }
  const ttF: number[] = [];
  const ddF: number[] = [];
  for (let ii = ch; ii < filteredData.length; ii += 32) {
    ttF.push(ii / 32);
    ddF.push(filteredData[ii]);
  }
  const data = [
    {
      x: tt,
      y: dd,
      type: "scatter",
    },
    {
      x: ttF,
      y: ddF,
      type: "scatter",
    },
  ];
  const layout = {
    width: width,
    height: 400,
    title: "Compressed data",
  };
  return <LazyPlotlyPlot data={data} layout={layout} />;
};

const useRemoteZarr = (url: string) => {
  const [z, setZ] = useState<RemoteH5FileLindi | undefined>(undefined);
  useEffect(() => {
    let canceled = false;
    const load = async () => {
      const client = new RemoteZarrClient(url);
      if (canceled) return;
      const x = new RemoteH5FileLindi(url, client, {});
      setZ(x);
    };
    load();
    return () => {
      canceled = true;
    };
  }, [url]);
  return z;
};

const SmallScreenMessage: FunctionComponent<{ onOkay: () => void }> = ({
  onOkay,
}) => {
  return (
    <div style={{ padding: 20 }}>
      <p>
        This page is not optimized for small screens or mobile devices. Please
        use a larger screen or expand your browser window width.
      </p>
      <p>
        <button onClick={onOkay}>I understand, continue anyway</button>
      </p>
    </div>
  );
};

export default App;
