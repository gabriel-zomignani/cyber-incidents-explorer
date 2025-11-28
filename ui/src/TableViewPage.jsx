import { useEffect, useMemo, useState } from "react";
import Papa from "papaparse";
import TableView from "./TableView";
import "./TableView.css";

// ==== CSV LOCATION ====
const CSV_URL = "/data/cyber_clean.csv";

export default function TableViewPage() {
  const [raw, setRaw] = useState([]);
  const [q, setQ] = useState("");
  const [actor, setActor] = useState("");
  const [country, setCountry] = useState("");
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);

  // Load CSV once
  useEffect(() => {
    Papa.parse(CSV_URL, {
      download: true,
      header: true,
      skipEmptyLines: true,
      complete: (res) => setRaw(res.data),
      error: (e) => console.error("CSV error:", e),
    });
  }, []);

  // Options for filters
  const actorOptions = useMemo(
    () => Array.from(new Set(raw.map((x) => x.actor).filter(Boolean))).sort(),
    [raw]
  );

  const countryOptions = useMemo(
    () => Array.from(new Set(raw.map((x) => x.country).filter(Boolean))).sort(),
    [raw]
  );

  // Filtered rows
  const filtered = useMemo(() => {
    let rows = raw;

    if (q) {
      const needle = q.toLowerCase();
      rows = rows.filter(
        (r) =>
          (r.description ?? "").toLowerCase().includes(needle) ||
          (r.organization ?? "").toLowerCase().includes(needle)
      );
    }

    if (actor) rows = rows.filter((r) => r.actor === actor);
    if (country) rows = rows.filter((r) => r.country === country);

    return rows;
  }, [raw, q, actor, country]);

  const total = filtered.length;
  const pages = Math.max(1, Math.ceil(total / pageSize));

  const pageRows = useMemo(() => {
    const start = (page - 1) * pageSize;
    return filtered.slice(start, start + pageSize);
  }, [filtered, page, pageSize]);

  // Reset page if filters change
  useEffect(() => setPage(1), [q, actor, country, pageSize]);

  return (
    <div className="page">
      <h1>Cyber Events — Table</h1>

      {/* Filters & controls */}
      <div className="controls">
        <input
          className="table-input"
          placeholder="Search description or organization…"
          value={q}
          onChange={(e) => setQ(e.target.value)}
        />

        <select value={actor} onChange={(e) => setActor(e.target.value)}>
          <option value="">Actor (all)</option>
          {actorOptions.map((a) => (
            <option key={a} value={a}>
              {a}
            </option>
          ))}
        </select>

        <select value={country} onChange={(e) => setCountry(e.target.value)}>
          <option value="">Country (all)</option>
          {countryOptions.map((c) => (
            <option key={c} value={c}>
              {c}
            </option>
          ))}
        </select>

        <select
          value={pageSize}
          onChange={(e) => setPageSize(Number(e.target.value))}
        >
          {[10, 25, 50, 100].map((n) => (
            <option key={n} value={n}>
              {n} / page
            </option>
          ))}
        </select>

        <div style={{ color: "var(--muted)" }}>Rows: {total}</div>
      </div>

      {/* Table */}
      <TableView rows={pageRows} />

      {/* Pager */}
      <div className="pager">
        <button onClick={() => setPage(1)} disabled={page === 1}>
          « First
        </button>
        <button
          onClick={() => setPage((p) => Math.max(1, p - 1))}
          disabled={page === 1}
        >
          ‹ Prev
        </button>
        <div>
          Page {page} / {pages}
        </div>
        <button
          onClick={() => setPage((p) => Math.min(pages, p + 1))}
          disabled={page === pages}
        >
          Next ›
        </button>
        <button onClick={() => setPage(pages)} disabled={page === pages}>
          Last »
        </button>
      </div>
    </div>
  );
}
