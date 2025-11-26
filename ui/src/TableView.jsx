import { useMemo, useState } from "react";

export default function TableView({ rows }) {
  // columns from first row
  const columns = useMemo(() => (rows?.length ? Object.keys(rows[0]) : []), [rows]);

  // UI state
  const [query, setQuery] = useState("");
  const [sortBy, setSortBy] = useState("");        // column key
  const [sortDir, setSortDir] = useState("asc");   // 'asc' | 'desc'
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(25);

  // Helpers
  const normalize = (v) =>
    v == null ? "" : typeof v === "string" ? v : String(v);

  const cmp = (a, b) => {
    // Special case: event_date if present => compare as dates
    if (sortBy === "event_date") {
      const da = new Date(a?.[sortBy] ?? "");
      const db = new Date(b?.[sortBy] ?? "");
      return da - db;
    }

    const va = normalize(a?.[sortBy]).toLowerCase();
    const vb = normalize(b?.[sortBy]).toLowerCase();

    // numeric if both look like numbers
    const na = Number(va), nb = Number(vb);
    if (!Number.isNaN(na) && !Number.isNaN(nb)) return na - nb;

    return va.localeCompare(vb);
  };

  // filter
  const filtered = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return rows ?? [];
    return (rows ?? []).filter((r) =>
      columns.some((c) => normalize(r[c]).toLowerCase().includes(q))
    );
  }, [rows, columns, query]);

  // sort
  const sorted = useMemo(() => {
    if (!sortBy) return filtered;
    const copy = [...filtered].sort(cmp);
    return sortDir === "asc" ? copy : copy.reverse();
  }, [filtered, sortBy, sortDir]);

  // pagination
  const total = sorted.length;
  const pageCount = Math.max(1, Math.ceil(total / pageSize));
  const safePage = Math.min(page, pageCount);
  const start = (safePage - 1) * pageSize;
  const pageRows = sorted.slice(start, start + pageSize);

  // actions
  const toggleSort = (col) => {
    if (col === sortBy) {
      setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    } else {
      setSortBy(col);
      setSortDir("asc");
    }
    setPage(1);
  };

  return (
    <div className="table-wrap">
      <table>
        <thead>
          <tr>
            {columns.map((c) => (
              <th key={c} onClick={() => toggleSort(c)}>
                {c.replace(/_/g, " ")}
                {sortBy === c ? (sortDir === "asc" ? " ▲" : " ▼") : ""}
              </th>
            ))}
          </tr>
        </thead>

        <tbody>
          {pageRows.length === 0 ? (
            <tr>
              <td colSpan={columns.length} className="empty">
                No rows
              </td>
            </tr>
          ) : (
            pageRows.map((r, i) => (
              <tr key={start + i}>
                {columns.map((c) => (
                  <td
                    key={c}
                    className={c === "description" ? "description-cell" : ""}
                  >
                    {normalize(r[c])}
                  </td>
                ))}
              </tr>
            ))
          )}
        </tbody>
      </table>
    </div>
  );
}
