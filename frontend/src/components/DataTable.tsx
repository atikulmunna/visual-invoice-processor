import type { ReactNode } from "react";

import { Skeleton } from "./ui";

export interface Column<Row> {
  key: string;
  header: string;
  render: (row: Row) => ReactNode;
  numeric?: boolean;
}

interface DataTableProps<Row> {
  caption: string;
  columns: Column<Row>[];
  rows: Row[];
  rowKey: (row: Row) => string;
  loading?: boolean;
  empty?: ReactNode;
}

const SKELETON_ROWS = 4;

export function DataTable<Row>({ caption, columns, rows, rowKey, loading = false, empty }: DataTableProps<Row>) {
  return (
    // Focusable so keyboard users can scroll a table wider than a phone screen.
    <div className="table-wrap" role="region" aria-label={caption} tabIndex={0}>
      <table className="table" aria-busy={loading || undefined}>
        <caption className="visually-hidden">{caption}</caption>
        <thead>
          <tr>
            {columns.map((column) => (
              <th key={column.key} scope="col" className={column.numeric ? "numeric" : undefined}>
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {loading &&
            Array.from({ length: SKELETON_ROWS }, (_, index) => (
              <tr key={`skeleton-${index}`}>
                {columns.map((column) => (
                  <td key={column.key}>
                    <Skeleton width={column.numeric ? 80 : "70%"} />
                  </td>
                ))}
              </tr>
            ))}
          {!loading &&
            rows.map((row) => (
              <tr key={rowKey(row)}>
                {columns.map((column) => (
                  <td key={column.key} className={column.numeric ? "numeric" : undefined}>
                    {column.render(row)}
                  </td>
                ))}
              </tr>
            ))}
          {!loading && rows.length === 0 && empty && (
            <tr>
              <td colSpan={columns.length}>{empty}</td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
