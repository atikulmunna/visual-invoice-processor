import type { ReactNode } from "react";

import { Icon } from "./Icon";
import { Skeleton } from "./ui";

export interface Column<Row> {
  key: string;
  header: string;
  render: (row: Row) => ReactNode;
  numeric?: boolean;
  /** Set for columns the server can sort by. */
  sortKey?: string;
}

export interface TableSort {
  key: string;
  order: "asc" | "desc";
}

interface DataTableProps<Row> {
  caption: string;
  columns: Column<Row>[];
  rows: Row[];
  rowKey: (row: Row) => string;
  loading?: boolean;
  /** Fresh rows are on their way; the current ones stay in place, dimmed. */
  refreshing?: boolean;
  empty?: ReactNode;
  sort?: TableSort;
  onSort?: (key: string) => void;
}

const SKELETON_ROWS = 4;

function HeaderCell<Row>({ column, sort, onSort }: { column: Column<Row>; sort?: TableSort; onSort?: (key: string) => void }) {
  const className = column.numeric ? "numeric" : undefined;
  if (!column.sortKey || !onSort) {
    return (
      <th scope="col" className={className}>
        {column.header}
      </th>
    );
  }
  const active = sort?.key === column.sortKey;
  const ariaSort = active ? (sort?.order === "asc" ? "ascending" : "descending") : "none";
  return (
    <th scope="col" className={className} aria-sort={ariaSort}>
      <button type="button" className="th-sort" data-active={active || undefined} onClick={() => onSort(column.sortKey!)}>
        {column.header}
        <Icon name={active && sort?.order === "asc" ? "arrowUp" : "arrowDown"} size={13} />
      </button>
    </th>
  );
}

export function DataTable<Row>({
  caption,
  columns,
  rows,
  rowKey,
  loading = false,
  refreshing = false,
  empty,
  sort,
  onSort,
}: DataTableProps<Row>) {
  return (
    // Focusable so keyboard users can scroll a table wider than a phone screen.
    <div className="table-wrap" role="region" aria-label={caption} tabIndex={0}>
      <table className="table" aria-busy={loading || refreshing || undefined} data-refreshing={refreshing || undefined}>
        <caption className="visually-hidden">{caption}</caption>
        <thead>
          <tr>
            {columns.map((column) => (
              <HeaderCell key={column.key} column={column} sort={sort} onSort={onSort} />
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
