import { pageWindow } from "../lib/records";
import { Icon } from "./Icon";

interface PaginationProps {
  page: number;
  count: number;
  onChange: (page: number) => void;
}

export function Pagination({ page, count, onChange }: PaginationProps) {
  if (count <= 1) {
    return null;
  }
  return (
    <nav className="pagination" aria-label="Pages">
      <button
        type="button"
        className="page-step"
        aria-label="Previous page"
        disabled={page <= 1}
        onClick={() => onChange(page - 1)}
      >
        <Icon name="arrowLeft" size={16} />
      </button>
      {pageWindow(page, count).map((item, index) =>
        item === "gap" ? (
          <span key={`gap-${index}`} className="page-gap" aria-hidden="true">
            ...
          </span>
        ) : (
          <button
            key={item}
            type="button"
            className="page-number"
            aria-current={item === page ? "page" : undefined}
            aria-label={`Page ${item}`}
            onClick={() => onChange(item)}
          >
            {item}
          </button>
        ),
      )}
      <button
        type="button"
        className="page-step"
        aria-label="Next page"
        disabled={page >= count}
        onClick={() => onChange(page + 1)}
      >
        <Icon name="arrowRight" size={16} />
      </button>
    </nav>
  );
}
