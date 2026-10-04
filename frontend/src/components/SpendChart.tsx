import { useLayoutEffect, useRef, useState, type KeyboardEvent, type PointerEvent, type RefObject } from "react";

import { formatMoney } from "../lib/format";
import { axisTicks, compactAmount, monthLabel, plural, type MonthTotal } from "../lib/overview";
import { Button } from "./ui";

const HEIGHT = 240;
const MARGIN = { top: 28, right: 8, bottom: 30, left: 52 };
const MAX_BAR_WIDTH = 24;
const CORNER = 4;
// Below this band width every other month label is skipped so labels never collide.
const MIN_LABEL_BAND = 36;
const TOOLTIP_HALF_WIDTH = 90;

function useElementWidth(ref: RefObject<HTMLElement | null>): number {
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const element = ref.current;
    if (!element) {
      return;
    }
    setWidth(element.clientWidth);
    const observer = new ResizeObserver(([entry]) => setWidth(entry.contentRect.width));
    observer.observe(element);
    return () => observer.disconnect();
  }, [ref]);
  return width;
}

/** A column rising from the baseline: rounded at the data end, square at the base. */
function columnPath(x: number, top: number, width: number, base: number): string {
  const radius = Math.min(CORNER, base - top, width / 2);
  return [
    `M${x},${base}`,
    `V${top + radius}`,
    `Q${x},${top} ${x + radius},${top}`,
    `H${x + width - radius}`,
    `Q${x + width},${top} ${x + width},${top + radius}`,
    `V${base}`,
    "Z",
  ].join(" ");
}

function describe(month: MonthTotal, currency: string | null): string {
  return `${monthLabel(month.month, true)}: ${formatMoney(month.total, currency)}, ${plural(month.count, "record")}`;
}

function MonthTable({ months, currency }: { months: MonthTotal[]; currency: string | null }) {
  return (
    <div className="table-wrap" role="region" aria-label="Spending by month" tabIndex={0}>
      <table className="table table-compact">
        <thead>
          <tr>
            <th scope="col">Month</th>
            <th scope="col" className="numeric">Records</th>
            <th scope="col" className="numeric">Total</th>
          </tr>
        </thead>
        <tbody>
          {months.map((month) => (
            <tr key={month.month}>
              <td>{monthLabel(month.month, true)}</td>
              <td className="numeric">{month.count}</td>
              <td className="numeric">{formatMoney(month.total, currency)}</td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

/** Spending per invoice month in one currency. Hover or use the arrow keys to read a month. */
export function SpendChart({ months, currency }: { months: MonthTotal[]; currency: string | null }) {
  const frameRef = useRef<HTMLDivElement>(null);
  const width = useElementWidth(frameRef);
  const [active, setActive] = useState<number | null>(null);
  const [asTable, setAsTable] = useState(false);

  const ticks = axisTicks(Math.max(0, ...months.map((month) => month.total)));
  const top = ticks.at(-1) || 1;
  const plotWidth = Math.max(width - MARGIN.left - MARGIN.right, 0);
  const plotHeight = HEIGHT - MARGIN.top - MARGIN.bottom;
  const base = MARGIN.top + plotHeight;
  const band = months.length ? plotWidth / months.length : 0;
  const barWidth = Math.min(MAX_BAR_WIDTH, band * 0.6);
  const y = (value: number) => base - (value / top) * plotHeight;
  const center = (index: number) => MARGIN.left + band * index + band / 2;
  const peak = months.reduce((best, month, index) => (month.total > (months[best]?.total ?? 0) ? index : best), 0);

  function pointAt(event: PointerEvent<SVGSVGElement>) {
    const x = event.clientX - event.currentTarget.getBoundingClientRect().left - MARGIN.left;
    setActive(band > 0 ? Math.min(months.length - 1, Math.max(0, Math.floor(x / band))) : null);
  }

  function step(event: KeyboardEvent<HTMLDivElement>) {
    const moves: Record<string, number> = { ArrowLeft: -1, ArrowRight: 1 };
    const last = months.length - 1;
    let next: number | null = null;
    if (event.key in moves) next = Math.min(last, Math.max(0, (active ?? last) + moves[event.key]));
    if (event.key === "Home") next = 0;
    if (event.key === "End") next = last;
    if (next !== null) {
      event.preventDefault();
      setActive(next);
    }
  }

  const activeMonth = active !== null ? months[active] : null;
  // Keeps the tooltip inside the card at either end.
  const tooltipLeft = (index: number) => Math.min(Math.max(center(index), TOOLTIP_HALF_WIDTH), width - TOOLTIP_HALF_WIDTH);

  return (
    <div className="chart" ref={frameRef}>
      <div className="chart-tools">
        <Button variant="ghost" size="sm" onClick={() => setAsTable((value) => !value)} aria-pressed={asTable}>
          {asTable ? "Show chart" : "Show as table"}
        </Button>
      </div>
      {asTable ? (
        <MonthTable months={months} currency={currency} />
      ) : (
        <div
          className="chart-frame"
          tabIndex={0}
          role="group"
          aria-label={`Spending by month in ${currency ?? "your currency"}. Use the left and right arrow keys to read each month.`}
          onKeyDown={step}
          onFocus={() => setActive((value) => value ?? months.length - 1)}
          onBlur={() => setActive(null)}
        >
          <svg width={width} height={HEIGHT} aria-hidden="true" onPointerMove={pointAt} onPointerLeave={() => setActive(null)}>
            {ticks.map((tick) => (
              <g key={tick}>
                <line className="chart-grid" x1={MARGIN.left} x2={width - MARGIN.right} y1={y(tick)} y2={y(tick)} />
                <text className="chart-tick" x={MARGIN.left - 8} y={y(tick)} dy="0.32em" textAnchor="end">
                  {compactAmount(tick, currency)}
                </text>
              </g>
            ))}
            {months.map((month, index) => (
              <g key={month.month} className="chart-column" data-active={active === index || undefined}>
                {month.total > 0 && (
                  <path className="chart-bar" d={columnPath(center(index) - barWidth / 2, y(month.total), barWidth, base)} />
                )}
                {(band >= MIN_LABEL_BAND || (months.length - 1 - index) % 2 === 0) && (
                  <text className="chart-tick" x={center(index)} y={base + 18} textAnchor="middle">
                    {monthLabel(month.month)}
                  </text>
                )}
              </g>
            ))}
            {months[peak]?.total > 0 && active === null && (
              <text className="chart-value" x={center(peak)} y={y(months[peak].total) - 8} textAnchor="middle">
                {compactAmount(months[peak].total, currency)}
              </text>
            )}
          </svg>
          {activeMonth && (
            <div className="chart-tooltip" style={{ left: tooltipLeft(active!), top: y(activeMonth.total) - 10 }}>
              <strong>{formatMoney(activeMonth.total, currency)}</strong>
              <span>
                {monthLabel(activeMonth.month, true)}, {plural(activeMonth.count, "record")}
              </span>
            </div>
          )}
          <span className="visually-hidden" aria-live="polite">
            {activeMonth ? describe(activeMonth, currency) : ""}
          </span>
        </div>
      )}
    </div>
  );
}
