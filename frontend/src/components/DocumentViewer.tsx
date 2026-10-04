import { useState } from "react";

import type { ReviewDocument } from "../lib/review";
import { Icon } from "./Icon";
import { Button, buttonClass, EmptyState } from "./ui";

const ZOOM_STEPS = [0.5, 0.75, 1, 1.25, 1.5, 2, 3];

/** The original document beside the form. PDFs use the browser's own viewer;
 * images get zoom and rotate controls. */
export function DocumentViewer({ document }: { document: ReviewDocument }) {
  const [zoomIndex, setZoomIndex] = useState(2);
  const [rotation, setRotation] = useState(0);
  const name = document.name ?? "Original document";
  const isPdf = document.content_type === "application/pdf";
  const zoom = ZOOM_STEPS[zoomIndex];

  return (
    <section className="viewer" aria-label="Original document">
      <div className="viewer-toolbar">
        <span className="viewer-name" title={name}>
          <Icon name="file" size={16} /> {name}
        </span>
        {document.url && !isPdf && (
          <div className="row" style={{ gap: 4 }}>
            <Button size="sm" variant="ghost" className="btn-icon" icon="minus" aria-label="Zoom out"
              disabled={zoomIndex === 0} onClick={() => setZoomIndex((index) => index - 1)} />
            <span className="viewer-zoom" aria-live="polite">{Math.round(zoom * 100)}%</span>
            <Button size="sm" variant="ghost" className="btn-icon" icon="plus" aria-label="Zoom in"
              disabled={zoomIndex === ZOOM_STEPS.length - 1} onClick={() => setZoomIndex((index) => index + 1)} />
            <Button size="sm" variant="ghost" className="btn-icon" icon="rotate" aria-label="Rotate"
              onClick={() => setRotation((angle) => (angle + 90) % 360)} />
          </div>
        )}
        {document.url && (
          <a className={buttonClass("ghost", "sm")} href={document.url} target="_blank" rel="noopener noreferrer">
            Open in new tab
          </a>
        )}
      </div>
      <div className="viewer-body">
        {!document.url ? (
          <EmptyState icon="file" title="The original file is not available">
            Source files are deleted 30 days after upload. The extracted details are kept.
          </EmptyState>
        ) : isPdf && navigator.pdfViewerEnabled === false ? (
          // Phone browsers such as Chrome on Android cannot show a PDF inside the page.
          <EmptyState
            icon="file"
            title="This browser opens PDFs separately"
            action={
              <a className={buttonClass("primary")} href={document.url} target="_blank" rel="noopener noreferrer">
                Open the PDF
              </a>
            }
          >
            The link works for five minutes.
          </EmptyState>
        ) : isPdf ? (
          <iframe src={document.url} title={`Original document: ${name}`} />
        ) : (
          <div className="viewer-canvas">
            <img
              src={document.url}
              alt={`Original document: ${name}`}
              style={{ transform: `scale(${zoom}) rotate(${rotation}deg)` }}
            />
          </div>
        )}
      </div>
    </section>
  );
}
