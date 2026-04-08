import { useEffect, useState, type ReactNode } from "react";

import {
  executionRecordToDetailJson,
  executionRecordToRows,
} from "../lib/recordsForSpan";

import type { ExecutionRecord } from "../types";

type ExecutionDetailDrawerProps = {
  open: boolean;
  onClose: () => void;
  heading: string;
  subheading?: string | null;
  badge?: string;
  kindHue?: number;
  rows: Array<[string, string]>;
  /** Rendered after the Fields section (e.g. cross-links to node detail). */
  afterFields?: ReactNode;
  rawTitle?: string;
  rawJson?: string;
  /** Semantic log lines sharing this span’s run + node + frame (``records.jsonl``). */
  relatedRecords?: ExecutionRecord[];
  relatedScopeLabel?: string;
};

export function ExecutionDetailDrawer({
  open,
  onClose,
  heading,
  subheading,
  badge,
  kindHue,
  rows,
  afterFields,
  rawTitle = "Raw JSON",
  rawJson,
  relatedRecords,
  relatedScopeLabel,
}: ExecutionDetailDrawerProps) {
  const [expandedRelatedId, setExpandedRelatedId] = useState<string | null>(
    null,
  );

  useEffect(() => {
    if (!open) {
      return;
    }
    const onKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") {
        onClose();
      }
    };
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [open, onClose]);

  useEffect(() => {
    if (!open) {
      setExpandedRelatedId(null);
    }
  }, [open]);

  useEffect(() => {
    setExpandedRelatedId(null);
  }, [relatedRecords, heading]);

  if (!open) {
    return null;
  }

  return (
    <>
      <button
        type="button"
        className="exec-detail-backdrop"
        aria-label="Close details"
        onClick={onClose}
      />
      <aside
        className="exec-detail-panel"
        role="dialog"
        aria-modal="true"
        aria-labelledby="exec-detail-heading"
      >
        <header className="exec-detail-header">
          <div
            className="exec-detail-stripe"
            style={
              kindHue != null
                ? { background: `hsl(${kindHue} 58% 52%)` }
                : undefined
            }
          />
          <div className="exec-detail-head-copy">
            {badge ? (
              <span className="exec-detail-badge mono">{badge}</span>
            ) : null}
            <h2 id="exec-detail-heading" className="exec-detail-title">
              {heading}
            </h2>
            {subheading ? (
              <p className="exec-detail-sub mono">{subheading}</p>
            ) : null}
          </div>
          <button
            type="button"
            className="exec-detail-close"
            onClick={onClose}
            aria-label="Close panel"
          >
            ×
          </button>
        </header>
        <div className="exec-detail-body">
          <section className="exec-detail-section">
            <h3 className="exec-detail-section-title">Fields</h3>
            <dl className="exec-detail-dl">
              {rows.map(([k, v]) => (
                <div key={k} className="exec-detail-row">
                  <dt>{k}</dt>
                  <dd className="mono">{v}</dd>
                </div>
              ))}
            </dl>
          </section>
          {afterFields ?? null}
          {relatedRecords != null ? (
            <section className="exec-detail-section">
              <h3 className="exec-detail-section-title">
                Related semantic records
                {relatedRecords.length > 0 ? (
                  <span className="exec-detail-count"> ({relatedRecords.length})</span>
                ) : null}
              </h3>
              {relatedScopeLabel ? (
                <p className="exec-detail-related-scope mono">{relatedScopeLabel}</p>
              ) : null}
              {relatedRecords.length > 0 ? (
                <ul className="exec-detail-related-list">
                  {relatedRecords.map((rec) => {
                    const isOpen = expandedRelatedId === rec.record_id;
                    const panelId = `related-record-${rec.record_id}`;
                    return (
                      <li key={rec.record_id} className="exec-detail-related-block">
                        <button
                          type="button"
                          className={`exec-detail-related-trigger ${isOpen ? "open" : ""}`}
                          aria-expanded={isOpen}
                          aria-controls={panelId}
                          onClick={() =>
                            setExpandedRelatedId((current) =>
                              current === rec.record_id ? null : rec.record_id,
                            )
                          }
                        >
                          <span className="exec-detail-related-chevron" aria-hidden>
                            ›
                          </span>
                          <span className="exec-detail-related-time mono">
                            {new Date(rec.timestamp_ms).toLocaleTimeString()}
                          </span>
                          <span className="exec-detail-related-event mono">
                            {rec.event_type}
                          </span>
                          <span className="exec-detail-related-seq mono">
                            #{rec.sequence}
                          </span>
                        </button>
                        {isOpen ? (
                          <div
                            className="exec-detail-related-expanded"
                            id={panelId}
                            role="region"
                            aria-label={`Details for ${rec.event_type}`}
                          >
                            <h4 className="exec-detail-related-expanded-title">
                              Fields
                            </h4>
                            <dl className="exec-detail-dl exec-detail-dl-nested">
                              {executionRecordToRows(rec).map(([k, v]) => (
                                <div key={k} className="exec-detail-row">
                                  <dt>{k}</dt>
                                  <dd className="mono">{v}</dd>
                                </div>
                              ))}
                            </dl>
                            <h4 className="exec-detail-related-expanded-title">
                              Full record JSON
                            </h4>
                            <pre className="exec-detail-pre exec-detail-pre-nested">
                              {JSON.stringify(
                                executionRecordToDetailJson(rec),
                                null,
                                2,
                              )}
                            </pre>
                          </div>
                        ) : null}
                      </li>
                    );
                  })}
                </ul>
              ) : (
                <p className="exec-detail-related-empty">
                  No matching lines in the loaded record stream for this run + node + frame.
                  Clear @node_id to load all nodes, or pick a run that includes semantic
                  records for this scope.
                </p>
              )}
            </section>
          ) : null}
          {rawJson ? (
            <section className="exec-detail-section">
              <h3 className="exec-detail-section-title">{rawTitle}</h3>
              <pre className="exec-detail-pre">{rawJson}</pre>
            </section>
          ) : null}
        </div>
      </aside>
    </>
  );
}
