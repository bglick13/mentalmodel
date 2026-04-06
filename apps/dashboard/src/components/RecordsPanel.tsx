import type { ExecutionRecord } from "../types";

type RecordsPanelProps = {
  title: string;
  records: ExecutionRecord[];
};

export function RecordsPanel({ title, records }: RecordsPanelProps) {
  return (
    <div className="panel records-panel">
      <div className="panel-header">
        <div>
          <div className="panel-title">{title}</div>
          <div className="panel-subtitle">
            Semantic execution records in sequence order.
          </div>
        </div>
      </div>
      <div className="records-table-wrap">
        <table className="records-table">
          <thead>
            <tr>
              <th>Seq</th>
              <th>Node</th>
              <th>Frame</th>
              <th>Event</th>
              <th>Timestamp</th>
              <th>Payload</th>
            </tr>
          </thead>
          <tbody>
            {records.map((record) => (
              <tr key={record.record_id}>
                <td>{record.sequence}</td>
                <td>{record.node_id}</td>
                <td>{record.frame_id}</td>
                <td>{record.event_type}</td>
                <td>{new Date(record.timestamp_ms).toLocaleTimeString()}</td>
                <td>
                  <pre>{JSON.stringify(record.payload, null, 2)}</pre>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
