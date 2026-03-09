import React, { useEffect, useRef } from 'react';

function formatTimestamp(isoString) {
  if (!isoString) return '';
  const date = new Date(isoString);
  return date.toLocaleString('en-US', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hour12: false,
  });
}

function formatSourceType(sourceType) {
  if (sourceType === 'application') return 'App';
  if (sourceType === 'backend') return 'Sys';
  return sourceType || '-';
}

function LogTable({ logs, loading, tailing, newLogsCount, containerRef }) {
  const tableRef = useRef(null);
  const prevLogsLengthRef = useRef(0);

  useEffect(() => {
    if (tailing && tableRef.current && logs.length > 0) {
      if (prevLogsLengthRef.current !== logs.length || newLogsCount > 0) {
        tableRef.current.scrollTop = 0;
      }
    }
    prevLogsLengthRef.current = logs.length;
  }, [logs, tailing, newLogsCount]);

  if (loading) {
    return (
      <div className="loading">
        Loading logs...
      </div>
    );
  }

  if (!logs || logs.length === 0) {
    return (
      <div className="empty-state">
        <p>No logs found matching the current filters.</p>
      </div>
    );
  }

  return (
    <div 
      className={`log-table-container ${tailing ? 'tailing-mode' : ''}`} 
      ref={(el) => {
        tableRef.current = el;
        if (containerRef) containerRef.current = el;
      }}
    >
      {tailing && newLogsCount > 0 && (
        <div className="new-logs-banner">
          {newLogsCount} new log{newLogsCount > 1 ? 's' : ''} received
        </div>
      )}
      <table className="log-table">
        <thead>
          <tr>
            <th style={{ width: '160px' }}>Timestamp</th>
            <th style={{ width: '50px' }}>Source</th>
            <th style={{ width: '80px' }}>Level</th>
            <th style={{ width: '120px' }}>Collection</th>
            <th>Message</th>
          </tr>
        </thead>
        <tbody>
          {logs.map((log, index) => (
            <tr 
              key={log.id} 
              className={`${tailing && index < newLogsCount ? 'new-log-row' : ''} ${log.source_type === 'backend' ? 'backend-log-row' : ''}`}
            >
              <td className="timestamp-cell">
                {formatTimestamp(log.timestamp)}
              </td>
              <td>
                <span className={`source-badge ${log.source_type || 'application'}`}>
                  {formatSourceType(log.source_type)}
                </span>
              </td>
              <td>
                <span className={`level-badge ${log.level}`}>
                  {log.level}
                </span>
              </td>
              <td className="collection-cell">
                {log.collection_name || '-'}
              </td>
              <td className="message-cell" title={log.message}>
                {log.message}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}

export default LogTable;
