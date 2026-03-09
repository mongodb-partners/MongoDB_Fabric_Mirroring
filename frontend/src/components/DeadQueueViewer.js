import React, { useState, useEffect, useCallback } from 'react';
import {
  fetchDeadQueue,
  fetchDeadQueueStats,
  fetchDeadQueueCollections,
  fetchDeadQueueFields,
  fetchDeadQueueEntry,
  deleteDeadQueueEntry,
  cleanupDeadQueue
} from '../api/deadQueueApi';

function DeadQueueViewer() {
  const [entries, setEntries] = useState([]);
  const [stats, setStats] = useState(null);
  const [collections, setCollections] = useState([]);
  const [fields, setFields] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  // Filters
  const [selectedCollection, setSelectedCollection] = useState('');
  const [selectedField, setSelectedField] = useState('');
  const [searchText, setSearchText] = useState('');
  const [searchInput, setSearchInput] = useState('');
  
  // Pagination
  const [offset, setOffset] = useState(0);
  const [total, setTotal] = useState(0);
  const [hasMore, setHasMore] = useState(false);
  const limit = 50;
  
  // Detail modal
  const [selectedEntry, setSelectedEntry] = useState(null);
  const [showModal, setShowModal] = useState(false);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const [queueResult, statsResult, collectionsResult] = await Promise.all([
        fetchDeadQueue({
          collection: selectedCollection || undefined,
          field: selectedField || undefined,
          search: searchText || undefined,
          limit,
          offset,
          order: 'desc'
        }),
        fetchDeadQueueStats(),
        fetchDeadQueueCollections()
      ]);
      
      setEntries(queueResult.entries);
      setTotal(queueResult.total);
      setHasMore(queueResult.has_more);
      setStats(statsResult);
      setCollections(collectionsResult.collections);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [selectedCollection, selectedField, searchText, offset]);

  useEffect(() => {
    loadData();
  }, [loadData]);

  // Load fields when collection changes
  useEffect(() => {
    const loadFields = async () => {
      try {
        const result = await fetchDeadQueueFields(selectedCollection || null);
        setFields(result.fields);
      } catch (err) {
        console.error('Failed to load fields:', err);
      }
    };
    loadFields();
  }, [selectedCollection]);

  const handleSearch = () => {
    setSearchText(searchInput);
    setOffset(0);
  };

  const handleKeyPress = (e) => {
    if (e.key === 'Enter') {
      handleSearch();
    }
  };

  const handleCollectionChange = (e) => {
    setSelectedCollection(e.target.value);
    setSelectedField('');
    setOffset(0);
  };

  const handleFieldChange = (e) => {
    setSelectedField(e.target.value);
    setOffset(0);
  };

  const handleClearFilters = () => {
    setSelectedCollection('');
    setSelectedField('');
    setSearchText('');
    setSearchInput('');
    setOffset(0);
  };

  const handleViewDetails = async (entryId) => {
    try {
      const entry = await fetchDeadQueueEntry(entryId);
      setSelectedEntry(entry);
      setShowModal(true);
    } catch (err) {
      alert('Failed to load entry details: ' + err.message);
    }
  };

  const handleDelete = async (entryId) => {
    if (!window.confirm('Are you sure you want to delete this entry?')) {
      return;
    }
    try {
      await deleteDeadQueueEntry(entryId);
      loadData();
    } catch (err) {
      alert('Failed to delete entry: ' + err.message);
    }
  };

  const handleCleanup = async () => {
    const days = window.prompt('Delete entries older than how many days?', '30');
    if (days === null) return;
    
    const retentionDays = parseInt(days, 10);
    if (isNaN(retentionDays) || retentionDays < 1) {
      alert('Please enter a valid number of days (1 or more)');
      return;
    }
    
    try {
      const result = await cleanupDeadQueue(retentionDays);
      alert(`Deleted ${result.deleted_count} entries older than ${retentionDays} days`);
      loadData();
    } catch (err) {
      alert('Cleanup failed: ' + err.message);
    }
  };

  const formatTimestamp = (ts) => {
    if (!ts) return '-';
    const date = new Date(ts);
    return date.toLocaleString();
  };

  const truncateText = (text, maxLen = 50) => {
    if (!text) return '-';
    return text.length > maxLen ? text.substring(0, maxLen) + '...' : text;
  };

  return (
    <div className="dead-queue-viewer">
      <div className="dead-queue-header">
        <h2>Dead Queue - Failed Conversions</h2>
        {stats && (
          <div className="dead-queue-summary">
            <span className="stat-item">
              <strong>Total:</strong> {stats.total}
            </span>
            <span className="stat-item">
              <strong>Last 24h:</strong> {stats.last_24_hours}
            </span>
          </div>
        )}
      </div>

      <div className="dead-queue-controls">
        <div className="filter-row">
          <div className="filter-group">
            <label>Collection</label>
            <select value={selectedCollection} onChange={handleCollectionChange}>
              <option value="">All Collections</option>
              {collections.map(col => (
                <option key={col} value={col}>{col}</option>
              ))}
            </select>
          </div>
          
          <div className="filter-group">
            <label>Field</label>
            <select value={selectedField} onChange={handleFieldChange}>
              <option value="">All Fields</option>
              {fields.map(field => (
                <option key={field} value={field}>{field}</option>
              ))}
            </select>
          </div>
          
          <div className="filter-group search-group">
            <label>Search</label>
            <div className="search-input-wrapper">
              <input
                type="text"
                placeholder="Search values, errors, IDs..."
                value={searchInput}
                onChange={(e) => setSearchInput(e.target.value)}
                onKeyPress={handleKeyPress}
              />
              <button onClick={handleSearch} className="search-btn">Search</button>
            </div>
          </div>
        </div>
        
        <div className="action-row">
          <button onClick={handleClearFilters} className="btn-secondary">
            Clear Filters
          </button>
          <button onClick={loadData} className="btn-secondary">
            Refresh
          </button>
          <button onClick={handleCleanup} className="btn-warning">
            Cleanup Old Entries
          </button>
        </div>
      </div>

      {error && (
        <div className="error-message">
          Error: {error}
        </div>
      )}

      {loading ? (
        <div className="loading">Loading...</div>
      ) : entries.length === 0 ? (
        <div className="no-data">
          No failed conversions found.
          {(selectedCollection || selectedField || searchText) && (
            <span> Try adjusting your filters.</span>
          )}
        </div>
      ) : (
        <>
          <div className="dead-queue-table-container">
            <table className="dead-queue-table">
              <thead>
                <tr>
                  <th>Timestamp</th>
                  <th>Collection</th>
                  <th>Document ID</th>
                  <th>Field</th>
                  <th>Original Type</th>
                  <th>Target Type</th>
                  <th>Value (truncated)</th>
                  <th>Error</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {entries.map(entry => (
                  <tr key={entry.id}>
                    <td className="timestamp-cell">{formatTimestamp(entry.timestamp)}</td>
                    <td className="collection-cell">{entry.collection_name}</td>
                    <td className="docid-cell">{truncateText(entry.document_id, 24)}</td>
                    <td className="field-cell">{entry.field_name}</td>
                    <td className="type-cell">{entry.original_type}</td>
                    <td className="type-cell">{entry.target_type}</td>
                    <td className="value-cell" title={entry.original_value}>
                      {truncateText(entry.original_value, 30)}
                    </td>
                    <td className="error-cell" title={entry.error_message}>
                      {truncateText(entry.error_message, 40)}
                    </td>
                    <td className="actions-cell">
                      <button 
                        onClick={() => handleViewDetails(entry.id)}
                        className="btn-small btn-info"
                      >
                        View
                      </button>
                      <button 
                        onClick={() => handleDelete(entry.id)}
                        className="btn-small btn-danger"
                      >
                        Delete
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="pagination">
            <span className="pagination-info">
              Showing {offset + 1} - {Math.min(offset + entries.length, total)} of {total}
            </span>
            <div className="pagination-buttons">
              <button 
                onClick={() => setOffset(Math.max(0, offset - limit))}
                disabled={offset === 0}
              >
                Previous
              </button>
              <button 
                onClick={() => setOffset(offset + limit)}
                disabled={!hasMore}
              >
                Next
              </button>
            </div>
          </div>
        </>
      )}

      {showModal && selectedEntry && (
        <div className="modal-overlay" onClick={() => setShowModal(false)}>
          <div className="modal-content" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Entry Details</h3>
              <button className="modal-close" onClick={() => setShowModal(false)}>×</button>
            </div>
            <div className="modal-body">
              <div className="detail-row">
                <label>ID:</label>
                <span>{selectedEntry.id}</span>
              </div>
              <div className="detail-row">
                <label>Timestamp:</label>
                <span>{formatTimestamp(selectedEntry.timestamp)}</span>
              </div>
              <div className="detail-row">
                <label>Collection:</label>
                <span>{selectedEntry.collection_name}</span>
              </div>
              <div className="detail-row">
                <label>Document ID:</label>
                <span className="monospace">{selectedEntry.document_id || '-'}</span>
              </div>
              <div className="detail-row">
                <label>Field:</label>
                <span>{selectedEntry.field_name}</span>
              </div>
              <div className="detail-row">
                <label>Original Type:</label>
                <span>{selectedEntry.original_type}</span>
              </div>
              <div className="detail-row">
                <label>Target Type:</label>
                <span>{selectedEntry.target_type}</span>
              </div>
              <div className="detail-row full-width">
                <label>Original Value:</label>
                <pre className="value-display">{selectedEntry.original_value}</pre>
              </div>
              <div className="detail-row full-width">
                <label>Error Message:</label>
                <pre className="error-display">{selectedEntry.error_message}</pre>
              </div>
              {selectedEntry.document_json && (
                <div className="detail-row full-width">
                  <label>Document JSON:</label>
                  <pre className="json-display">{selectedEntry.document_json}</pre>
                </div>
              )}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

export default DeadQueueViewer;
