import React, { useState, useEffect, useCallback } from 'react';
import { fetchParquetFiles, fetchMetricsCollections } from '../api/metricsApi';

function ReportsPage() {
  const [parquetFiles, setParquetFiles] = useState([]);
  const [collections, setCollections] = useState([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  
  const [filters, setFilters] = useState({
    collection: '',
    hours: 168,
    limit: 50
  });
  
  const [pagination, setPagination] = useState({
    offset: 0,
    total: 0,
    hasMore: false
  });

  const formatBytes = (bytes) => {
    if (!bytes || bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const formatNumber = (num) => {
    if (!num) return '0';
    return num.toLocaleString();
  };

  const loadCollections = useCallback(async () => {
    try {
      const data = await fetchMetricsCollections();
      setCollections(data.collections || []);
    } catch (err) {
      console.error('Failed to load collections:', err);
    }
  }, []);

  const loadParquetFiles = useCallback(async (resetOffset = false) => {
    setLoading(true);
    setError(null);
    
    const currentOffset = resetOffset ? 0 : pagination.offset;
    
    try {
      const data = await fetchParquetFiles({
        collection: filters.collection || undefined,
        hours: filters.hours,
        limit: filters.limit,
        offset: currentOffset
      });
      
      setParquetFiles(data.files || []);
      setPagination({
        offset: data.offset || 0,
        total: data.total || 0,
        hasMore: data.has_more || false
      });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [filters, pagination.offset]);

  useEffect(() => {
    loadCollections();
  }, [loadCollections]);

  useEffect(() => {
    loadParquetFiles(true);
  }, [filters]);

  const handleFilterChange = (key, value) => {
    setFilters(prev => ({ ...prev, [key]: value }));
  };

  const handlePageChange = (newOffset) => {
    setPagination(prev => ({ ...prev, offset: newOffset }));
    loadParquetFilesWithOffset(newOffset);
  };

  const loadParquetFilesWithOffset = async (offset) => {
    setLoading(true);
    setError(null);
    
    try {
      const data = await fetchParquetFiles({
        collection: filters.collection || undefined,
        hours: filters.hours,
        limit: filters.limit,
        offset: offset
      });
      
      setParquetFiles(data.files || []);
      setPagination({
        offset: data.offset || 0,
        total: data.total || 0,
        hasMore: data.has_more || false
      });
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  };

  const currentPage = Math.floor(pagination.offset / filters.limit) + 1;
  const totalPages = Math.ceil(pagination.total / filters.limit);

  const goToPage = (page) => {
    const newOffset = (page - 1) * filters.limit;
    handlePageChange(newOffset);
  };

  const renderPagination = () => {
    if (totalPages <= 1) return null;

    const pages = [];
    const maxVisiblePages = 7;
    
    let startPage = Math.max(1, currentPage - Math.floor(maxVisiblePages / 2));
    let endPage = Math.min(totalPages, startPage + maxVisiblePages - 1);
    
    if (endPage - startPage + 1 < maxVisiblePages) {
      startPage = Math.max(1, endPage - maxVisiblePages + 1);
    }

    if (startPage > 1) {
      pages.push(
        <button key={1} onClick={() => goToPage(1)} className="page-btn">
          1
        </button>
      );
      if (startPage > 2) {
        pages.push(<span key="ellipsis1" className="ellipsis">...</span>);
      }
    }

    for (let i = startPage; i <= endPage; i++) {
      pages.push(
        <button
          key={i}
          onClick={() => goToPage(i)}
          className={`page-btn ${i === currentPage ? 'active' : ''}`}
        >
          {i}
        </button>
      );
    }

    if (endPage < totalPages) {
      if (endPage < totalPages - 1) {
        pages.push(<span key="ellipsis2" className="ellipsis">...</span>);
      }
      pages.push(
        <button key={totalPages} onClick={() => goToPage(totalPages)} className="page-btn">
          {totalPages}
        </button>
      );
    }

    return (
      <div className="pagination">
        <button
          onClick={() => goToPage(currentPage - 1)}
          disabled={currentPage === 1}
          className="page-btn nav-arrow"
        >
          &laquo; Prev
        </button>
        {pages}
        <button
          onClick={() => goToPage(currentPage + 1)}
          disabled={currentPage === totalPages}
          className="page-btn nav-arrow"
        >
          Next &raquo;
        </button>
      </div>
    );
  };

  return (
    <div className="reports-page">
      <div className="reports-header">
        <h2>Parquet Files Report</h2>
      </div>

      <div className="reports-controls">
        <div className="filter-group">
          <label>Collection:</label>
          <select
            value={filters.collection}
            onChange={(e) => handleFilterChange('collection', e.target.value)}
          >
            <option value="">All Collections</option>
            {collections.map(col => (
              <option key={col} value={col}>{col}</option>
            ))}
          </select>
        </div>

        <div className="filter-group">
          <label>Time Range:</label>
          <select
            value={filters.hours}
            onChange={(e) => handleFilterChange('hours', parseInt(e.target.value))}
          >
            <option value={24}>Last 24 hours</option>
            <option value={48}>Last 48 hours</option>
            <option value={168}>Last 7 days</option>
            <option value={720}>Last 30 days</option>
            <option value={2160}>Last 90 days</option>
          </select>
        </div>

        <div className="filter-group">
          <label>Per Page:</label>
          <select
            value={filters.limit}
            onChange={(e) => handleFilterChange('limit', parseInt(e.target.value))}
          >
            <option value={25}>25</option>
            <option value={50}>50</option>
            <option value={100}>100</option>
            <option value={200}>200</option>
          </select>
        </div>

        <button 
          className="refresh-btn" 
          onClick={() => loadParquetFiles(true)}
          disabled={loading}
        >
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      {error && (
        <div className="error-message">
          Error: {error}
        </div>
      )}

      <div className="reports-summary">
        <span>
          Showing {pagination.offset + 1} - {Math.min(pagination.offset + parquetFiles.length, pagination.total)} of {formatNumber(pagination.total)} files
        </span>
      </div>

      <div className="parquet-files-table">
        <table>
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Collection</th>
              <th>File Name</th>
              <th>Size</th>
              <th>Rows</th>
              <th>Type</th>
            </tr>
          </thead>
          <tbody>
            {parquetFiles.map((file, idx) => (
              <tr key={idx}>
                <td>{new Date(file.timestamp).toLocaleString()}</td>
                <td>{file.collection_name}</td>
                <td className="filename">{file.file_name}</td>
                <td>{formatBytes(file.file_size_bytes)}</td>
                <td>{formatNumber(file.row_count)}</td>
                <td className={`sync-type ${file.sync_type}`}>{file.sync_type}</td>
              </tr>
            ))}
            {parquetFiles.length === 0 && !loading && (
              <tr>
                <td colSpan="6" className="no-data">
                  No parquet files found for the selected filters
                </td>
              </tr>
            )}
            {loading && (
              <tr>
                <td colSpan="6" className="loading-row">
                  Loading...
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>

      {renderPagination()}
    </div>
  );
}

export default ReportsPage;
