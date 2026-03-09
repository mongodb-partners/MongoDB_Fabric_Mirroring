import React, { useState, useEffect, useCallback, useRef } from 'react';
import FilterPanel from './FilterPanel';
import LogTable from './LogTable';
import { fetchLogs, fetchLogStats, fetchCollections, fetchLogLevels, fetchSourceTypes } from '../api/logsApi';

const DEFAULT_FILTERS = {
  level: '',
  collection: '',
  sourceType: 'application',
  search: '',
  startTime: '',
  endTime: '',
};

const PAGE_SIZE = 50;
const TAIL_REFRESH_INTERVAL = 2000;

function LogViewer() {
  const [logs, setLogs] = useState([]);
  const [stats, setStats] = useState(null);
  const [collections, setCollections] = useState([]);
  const [levels, setLevels] = useState([]);
  const [sourceTypes, setSourceTypes] = useState(['application', 'backend']);
  const [filters, setFilters] = useState(DEFAULT_FILTERS);
  const [appliedFilters, setAppliedFilters] = useState(DEFAULT_FILTERS);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [pagination, setPagination] = useState({
    total: 0,
    offset: 0,
    hasMore: false,
  });
  const [autoRefresh, setAutoRefresh] = useState(false);
  const [tailing, setTailing] = useState(false);
  const [lastLogId, setLastLogId] = useState(null);
  const [newLogsCount, setNewLogsCount] = useState(0);
  const refreshIntervalRef = useRef(null);
  const tailIntervalRef = useRef(null);
  const tableContainerRef = useRef(null);

  const loadLogs = useCallback(async (currentFilters, offset = 0, isTailRefresh = false) => {
    try {
      if (!isTailRefresh) {
        setLoading(true);
      }
      setError(null);

      const params = {
        limit: PAGE_SIZE,
        offset,
        order: 'desc',
      };

      if (currentFilters.level) params.level = currentFilters.level;
      if (currentFilters.collection) params.collection = currentFilters.collection;
      if (currentFilters.sourceType) params.sourceType = currentFilters.sourceType;
      if (currentFilters.search) params.search = currentFilters.search;
      if (currentFilters.startTime) {
        params.startTime = new Date(currentFilters.startTime).toISOString();
      }
      if (currentFilters.endTime) {
        params.endTime = new Date(currentFilters.endTime).toISOString();
      }

      const result = await fetchLogs(params);
      
      if (isTailRefresh && result.logs.length > 0) {
        const newestLogId = result.logs[0].id;
        if (lastLogId && newestLogId > lastLogId) {
          const newCount = result.logs.filter(log => log.id > lastLogId).length;
          setNewLogsCount(prev => prev + newCount);
          setTimeout(() => setNewLogsCount(0), 1000);
        }
        setLastLogId(newestLogId);
      } else if (result.logs.length > 0) {
        setLastLogId(result.logs[0].id);
      }
      
      setLogs(result.logs);
      setPagination({
        total: result.total,
        offset: result.offset,
        hasMore: result.has_more,
      });
    } catch (err) {
      setError(err.message);
    } finally {
      if (!isTailRefresh) {
        setLoading(false);
      }
    }
  }, [lastLogId]);

  const loadStats = useCallback(async () => {
    try {
      const statsData = await fetchLogStats();
      setStats(statsData);
    } catch (err) {
      console.error('Failed to load stats:', err);
    }
  }, []);

  const loadCollections = useCallback(async () => {
    try {
      const data = await fetchCollections();
      setCollections(data.collections || []);
    } catch (err) {
      console.error('Failed to load collections:', err);
    }
  }, []);

  const loadLevels = useCallback(async () => {
    try {
      const data = await fetchLogLevels();
      setLevels(data.levels || []);
    } catch (err) {
      console.error('Failed to load levels:', err);
      setLevels(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']);
    }
  }, []);

  const loadSourceTypes = useCallback(async () => {
    try {
      const data = await fetchSourceTypes();
      setSourceTypes(data.source_types || ['application', 'backend']);
    } catch (err) {
      console.error('Failed to load source types:', err);
      setSourceTypes(['application', 'backend']);
    }
  }, []);

  useEffect(() => {
    loadCollections();
    loadLevels();
    loadSourceTypes();
    loadStats();
    loadLogs(appliedFilters, 0);
  }, [loadCollections, loadLevels, loadSourceTypes, loadStats, loadLogs, appliedFilters]);

  useEffect(() => {
    if (autoRefresh && !tailing) {
      refreshIntervalRef.current = setInterval(() => {
        loadLogs(appliedFilters, 0);
        loadStats();
      }, 5000);
    } else {
      if (refreshIntervalRef.current) {
        clearInterval(refreshIntervalRef.current);
        refreshIntervalRef.current = null;
      }
    }

    return () => {
      if (refreshIntervalRef.current) {
        clearInterval(refreshIntervalRef.current);
      }
    };
  }, [autoRefresh, tailing, appliedFilters, loadLogs, loadStats]);

  useEffect(() => {
    if (tailing) {
      loadLogs(appliedFilters, 0, false);
      loadStats();
      
      tailIntervalRef.current = setInterval(() => {
        loadLogs(appliedFilters, 0, true);
        loadStats();
      }, TAIL_REFRESH_INTERVAL);
    } else {
      if (tailIntervalRef.current) {
        clearInterval(tailIntervalRef.current);
        tailIntervalRef.current = null;
      }
    }

    return () => {
      if (tailIntervalRef.current) {
        clearInterval(tailIntervalRef.current);
      }
    };
  }, [tailing, appliedFilters, loadLogs, loadStats]);

  const handleApplyFilters = () => {
    setAppliedFilters({ ...filters });
    loadLogs(filters, 0);
  };

  const handleResetFilters = () => {
    setFilters(DEFAULT_FILTERS);
    setAppliedFilters(DEFAULT_FILTERS);
    loadLogs(DEFAULT_FILTERS, 0);
  };

  const handlePageChange = (newOffset) => {
    loadLogs(appliedFilters, newOffset);
  };

  const toggleAutoRefresh = () => {
    if (tailing) {
      setTailing(false);
    }
    setAutoRefresh(!autoRefresh);
  };

  const toggleTailing = () => {
    if (!tailing) {
      setAutoRefresh(false);
      setPagination(prev => ({ ...prev, offset: 0 }));
    }
    setTailing(!tailing);
    setNewLogsCount(0);
  };

  const currentPage = Math.floor(pagination.offset / PAGE_SIZE) + 1;
  const totalPages = Math.ceil(pagination.total / PAGE_SIZE);

  return (
    <div className="log-viewer">
      {stats && (
        <div className="log-stats">
          <div className="stat-card">
            <div className="stat-value">{stats.total_logs?.toLocaleString() || 0}</div>
            <div className="stat-label">Total Logs</div>
          </div>
          <div className="stat-card">
            <div className="stat-value">{stats.last_hour?.toLocaleString() || 0}</div>
            <div className="stat-label">Last Hour</div>
          </div>
          <div className="stat-card debug">
            <div className="stat-value">{stats.level_counts?.DEBUG || 0}</div>
            <div className="stat-label">Debug</div>
          </div>
          <div className="stat-card info">
            <div className="stat-value">{stats.level_counts?.INFO || 0}</div>
            <div className="stat-label">Info</div>
          </div>
          <div className="stat-card warning">
            <div className="stat-value">{stats.level_counts?.WARNING || 0}</div>
            <div className="stat-label">Warning</div>
          </div>
          <div className="stat-card error">
            <div className="stat-value">{stats.level_counts?.ERROR || 0}</div>
            <div className="stat-label">Error</div>
          </div>
          <div className="stat-card critical">
            <div className="stat-value">{stats.level_counts?.CRITICAL || 0}</div>
            <div className="stat-label">Critical</div>
          </div>
        </div>
      )}

      <FilterPanel
        filters={filters}
        onFilterChange={setFilters}
        onApplyFilters={handleApplyFilters}
        onResetFilters={handleResetFilters}
        collections={collections}
        levels={levels}
        sourceTypes={sourceTypes}
        autoRefresh={autoRefresh}
        onToggleAutoRefresh={toggleAutoRefresh}
        tailing={tailing}
        onToggleTailing={toggleTailing}
      />

      {error && (
        <div className="error-message">
          Error: {error}
        </div>
      )}

      <LogTable 
        logs={logs} 
        loading={loading} 
        tailing={tailing}
        newLogsCount={newLogsCount}
        containerRef={tableContainerRef}
      />

      <div className="pagination">
        <div className="pagination-info">
          {pagination.total > 0 ? (
            <>
              Showing {pagination.offset + 1} - {Math.min(pagination.offset + logs.length, pagination.total)} of {pagination.total.toLocaleString()} logs
              {tailing && <span className="tailing-indicator">● Tailing (live)</span>}
              {autoRefresh && !tailing && <span style={{ marginLeft: '1rem', color: '#0d6efd' }}>● Auto-refreshing</span>}
            </>
          ) : (
            'No logs'
          )}
        </div>
        <div className="pagination-controls">
          <button
            onClick={() => handlePageChange(0)}
            disabled={pagination.offset === 0 || loading || tailing}
          >
            First
          </button>
          <button
            onClick={() => handlePageChange(Math.max(0, pagination.offset - PAGE_SIZE))}
            disabled={pagination.offset === 0 || loading || tailing}
          >
            Previous
          </button>
          <span style={{ padding: '0.4rem 0.8rem', color: '#666' }}>
            Page {currentPage} of {totalPages || 1}
          </span>
          <button
            onClick={() => handlePageChange(pagination.offset + PAGE_SIZE)}
            disabled={!pagination.hasMore || loading || tailing}
          >
            Next
          </button>
          <button
            onClick={() => handlePageChange((totalPages - 1) * PAGE_SIZE)}
            disabled={!pagination.hasMore || loading || tailing}
          >
            Last
          </button>
        </div>
      </div>
    </div>
  );
}

export default LogViewer;
