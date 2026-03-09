import React from 'react';

const SOURCE_TYPE_LABELS = {
  'application': 'Application',
  'backend': 'Backend/Framework',
  '': 'All Sources',
};

function FilterPanel({
  filters,
  onFilterChange,
  onApplyFilters,
  onResetFilters,
  collections,
  levels,
  sourceTypes,
  autoRefresh,
  onToggleAutoRefresh,
  tailing,
  onToggleTailing,
}) {
  const handleChange = (field, value) => {
    onFilterChange({ ...filters, [field]: value });
  };

  return (
    <div className="filter-panel">
      <div className="filter-group">
        <label>Source</label>
        <select
          value={filters.sourceType || ''}
          onChange={(e) => handleChange('sourceType', e.target.value)}
          className="source-type-select"
        >
          <option value="">All Sources</option>
          {sourceTypes.map((type) => (
            <option key={type} value={type}>
              {SOURCE_TYPE_LABELS[type] || type}
            </option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <label>Level</label>
        <select
          value={filters.level || ''}
          onChange={(e) => handleChange('level', e.target.value)}
        >
          <option value="">All Levels</option>
          {levels.map((level) => (
            <option key={level} value={level}>
              {level}
            </option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <label>Collection</label>
        <select
          value={filters.collection || ''}
          onChange={(e) => handleChange('collection', e.target.value)}
        >
          <option value="">All Collections</option>
          {collections.map((coll) => (
            <option key={coll} value={coll}>
              {coll}
            </option>
          ))}
        </select>
      </div>

      <div className="filter-group">
        <label>Search</label>
        <input
          type="text"
          placeholder="Search in messages..."
          value={filters.search || ''}
          onChange={(e) => handleChange('search', e.target.value)}
          onKeyPress={(e) => e.key === 'Enter' && onApplyFilters()}
        />
      </div>

      <div className="filter-group">
        <label>Start Time</label>
        <input
          type="datetime-local"
          value={filters.startTime || ''}
          onChange={(e) => handleChange('startTime', e.target.value)}
        />
      </div>

      <div className="filter-group">
        <label>End Time</label>
        <input
          type="datetime-local"
          value={filters.endTime || ''}
          onChange={(e) => handleChange('endTime', e.target.value)}
        />
      </div>

      <div className="filter-actions">
        <button
          className={`btn btn-icon btn-tail ${tailing ? 'active' : ''}`}
          onClick={onToggleTailing}
          title={tailing ? 'Stop tailing' : 'Start tailing (show latest logs live)'}
        >
          {tailing ? '⏹' : '↓'}
        </button>
        <button
          className={`btn btn-icon ${autoRefresh ? 'active' : ''}`}
          onClick={onToggleAutoRefresh}
          title={autoRefresh ? 'Disable auto-refresh' : 'Enable auto-refresh (5s)'}
          disabled={tailing}
        >
          {autoRefresh ? '⏸' : '▶'}
        </button>
        <button className="btn btn-secondary" onClick={onResetFilters}>
          Reset
        </button>
        <button className="btn btn-primary" onClick={onApplyFilters}>
          Apply
        </button>
      </div>
    </div>
  );
}

export default FilterPanel;
