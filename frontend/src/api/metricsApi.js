const API_BASE = '/api';

export async function fetchDashboard(hours = 24) {
  const response = await fetch(`${API_BASE}/metrics/dashboard?hours=${hours}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch dashboard');
  }
  return response.json();
}

export async function fetchDocumentsMetrics(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  
  const response = await fetch(`${API_BASE}/metrics/documents?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch documents metrics');
  }
  return response.json();
}

export async function fetchDocumentsTimeseries(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  if (params.interval) queryParams.append('interval', params.interval);
  
  const response = await fetch(`${API_BASE}/metrics/documents/timeseries?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch documents timeseries');
  }
  return response.json();
}

export async function fetchConversionsMetrics(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  
  const response = await fetch(`${API_BASE}/metrics/conversions?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch conversions metrics');
  }
  return response.json();
}

export async function fetchConversionsTimeseries(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  if (params.interval) queryParams.append('interval', params.interval);
  
  const response = await fetch(`${API_BASE}/metrics/conversions/timeseries?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch conversions timeseries');
  }
  return response.json();
}

export async function fetchParquetMetrics(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  
  const response = await fetch(`${API_BASE}/metrics/parquet?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch parquet metrics');
  }
  return response.json();
}

export async function fetchParquetFiles(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  if (params.limit) queryParams.append('limit', params.limit);
  if (params.offset !== undefined) queryParams.append('offset', params.offset);
  
  const response = await fetch(`${API_BASE}/metrics/parquet/files?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch parquet files');
  }
  return response.json();
}

export async function fetchParquetTimeseries(params = {}) {
  const queryParams = new URLSearchParams();
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.hours) queryParams.append('hours', params.hours);
  if (params.interval) queryParams.append('interval', params.interval);
  
  const response = await fetch(`${API_BASE}/metrics/parquet/timeseries?${queryParams}`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch parquet timeseries');
  }
  return response.json();
}

export async function fetchMetricsCollections() {
  const response = await fetch(`${API_BASE}/metrics/collections`);
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch collections');
  }
  return response.json();
}
