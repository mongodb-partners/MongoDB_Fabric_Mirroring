const API_BASE = '/api';

export async function fetchLogs(params = {}) {
  const queryParams = new URLSearchParams();
  
  if (params.level) queryParams.append('level', params.level);
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.sourceType) queryParams.append('source_type', params.sourceType);
  if (params.search) queryParams.append('search', params.search);
  if (params.startTime) queryParams.append('start_time', params.startTime);
  if (params.endTime) queryParams.append('end_time', params.endTime);
  if (params.limit) queryParams.append('limit', params.limit);
  if (params.offset !== undefined) queryParams.append('offset', params.offset);
  if (params.order) queryParams.append('order', params.order);
  
  const url = `${API_BASE}/logs?${queryParams.toString()}`;
  const response = await fetch(url);
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch logs');
  }
  
  return response.json();
}

export async function fetchLogStats() {
  const response = await fetch(`${API_BASE}/logs/stats`);
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch log stats');
  }
  
  return response.json();
}

export async function fetchCollections() {
  const response = await fetch(`${API_BASE}/logs/collections`);
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch collections');
  }
  
  return response.json();
}

export async function fetchLogLevels() {
  const response = await fetch(`${API_BASE}/logs/levels`);
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch log levels');
  }
  
  return response.json();
}

export async function fetchSourceTypes() {
  const response = await fetch(`${API_BASE}/logs/source-types`);
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to fetch source types');
  }
  
  return response.json();
}

export async function cleanupLogs(retentionDays = 30) {
  const response = await fetch(`${API_BASE}/logs/cleanup`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({ retention_days: retentionDays }),
  });
  
  if (!response.ok) {
    const error = await response.json();
    throw new Error(error.error || 'Failed to cleanup logs');
  }
  
  return response.json();
}
