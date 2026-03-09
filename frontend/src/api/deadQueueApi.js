const API_BASE = '/api';

export async function fetchDeadQueue(params = {}) {
  const queryParams = new URLSearchParams();
  
  if (params.collection) queryParams.append('collection', params.collection);
  if (params.field) queryParams.append('field', params.field);
  if (params.search) queryParams.append('search', params.search);
  if (params.startTime) queryParams.append('start_time', params.startTime);
  if (params.endTime) queryParams.append('end_time', params.endTime);
  if (params.limit) queryParams.append('limit', params.limit);
  if (params.offset !== undefined) queryParams.append('offset', params.offset);
  if (params.order) queryParams.append('order', params.order);
  
  const queryString = queryParams.toString();
  const url = `${API_BASE}/dead-queue${queryString ? '?' + queryString : ''}`;
  
  const response = await fetch(url);
  if (!response.ok) throw new Error('Failed to fetch dead queue');
  return response.json();
}

export async function fetchDeadQueueEntry(entryId) {
  const response = await fetch(`${API_BASE}/dead-queue/${entryId}`);
  if (!response.ok) throw new Error('Failed to fetch entry details');
  return response.json();
}

export async function deleteDeadQueueEntry(entryId) {
  const response = await fetch(`${API_BASE}/dead-queue/${entryId}`, {
    method: 'DELETE'
  });
  if (!response.ok) throw new Error('Failed to delete entry');
  return response.json();
}

export async function fetchDeadQueueStats() {
  const response = await fetch(`${API_BASE}/dead-queue/stats`);
  if (!response.ok) throw new Error('Failed to fetch stats');
  return response.json();
}

export async function fetchDeadQueueCollections() {
  const response = await fetch(`${API_BASE}/dead-queue/collections`);
  if (!response.ok) throw new Error('Failed to fetch collections');
  return response.json();
}

export async function fetchDeadQueueFields(collection = null) {
  const url = collection 
    ? `${API_BASE}/dead-queue/fields?collection=${encodeURIComponent(collection)}`
    : `${API_BASE}/dead-queue/fields`;
  
  const response = await fetch(url);
  if (!response.ok) throw new Error('Failed to fetch fields');
  return response.json();
}

export async function cleanupDeadQueue(retentionDays = 30) {
  const response = await fetch(`${API_BASE}/dead-queue/cleanup`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ retention_days: retentionDays })
  });
  if (!response.ok) throw new Error('Failed to cleanup dead queue');
  return response.json();
}

export async function deleteByCollection(collectionName) {
  const response = await fetch(`${API_BASE}/dead-queue/collection/${encodeURIComponent(collectionName)}`, {
    method: 'DELETE'
  });
  if (!response.ok) throw new Error('Failed to delete collection entries');
  return response.json();
}
