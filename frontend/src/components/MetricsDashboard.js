import React, { useState, useEffect, useCallback } from 'react';
import { Line, Bar, Doughnut, Chart } from 'react-chartjs-2';
import {
  Chart as ChartJS,
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  BarController,
  LineController
} from 'chart.js';
import {
  fetchDashboard,
  fetchDocumentsTimeseries,
  fetchConversionsTimeseries,
  fetchParquetTimeseries,
  fetchMetricsCollections
} from '../api/metricsApi';

ChartJS.register(
  CategoryScale,
  LinearScale,
  PointElement,
  LineElement,
  BarElement,
  ArcElement,
  Title,
  Tooltip,
  Legend,
  Filler,
  BarController,
  LineController
);

const TIME_RANGES = [
  { value: 1, label: '1 Hour' },
  { value: 6, label: '6 Hours' },
  { value: 24, label: '24 Hours' },
  { value: 72, label: '3 Days' },
  { value: 168, label: '7 Days' }
];

function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function formatNumber(num) {
  if (num >= 1000000) return (num / 1000000).toFixed(1) + 'M';
  if (num >= 1000) return (num / 1000).toFixed(1) + 'K';
  return num.toString();
}

function MetricsDashboard() {
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [timeRange, setTimeRange] = useState(24);
  const [collection, setCollection] = useState('');
  const [collections, setCollections] = useState([]);
  const [dashboard, setDashboard] = useState(null);
  const [documentsTimeseries, setDocumentsTimeseries] = useState([]);
  const [conversionsTimeseries, setConversionsTimeseries] = useState([]);
  const [parquetTimeseries, setParquetTimeseries] = useState([]);

  const loadData = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const params = { hours: timeRange, interval: 15 };
      if (collection) params.collection = collection;

      const [dashboardData, docsTs, convTs, parqTs, colls] = await Promise.all([
        fetchDashboard(timeRange),
        fetchDocumentsTimeseries(params),
        fetchConversionsTimeseries(params),
        fetchParquetTimeseries(params),
        fetchMetricsCollections()
      ]);

      setDashboard(dashboardData);
      setDocumentsTimeseries(docsTs.data || []);
      setConversionsTimeseries(convTs.data || []);
      setParquetTimeseries(parqTs.data || []);
      setCollections(colls.collections || []);
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [timeRange, collection]);

  useEffect(() => {
    loadData();
    const interval = setInterval(loadData, 60000);
    return () => clearInterval(interval);
  }, [loadData]);

  // Get unique time buckets for documents chart
  const docTimeBuckets = [...new Set(documentsTimeseries.map(d => d.time_bucket))].sort();
  
  const documentsChartData = {
    labels: docTimeBuckets.map(bucket => {
      const date = new Date(bucket);
      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }),
    datasets: [
      {
        type: 'bar',
        label: 'Init Sync',
        data: docTimeBuckets.map(bucket => {
          const match = documentsTimeseries.find(d => d.time_bucket === bucket && d.sync_type === 'init');
          return match ? match.document_count : 0;
        }),
        borderColor: 'rgb(54, 162, 235)',
        backgroundColor: 'rgba(54, 162, 235, 0.7)',
        yAxisID: 'y',
        order: 2
      },
      {
        type: 'line',
        label: 'Delta Sync',
        data: docTimeBuckets.map(bucket => {
          const match = documentsTimeseries.find(d => d.time_bucket === bucket && d.sync_type === 'delta');
          return match ? match.document_count : 0;
        }),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        fill: true,
        tension: 0.3,
        yAxisID: 'y1',
        order: 1
      }
    ]
  };

  // Get unique time buckets for conversions chart
  const convTimeBuckets = [...new Set(conversionsTimeseries.map(d => d.time_bucket))].sort();
  
  const conversionsChartData = {
    labels: convTimeBuckets.map(bucket => {
      const date = new Date(bucket);
      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }),
    datasets: [
      {
        type: 'bar',
        label: 'Init Sync (Successful)',
        data: convTimeBuckets.map(bucket => {
          const match = conversionsTimeseries.find(d => d.time_bucket === bucket && d.sync_type === 'init');
          return match ? match.successful : 0;
        }),
        borderColor: 'rgb(54, 162, 235)',
        backgroundColor: 'rgba(54, 162, 235, 0.7)',
        stack: 'init',
        yAxisID: 'y',
        order: 2
      },
      {
        type: 'bar',
        label: 'Init Sync (Failed)',
        data: convTimeBuckets.map(bucket => {
          const match = conversionsTimeseries.find(d => d.time_bucket === bucket && d.sync_type === 'init');
          return match ? match.failed : 0;
        }),
        borderColor: 'rgb(255, 99, 132)',
        backgroundColor: 'rgba(255, 99, 132, 0.7)',
        stack: 'init',
        yAxisID: 'y',
        order: 2
      },
      {
        type: 'line',
        label: 'Delta Sync (Successful)',
        data: convTimeBuckets.map(bucket => {
          const match = conversionsTimeseries.find(d => d.time_bucket === bucket && d.sync_type === 'delta');
          return match ? match.successful : 0;
        }),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.2)',
        fill: true,
        tension: 0.3,
        yAxisID: 'y1',
        order: 1
      },
      {
        type: 'line',
        label: 'Delta Sync (Failed)',
        data: convTimeBuckets.map(bucket => {
          const match = conversionsTimeseries.find(d => d.time_bucket === bucket && d.sync_type === 'delta');
          return match ? match.failed : 0;
        }),
        borderColor: 'rgb(220, 53, 69)',
        backgroundColor: 'rgba(220, 53, 69, 0.2)',
        fill: true,
        tension: 0.3,
        yAxisID: 'y1',
        order: 1
      }
    ]
  };

  const conversionsPieData = dashboard ? {
    labels: ['Successful', 'Failed'],
    datasets: [{
      data: [dashboard.conversions.successful, dashboard.conversions.failed],
      backgroundColor: ['rgba(75, 192, 192, 0.8)', 'rgba(255, 99, 132, 0.8)'],
      borderColor: ['rgb(75, 192, 192)', 'rgb(255, 99, 132)'],
      borderWidth: 1
    }]
  } : null;

  const parquetChartData = {
    labels: parquetTimeseries.map(d => {
      const date = new Date(d.time_bucket);
      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }),
    datasets: [
      {
        type: 'bar',
        label: 'Files Uploaded',
        data: parquetTimeseries.map(d => d.file_count),
        borderColor: 'rgb(153, 102, 255)',
        backgroundColor: 'rgba(153, 102, 255, 0.7)',
        yAxisID: 'y',
        order: 2
      },
      {
        type: 'line',
        label: 'Total Rows',
        data: parquetTimeseries.map(d => d.total_rows),
        borderColor: 'rgb(255, 159, 64)',
        backgroundColor: 'rgba(255, 159, 64, 0.2)',
        fill: true,
        tension: 0.3,
        yAxisID: 'y1',
        order: 1
      }
    ]
  };

  // Combined data flow chart: MongoDB documents vs Azure rows
  const allTimeBuckets = [...new Set([
    ...documentsTimeseries.map(d => d.time_bucket),
    ...parquetTimeseries.map(d => d.time_bucket)
  ])].sort();

  const dataFlowChartData = {
    labels: allTimeBuckets.map(bucket => {
      const date = new Date(bucket);
      return date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    }),
    datasets: [
      {
        label: 'Documents from MongoDB',
        data: allTimeBuckets.map(bucket => {
          const matches = documentsTimeseries.filter(d => d.time_bucket === bucket);
          return matches.reduce((sum, d) => sum + (d.document_count || 0), 0);
        }),
        borderColor: 'rgb(75, 192, 192)',
        backgroundColor: 'rgba(75, 192, 192, 0.3)',
        fill: true,
        tension: 0.3
      },
      {
        label: 'Rows to Azure (Parquet)',
        data: allTimeBuckets.map(bucket => {
          const match = parquetTimeseries.find(d => d.time_bucket === bucket);
          return match ? match.total_rows : 0;
        }),
        borderColor: 'rgb(255, 159, 64)',
        backgroundColor: 'rgba(255, 159, 64, 0.3)',
        fill: true,
        tension: 0.3
      }
    ]
  };

  const chartOptions = {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: { position: 'top' }
    }
  };

  const documentsDualAxisOptions = {
    ...chartOptions,
    scales: {
      y: { type: 'linear', position: 'left', title: { display: true, text: 'Init Sync' } },
      y1: { type: 'linear', position: 'right', title: { display: true, text: 'Delta Sync' }, grid: { drawOnChartArea: false } }
    }
  };

  const conversionsDualAxisOptions = {
    ...chartOptions,
    scales: {
      y: { type: 'linear', position: 'left', title: { display: true, text: 'Init Sync' }, stacked: true },
      y1: { type: 'linear', position: 'right', title: { display: true, text: 'Delta Sync' }, grid: { drawOnChartArea: false } }
    }
  };

  const parquetDualAxisOptions = {
    ...chartOptions,
    scales: {
      y: { type: 'linear', position: 'left', title: { display: true, text: 'Files' } },
      y1: { type: 'linear', position: 'right', title: { display: true, text: 'Rows' }, grid: { drawOnChartArea: false } }
    }
  };

  if (loading && !dashboard) {
    return <div className="metrics-loading">Loading metrics...</div>;
  }

  if (error) {
    return <div className="metrics-error">Error: {error}</div>;
  }

  return (
    <div className="metrics-dashboard">
      <div className="metrics-controls">
        <div className="control-group">
          <label>Time Range:</label>
          <select value={timeRange} onChange={(e) => setTimeRange(Number(e.target.value))}>
            {TIME_RANGES.map(tr => (
              <option key={tr.value} value={tr.value}>{tr.label}</option>
            ))}
          </select>
        </div>
        <div className="control-group">
          <label>Collection:</label>
          <select value={collection} onChange={(e) => setCollection(e.target.value)}>
            <option value="">All Collections</option>
            {collections.map(c => (
              <option key={c} value={c}>{c}</option>
            ))}
          </select>
        </div>
        <button className="btn-refresh" onClick={loadData} disabled={loading}>
          {loading ? 'Loading...' : 'Refresh'}
        </button>
      </div>

      {dashboard && (
        <div className="metrics-summary">
          <div className="metric-card">
            <div className="metric-value">{formatNumber(dashboard.documents.total_fetched)}</div>
            <div className="metric-label">Documents Fetched</div>
          </div>
          <div className="metric-card">
            <div className="metric-value success">{formatNumber(dashboard.conversions.successful)}</div>
            <div className="metric-label">Successful Conversions</div>
          </div>
          <div className="metric-card">
            <div className="metric-value error">{formatNumber(dashboard.conversions.failed)}</div>
            <div className="metric-label">Failed Conversions</div>
          </div>
          <div className="metric-card">
            <div className="metric-value">{dashboard.conversions.success_rate}%</div>
            <div className="metric-label">Success Rate</div>
          </div>
          <div className="metric-card">
            <div className="metric-value">{formatNumber(dashboard.parquet_files.total_files)}</div>
            <div className="metric-label">Parquet Files</div>
          </div>
          <div className="metric-card">
            <div className="metric-value">{formatBytes(dashboard.parquet_files.total_size_bytes)}</div>
            <div className="metric-label">Total Size</div>
          </div>
        </div>
      )}

      <div className="chart-container full-width">
        <h3>Data Flow: MongoDB to Azure</h3>
        <div className="chart-wrapper tall">
          <Line data={dataFlowChartData} options={chartOptions} />
        </div>
      </div>

      <div className="charts-grid">
        <div className="chart-container">
          <h3>Documents Fetched Over Time</h3>
          <div className="chart-wrapper">
            <Chart type="bar" data={documentsChartData} options={documentsDualAxisOptions} />
          </div>
        </div>

        <div className="chart-container">
          <h3>Conversions Over Time</h3>
          <div className="chart-wrapper">
            <Chart type="bar" data={conversionsChartData} options={conversionsDualAxisOptions} />
          </div>
        </div>

        <div className="chart-container small">
          <h3>Conversion Success Rate</h3>
          <div className="chart-wrapper pie">
            {conversionsPieData && <Doughnut data={conversionsPieData} options={chartOptions} />}
          </div>
        </div>

        <div className="chart-container">
          <h3>Parquet Files Over Time</h3>
          <div className="chart-wrapper">
            <Chart type="bar" data={parquetChartData} options={parquetDualAxisOptions} />
          </div>
        </div>
      </div>
    </div>
  );
}

export default MetricsDashboard;
