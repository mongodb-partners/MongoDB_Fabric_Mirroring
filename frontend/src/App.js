import React, { useState } from 'react';
import LogViewer from './components/LogViewer';
import MetricsDashboard from './components/MetricsDashboard';
import DeadQueueViewer from './components/DeadQueueViewer';
import ReportsPage from './components/ReportsPage';
import './App.css';

function App() {
  const [activeTab, setActiveTab] = useState('logs');

  return (
    <div className="app">
      <header className="app-header">
        <h1>MongoDB Fabric Mirroring</h1>
        <nav className="app-nav">
          <button 
            className={`nav-btn ${activeTab === 'logs' ? 'active' : ''}`}
            onClick={() => setActiveTab('logs')}
          >
            Logs
          </button>
          <button 
            className={`nav-btn ${activeTab === 'metrics' ? 'active' : ''}`}
            onClick={() => setActiveTab('metrics')}
          >
            Metrics
          </button>
          <button 
            className={`nav-btn ${activeTab === 'reports' ? 'active' : ''}`}
            onClick={() => setActiveTab('reports')}
          >
            Reports
          </button>
          <button 
            className={`nav-btn ${activeTab === 'deadqueue' ? 'active' : ''}`}
            onClick={() => setActiveTab('deadqueue')}
          >
            Dead Queue
          </button>
        </nav>
      </header>
      
      <main className="app-main">
        {activeTab === 'logs' && <LogViewer />}
        {activeTab === 'metrics' && <MetricsDashboard />}
        {activeTab === 'reports' && <ReportsPage />}
        {activeTab === 'deadqueue' && <DeadQueueViewer />}
      </main>
      
      <footer className="app-footer">
        <p>MongoDB Fabric Mirroring Service Dashboard</p>
      </footer>
    </div>
  );
}

export default App;
