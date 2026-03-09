# MongoDB Fabric Mirroring - Web Frontend

This is the React-based web frontend for the MongoDB Fabric Mirroring service dashboard.

## Prerequisites

- Node.js 18+ 
- npm or yarn

## Development

1. Install dependencies:
   ```bash
   cd frontend
   npm install
   ```

2. Start the development server:
   ```bash
   npm start
   ```
   
   The React dev server runs on port 3000 and proxies API requests to Flask on port 5000.

3. Make sure the Flask backend is running with `ENABLE_FRONTEND=true`:
   ```bash
   ENABLE_FRONTEND=true python app.py
   ```

## Production Build

1. Build the production bundle:
   ```bash
   npm run build
   ```

2. The built files will be in the `build/` directory. Flask will serve these automatically when `ENABLE_FRONTEND=true`.

## Features

### Log Viewer
- View logs stored in SQLite database
- Filter by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Filter by MongoDB collection name
- Search within log messages
- Filter by date/time range
- Pagination with configurable page size
- Auto-refresh option (5 second intervals)
- Statistics dashboard showing log counts by level

### Coming Soon
- Metrics charts (documents fetched, conversions, parquet uploads)
- Dead letter queue viewer for failed conversions

## Environment Variables

The frontend behavior is controlled by these backend environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `ENABLE_FRONTEND` | `false` | Set to `true` to enable the web dashboard |
| `LOG_TO_SQLITE` | `true` | Enable SQLite logging for the log viewer |
| `LOG_RETENTION_DAYS` | `30` | Days to retain logs before automatic cleanup |

## API Endpoints

The frontend uses these API endpoints:

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/logs` | GET | Query logs with filters and pagination |
| `/api/logs/stats` | GET | Get log statistics |
| `/api/logs/collections` | GET | Get list of collections |
| `/api/logs/levels` | GET | Get list of log levels |
| `/api/logs/cleanup` | POST | Trigger log cleanup |
