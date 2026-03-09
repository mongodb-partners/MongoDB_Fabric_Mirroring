from flask import request, jsonify
from api import api_bp
from metrics_database import get_metrics_database


@api_bp.route('/metrics/dashboard', methods=['GET'])
def get_dashboard():
    """
    Get dashboard summary of all metrics.
    
    Query parameters:
        - hours: Time period in hours (default: 24)
    
    Returns:
        JSON with summary of documents, conversions, and parquet files
    """
    try:
        hours = int(request.args.get('hours', 24))
        
        db = get_metrics_database()
        summary = db.get_dashboard_summary(hours=hours)
        
        return jsonify(summary)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/documents', methods=['GET'])
def get_documents_metrics():
    """
    Get documents fetched metrics.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
    
    Returns:
        JSON with documents fetched summary
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        
        db = get_metrics_database()
        summary = db.get_documents_fetched_summary(
            collection_name=collection,
            hours=hours
        )
        
        return jsonify(summary)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/documents/timeseries', methods=['GET'])
def get_documents_timeseries():
    """
    Get time series data for documents fetched.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
        - interval: Aggregation interval in minutes (default: 60)
    
    Returns:
        JSON array of time-bucketed document counts
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        interval = int(request.args.get('interval', 60))
        
        db = get_metrics_database()
        data = db.get_documents_fetched_timeseries(
            collection_name=collection,
            hours=hours,
            interval_minutes=interval
        )
        
        return jsonify({"data": data, "hours": hours, "interval_minutes": interval})
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/conversions', methods=['GET'])
def get_conversions_metrics():
    """
    Get conversion metrics.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
    
    Returns:
        JSON with conversion summary (successful/failed counts)
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        
        db = get_metrics_database()
        summary = db.get_conversion_summary(
            collection_name=collection,
            hours=hours
        )
        
        return jsonify(summary)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/conversions/timeseries', methods=['GET'])
def get_conversions_timeseries():
    """
    Get time series data for conversions.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
        - interval: Aggregation interval in minutes (default: 60)
    
    Returns:
        JSON array of time-bucketed conversion counts
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        interval = int(request.args.get('interval', 60))
        
        db = get_metrics_database()
        data = db.get_conversion_timeseries(
            collection_name=collection,
            hours=hours,
            interval_minutes=interval
        )
        
        return jsonify({"data": data, "hours": hours, "interval_minutes": interval})
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/parquet', methods=['GET'])
def get_parquet_metrics():
    """
    Get parquet file metrics.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
    
    Returns:
        JSON with parquet files summary
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        
        db = get_metrics_database()
        summary = db.get_parquet_files_summary(
            collection_name=collection,
            hours=hours
        )
        
        return jsonify(summary)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/parquet/files', methods=['GET'])
def get_parquet_files_list():
    """
    Get list of parquet files with pagination.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
        - limit: Maximum number of files to return (default: 50)
        - offset: Number of records to skip (default: 0)
    
    Returns:
        JSON with paginated parquet file details
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        limit = min(int(request.args.get('limit', 50)), 500)
        offset = int(request.args.get('offset', 0))
        
        db = get_metrics_database()
        result = db.get_parquet_files_list(
            collection_name=collection,
            hours=hours,
            limit=limit,
            offset=offset
        )
        
        result["hours"] = hours
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/parquet/timeseries', methods=['GET'])
def get_parquet_timeseries():
    """
    Get time series data for parquet files.
    
    Query parameters:
        - collection: Filter by collection name
        - hours: Time period in hours (default: 24)
        - interval: Aggregation interval in minutes (default: 60)
    
    Returns:
        JSON array of time-bucketed parquet file metrics
    """
    try:
        collection = request.args.get('collection')
        hours = int(request.args.get('hours', 24))
        interval = int(request.args.get('interval', 60))
        
        db = get_metrics_database()
        data = db.get_parquet_timeseries(
            collection_name=collection,
            hours=hours,
            interval_minutes=interval
        )
        
        return jsonify({"data": data, "hours": hours, "interval_minutes": interval})
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/collections', methods=['GET'])
def get_metrics_collections():
    """
    Get list of collections with metrics.
    
    Returns:
        JSON array of collection names
    """
    try:
        db = get_metrics_database()
        collections = db.get_collections()
        
        return jsonify({"collections": collections})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/metrics/cleanup', methods=['POST'])
def cleanup_metrics():
    """
    Delete metrics older than the retention period.
    
    Request body (JSON):
        - retention_days: Number of days to retain metrics (default: 30)
    
    Returns:
        JSON with counts of deleted records per table
    """
    try:
        data = request.get_json() or {}
        retention_days = int(data.get('retention_days', 30))
        
        if retention_days < 1:
            return jsonify({"error": "retention_days must be at least 1"}), 400
        
        db = get_metrics_database()
        deleted = db.cleanup_old_metrics(retention_days)
        
        return jsonify({
            "deleted": deleted,
            "retention_days": retention_days
        })
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500
