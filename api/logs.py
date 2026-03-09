from flask import request, jsonify
from api import api_bp
from log_database import get_log_database


@api_bp.route('/logs', methods=['GET'])
def get_logs():
    """
    Query logs with filters.
    
    Query parameters:
        - level: Filter by log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        - collection: Filter by collection name
        - source_type: Filter by source type ('application' or 'backend')
        - search: Search text in log messages
        - start_time: Filter logs after this timestamp (ISO format)
        - end_time: Filter logs before this timestamp (ISO format)
        - limit: Maximum number of logs to return (default: 100, max: 1000)
        - offset: Number of logs to skip for pagination (default: 0)
        - order: Sort order - 'desc' (default) or 'asc'
    
    Returns:
        JSON with logs array, total count, and pagination info
    """
    try:
        level_filter = request.args.get('level')
        collection_filter = request.args.get('collection')
        source_type_filter = request.args.get('source_type')
        search_text = request.args.get('search')
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        
        limit = min(int(request.args.get('limit', 100)), 1000)
        offset = int(request.args.get('offset', 0))
        order = request.args.get('order', 'desc').lower()
        order_desc = order != 'asc'
        
        db = get_log_database()
        result = db.query_logs(
            level_filter=level_filter,
            collection_filter=collection_filter,
            source_type_filter=source_type_filter,
            search_text=search_text,
            start_time=start_time,
            end_time=end_time,
            limit=limit,
            offset=offset,
            order_desc=order_desc
        )
        
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/stats', methods=['GET'])
def get_log_stats():
    """
    Get log statistics for dashboard display.
    
    Returns:
        JSON with total count, counts by level, and recent activity metrics
    """
    try:
        db = get_log_database()
        stats = db.get_log_stats()
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/collections', methods=['GET'])
def get_collections():
    """
    Get list of unique collection names from logs.
    
    Returns:
        JSON array of collection names
    """
    try:
        db = get_log_database()
        collections = db.get_collections()
        return jsonify({"collections": collections})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/levels', methods=['GET'])
def get_log_levels():
    """
    Get list of available log levels.
    
    Returns:
        JSON array of log level names
    """
    try:
        db = get_log_database()
        levels = db.get_log_levels()
        
        if not levels:
            levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        
        return jsonify({"levels": levels})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/source-types', methods=['GET'])
def get_source_types():
    """
    Get list of available source types.
    
    Returns:
        JSON array of source type names ('application', 'backend')
    """
    try:
        db = get_log_database()
        source_types = db.get_source_types()
        return jsonify({"source_types": source_types})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/cleanup', methods=['POST'])
def cleanup_logs():
    """
    Delete logs older than the retention period.
    
    Request body (JSON):
        - retention_days: Number of days to retain logs (default: 30)
    
    Returns:
        JSON with the number of deleted logs
    """
    try:
        data = request.get_json() or {}
        retention_days = int(data.get('retention_days', 30))
        
        if retention_days < 1:
            return jsonify({"error": "retention_days must be at least 1"}), 400
        
        db = get_log_database()
        deleted_count = db.cleanup_old_logs(retention_days)
        
        return jsonify({
            "deleted_count": deleted_count,
            "retention_days": retention_days
        })
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/compression/stats', methods=['GET'])
def get_compression_stats():
    """
    Get compression statistics for the log database.
    
    Returns:
        JSON with compression statistics including total logs, compressed count,
        compression rate, and total storage size.
    """
    try:
        db = get_log_database()
        stats = db.get_compression_stats()
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/logs/compression/compress', methods=['POST'])
def compress_existing_logs():
    """
    Compress existing uncompressed log messages.
    
    Request body (JSON):
        - batch_size: Number of logs to process per batch (default: 1000)
    
    Returns:
        JSON with compression results including count of compressed and skipped logs.
    """
    try:
        data = request.get_json() or {}
        batch_size = int(data.get('batch_size', 1000))
        
        if batch_size < 1 or batch_size > 10000:
            return jsonify({"error": "batch_size must be between 1 and 10000"}), 400
        
        db = get_log_database()
        result = db.compress_existing_logs(batch_size)
        
        return jsonify(result)
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500
