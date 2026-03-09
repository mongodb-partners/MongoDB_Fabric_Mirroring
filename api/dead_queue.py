from flask import request, jsonify
from api import api_bp
from dead_queue_database import get_dead_queue_database


@api_bp.route('/dead-queue', methods=['GET'])
def get_dead_queue():
    """
    Query dead queue entries with filters.
    
    Query parameters:
        - collection: Filter by collection name
        - field: Filter by field name
        - search: Search text in values, errors, document IDs
        - start_time: Filter entries after this timestamp (ISO format)
        - end_time: Filter entries before this timestamp (ISO format)
        - limit: Maximum number of entries to return (default: 100, max: 1000)
        - offset: Number of entries to skip for pagination (default: 0)
        - order: Sort order - 'desc' (default) or 'asc'
    
    Returns:
        JSON with entries array, total count, and pagination info
    """
    try:
        collection_filter = request.args.get('collection')
        field_filter = request.args.get('field')
        search_text = request.args.get('search')
        start_time = request.args.get('start_time')
        end_time = request.args.get('end_time')
        
        limit = min(int(request.args.get('limit', 100)), 1000)
        offset = int(request.args.get('offset', 0))
        order = request.args.get('order', 'desc').lower()
        order_desc = order != 'asc'
        
        db = get_dead_queue_database()
        result = db.query_dead_queue(
            collection_filter=collection_filter,
            field_filter=field_filter,
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


@api_bp.route('/dead-queue/<int:entry_id>', methods=['GET'])
def get_dead_queue_entry(entry_id):
    """
    Get full details of a dead queue entry.
    
    Returns:
        JSON with complete entry details including document JSON
    """
    try:
        db = get_dead_queue_database()
        entry = db.get_entry_details(entry_id)
        
        if not entry:
            return jsonify({"error": "Entry not found"}), 404
        
        return jsonify(entry)
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/dead-queue/<int:entry_id>', methods=['DELETE'])
def delete_dead_queue_entry(entry_id):
    """
    Delete a dead queue entry.
    
    Returns:
        JSON with success status
    """
    try:
        db = get_dead_queue_database()
        deleted = db.delete_entry(entry_id)
        
        if not deleted:
            return jsonify({"error": "Entry not found"}), 404
        
        return jsonify({"success": True, "deleted_id": entry_id})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/dead-queue/stats', methods=['GET'])
def get_dead_queue_stats():
    """
    Get dead queue statistics.
    
    Returns:
        JSON with total count, counts by collection and field
    """
    try:
        db = get_dead_queue_database()
        stats = db.get_stats()
        return jsonify(stats)
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/dead-queue/collections', methods=['GET'])
def get_dead_queue_collections():
    """
    Get list of collections with dead queue entries.
    
    Returns:
        JSON array of collection names
    """
    try:
        db = get_dead_queue_database()
        collections = db.get_collections()
        return jsonify({"collections": collections})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/dead-queue/fields', methods=['GET'])
def get_dead_queue_fields():
    """
    Get list of fields that had conversion failures.
    
    Query parameters:
        - collection: Optional filter by collection name
    
    Returns:
        JSON array of field names
    """
    try:
        collection = request.args.get('collection')
        db = get_dead_queue_database()
        fields = db.get_fields(collection)
        return jsonify({"fields": fields})
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/dead-queue/cleanup', methods=['POST'])
def cleanup_dead_queue():
    """
    Delete entries older than the retention period.
    
    Request body (JSON):
        - retention_days: Number of days to retain entries (default: 30)
    
    Returns:
        JSON with the number of deleted entries
    """
    try:
        data = request.get_json() or {}
        retention_days = int(data.get('retention_days', 30))
        
        if retention_days < 1:
            return jsonify({"error": "retention_days must be at least 1"}), 400
        
        db = get_dead_queue_database()
        deleted_count = db.cleanup_old_entries(retention_days)
        
        return jsonify({
            "deleted_count": deleted_count,
            "retention_days": retention_days
        })
        
    except ValueError as e:
        return jsonify({"error": f"Invalid parameter: {str(e)}"}), 400
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500


@api_bp.route('/dead-queue/collection/<collection_name>', methods=['DELETE'])
def delete_dead_queue_by_collection(collection_name):
    """
    Delete all dead queue entries for a specific collection.
    
    Returns:
        JSON with the number of deleted entries
    """
    try:
        db = get_dead_queue_database()
        deleted_count = db.delete_by_collection(collection_name)
        
        return jsonify({
            "deleted_count": deleted_count,
            "collection": collection_name
        })
        
    except Exception as e:
        return jsonify({"error": f"Internal error: {str(e)}"}), 500
