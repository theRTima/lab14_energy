from flask import Flask, request, jsonify
from datetime import datetime
import logging

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

aggregated_data_store = []

@app.route('/health', methods=['GET'])
def health():
    return jsonify({"status": "healthy"}), 200

@app.route('/aggregate', methods=['POST'])
def receive_aggregate():
    try:
        data = request.get_json()

        if not data:
            return jsonify({"error": "No data provided"}), 400

        aggregated_data_store.append({
            "received_at": datetime.now().isoformat(),
            "data": data
        })

        logger.info(f"Received aggregated data from collector {data.get('collector_id')} "
                   f"for shard {data.get('shard_id')}: {data.get('total_count')} readings")

        total_kwh = sum(m['sum_kwh'] for m in data.get('metrics', {}).values())
        avg_kwh = sum(m['avg_kwh'] for m in data.get('metrics', {}).values()) / len(data.get('metrics', {})) if data.get('metrics') else 0

        logger.info(f"  Total energy: {total_kwh:.2f} kWh, Average power: {avg_kwh:.2f} kWh")

        return jsonify({
            "status": "success",
            "received_count": data.get('total_count'),
            "meters_count": len(data.get('metrics', {}))
        }), 200

    except Exception as e:
        logger.error(f"Error processing aggregate data: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/stats', methods=['GET'])
def get_stats():
    if not aggregated_data_store:
        return jsonify({
            "total_windows": 0,
            "total_readings": 0
        }), 200

    total_readings = sum(item['data'].get('total_count', 0) for item in aggregated_data_store)
    total_windows = len(aggregated_data_store)

    collectors = {}
    for item in aggregated_data_store:
        collector_id = item['data'].get('collector_id')
        if collector_id:
            collectors[collector_id] = collectors.get(collector_id, 0) + 1

    return jsonify({
        "total_windows": total_windows,
        "total_readings": total_readings,
        "collectors": collectors,
        "latest_window": aggregated_data_store[-1]['data'] if aggregated_data_store else None
    }), 200

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=False)
