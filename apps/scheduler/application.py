from flask import Flask, jsonify, request
from flask_cors import CORS
from data_operations import fetch_all_data, prepare_data_for_optimization
from genetic_scheduler import GeneticScheduler
from datetime import datetime, timedelta
import threading
import uuid

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Disable CSRF protection
app.config['WTF_CSRF_ENABLED'] = False

@app.route('/optimise', methods=['POST'])
def optimise():
    # Fetch all data from the database
    result = fetch_all_data()
    
    # If there was an error fetching data, return the error
    if result.get("status") == "error":
        return jsonify({
            "status": "error",
            "message": result.get("message")
        }), 500
    
    # Prepare data for optimization (but don't return it)
    optimized_data = prepare_data_for_optimization(result["data"])
    
    # Return only success message without the data
    return jsonify({
        "status": "success",
        "message": "Data fetched and prepared for optimization"
    })

def run_optimization(start_date, planning_horizon_days, optimization_run_id, previous_run_id=None):
    try:
        scheduler = GeneticScheduler()
        scheduler.run_optimization(
            start_date=start_date,
            planning_horizon_days=planning_horizon_days,
            debug_level=1,
            optimization_run_id=optimization_run_id,
            previous_run_id=previous_run_id
        )
    except Exception as e:
        print(f"Error in optimization thread: {str(e)}")

@app.route('/optimize', methods=['POST'])
def optimize():
    try:
        # Get parameters from request
        data = request.get_json()
        
        # Get planning horizon days
        planning_horizon_days = data.get('planning_horizon_days', 120) if data else 120
        
        # Generate a unique ID for this optimization run
        optimization_run_id = str(uuid.uuid4())
        
        # Start optimization in a separate thread
        thread = threading.Thread(
            target=run_optimization,
            args=(None, planning_horizon_days, optimization_run_id)
        )
        thread.daemon = True  # Thread will be terminated when main program exits
        thread.start()
        
        # Return immediate success response
        return jsonify({
            "status": "success",
            "message": "Optimization started successfully",
            "optimization_run_id": optimization_run_id
        })
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/rolling-optimize', methods=['POST'])
def rolling_optimize():
    try:
        # Get parameters from request
        data = request.get_json()
        
        # Get required optimization_run_id
        optimization_run_id = data.get('optimization_run_id')
        if not optimization_run_id:
            return jsonify({
                "status": "error",
                "message": "optimization_run_id is required"
            }), 400
            
        # Get planning horizon days
        planning_horizon_days = data.get('planning_horizon_days', 120) if data else 120
        
        # Generate a unique ID for this optimization run
        new_optimization_run_id = str(uuid.uuid4())
        
        # Start optimization in a separate thread
        thread = threading.Thread(
            target=run_optimization,
            args=(None, planning_horizon_days, new_optimization_run_id, optimization_run_id)
        )
        thread.daemon = True  # Thread will be terminated when main program exits
        thread.start()
        
        # Return immediate success response
        return jsonify({
            "status": "success",
            "message": "Rolling optimization started successfully",
            "optimization_run_id": new_optimization_run_id
        })
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

if __name__ == '__main__':
    app.run(debug=True)
