from flask import Flask, request, jsonify
import boto3
import json

app = Flask(__name__)

@app.route('/send-data', methods=['POST'])
def send_data():
    # Assuming JSON data is sent in the request body
    data = request.json

    # Convert data to JSON string format for Firehose
    record = {
        'Data': json.dumps(data) + '\n'  # Firehose expects newline-delimited records
    }

    try:
        # Initialize Firehose client
        firehose_client = boto3.client('firehose', region_name='eu-west-1')
    except Exception as e:
        return jsonify({'status': 'error', 'message': f"Error while connceting to firehose {e}"}), 500
    try:
        # Send data to Firehose
        response = firehose_client.put_record(
            DeliveryStreamName= 'amplitude-firehose-firehose-stream',
            Record=record
        )
        return jsonify({'status': 'success', 'response': response}), 200
    except Exception as e:
        return jsonify({'status': 'error', 'message': f"Error while writing to firehose {e}"}), 500        

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
