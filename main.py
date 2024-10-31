from flask import Flask, request, jsonify
import boto3
import json
import os

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
        firehose_client = boto3.client('firehose', region_name=os.environ.get('AWS_REGION', 'us-west-1'))
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

@app.route('/')
def hello_world():
    return '{"unit_lost_total": 2, "empire_approval": 41, "empire_diplomatic_relations": "None,Unknown,Unknown,Peace", "empire_money_net": 43, "sessionid": "0707a530b99346d799e7809e30c2cb02", "is_human": true, "eventlatitude": 25.4, "eventtimezone": "+03:00", "battle_won_per_empire_total": "0,0,0,0", "score": 1995, "active_quest_count": 1, "unit_killed_total": 13, "empire_grievances_count": "0,0,0,0,0,0,0", "empire_id": 0, "protectorate_minor_factions": "MinorFaction_Xavius:Assimilation:2.00", "empire_science_net": 43, "eventpostcode": "", "empire_army_upkeep": 31, "empire_army_naval_count": 0, "protectorate_slot_count": 3, "faction_quest_step": 5, "battle_count_per_empire_total": "0,0,0,0", "eventcountrycode": "SA", "empire_money_stock": 536, "empire_army_ground_count": 2, "user_id": "b/fDUWPaveu+62O6X58fymfH/XYEBdb0Gor9b98LniI=", "empire_influence_stock": 286, "eventcountry": "Saudi Arabia", "eventid": "22.3.2", "curiosity_collected_total": 17, "turn": 99, "battle_won_total": 7, "game_session_id": "5c4af3aac67e4204b2cb105fb383069e", "gamecode": "1896240", "battle_count_total": 7, "eventname": "EmpireEndTurn", "war_won_per_empire_total": "MinorFaction_Xavius:Assimilation:2.000,0,0,0", "game_id": "92fdb973-93e4-42b2-b4db-da0cece1309c", "application_version": "V0.13.00129383-S20, VIP (64-bit Standalone, build: 0)", "eventhaship": "HtuKh0HUX0TbgiaisR6NrA==", "eventlongitude": 49.65, "eventcity": "Al Jubayl", "faction_name": "Faction_KinOfSheredyn", "eventtimestamp": "2024-10-25T06:55:24Z", "eventtimestampclient": "2024-10-25T09:55:24.2256420+03:00", "recordid": "9965e6a5-cb6d-4482-bf2f-6aeff23d36e6", "eventstate": "Ash Sharqiyah", "empire_influence_net": 32, "platform_id": "1"}'

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3000)
