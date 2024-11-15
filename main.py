import re
import boto3
import json
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

app = FastAPI()

VALID_GAME_ID = {13, 21, 22}

def validate_eventid(eventid: str) -> bool:
    pattern = r"^(\d+)\.(\d+)\.(\d+)$"
    match = re.match(pattern, eventid)
    
    if not match:
        return False
    
    x, y, z = int(match.group(1)), int(match.group(2)), int(match.group(3))
    
    return x in VALID_GAME_ID
  

def initialize_firehose_client():
    try:
        return boto3.client('firehose', region_name='eu-west-1')
    except Exception as e:
        raise Exception(f"Error while connecting to Firehose: {e}")


def send_to_firehose(delivery_stream_name: str, data: dict):
    firehose_client = initialize_firehose_client()

    record = {
        'Data': json.dumps(data) + '\n'
    }
    
    try:
        response = firehose_client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record=record
        )
        return response
    except Exception as e:        
        raise Exception(f"Error while writing to Firehose: {e}")


@app.post("/send-data")
async def send_data(request: Request):
    data = await request.json()
    try:
        eventid = data.get("eventid")

        if not eventid or not validate_eventid(eventid):
            raise HTTPException(status_code=400, detail="Invalid eventid format. Expected x.y.z where x is a valid choice, and y, z are integers.")

        response = send_to_firehose('amplitude-firehose-firehose-stream', data)
    
        return JSONResponse(content={"status": "success", "response": response}, status_code=200)
    except Exception as e:
        response = send_to_firehose('amplitude-firehose-error-stream', data)
        return JSONResponse(content={"status": "failure", "response": response, "error": str(e)}, status_code=500)


@app.get("/test")
async def send_data(request: Request):
    return JSONResponse(content={"status": "hello valmon"}, status_code=200)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
