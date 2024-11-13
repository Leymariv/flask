from fastapi import FastAPI, HTTPException, Request
import boto3
import json
from fastapi.responses import JSONResponse

app = FastAPI()

def initialize_firehose_client():
    try:
        return boto3.client('firehose', region_name='eu-west-1')
    except Exception as e:
        raise Exception(f"Error while connecting to Firehose: {e}")


def send_to_firehose(data: dict):
    record = {
        'Data': json.dumps(data) + '\n'  # Firehose expects newline-delimited records
    }

    firehose_client = initialize_firehose_client()
    
    try:
        response = firehose_client.put_record(
            DeliveryStreamName='amplitude-firehose-firehose-stream',
            Record=record
        )
        return response
    except Exception as e:
        raise Exception(f"Error while writing to Firehose: {e}")


@app.post("/send-data")
async def send_data(request: Request):
    try:
        data = await request.json()

        response = send_to_firehose(data)

        return JSONResponse(content={"status": "success", "response": response}, status_code=200)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
