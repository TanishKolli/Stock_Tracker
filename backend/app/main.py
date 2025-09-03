from datetime import datetime, timedelta
from fastapi import FastAPI
from fastapi.responses import JSONResponse
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from fastapi.middleware.cors import CORSMiddleware
import uvicorn


app = FastAPI()

origins = [
    "http://127.0.0.1:5500",
    # Add more allowed origins if needed
]

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # You can restrict to specific HTTP methods if needed
    allow_headers=["*"],  # You can restrict to specific HTTP headers if needed
)


cluster = Cluster(['localhost'])  # Replace with your Cassandra node IP
session = cluster.connect('spark_streams')  # Replace with your keyspace name

def process_dataframe_for_last(df, time_range='6_hours'):

    def round_up_to_10_minutes(dt):
        remainder = dt.minute % 10
        if remainder == 0:
            return dt
        else:
            return dt + timedelta(minutes=(10 - remainder))
        
    # Get current date and time
    now = datetime.now()
    now = pd.to_datetime(now)
    rounded_now = round_up_to_10_minutes(now)

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    df['timestamp'] = pd.to_datetime(pd.to_datetime(df['timestamp']).dt.tz_convert('Asia/Bangkok')).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    end = rounded_now
    start = end - pd.Timedelta(hours=6)
    if time_range == "7_days":
        end = pd.to_datetime(now.date())
        start = end - pd.Timedelta(days=8)

    df_filtered = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]

    if time_range == '6_hours':
        df_filtered['timestamp_formated']= df_filtered['timestamp'].dt.strftime('%H:%M')
    else:
        df_filtered['timestamp_formated']= df_filtered['timestamp'].dt.strftime('%Y-%m-%d')

    return df_filtered

def query_request_excutor(session, project_id, from_timestamp):
    query = f"""
        SELECT * FROM aggregated_api_request
        WHERE project_id = '{project_id}'
        AND timestamp >= '{from_timestamp}'
        ;
    """
    statement = SimpleStatement(query)
    result = session.execute(statement)

    return result

# Close the session when the application shuts down
@app.on_event("shutdown")
def shutdown_event():
    session.shutdown()

def total_request(project_id):
    query = f"""
        SELECT sum(count) FROM aggregated_api_request
        WHERE project_id = '{project_id}'
        ;
    """
    statement = SimpleStatement(query)
    result = session.execute(statement)

    # Calculate request count
    df = pd.DataFrame(result)

    return int(df.iloc[0, 0])


@app.get("/api/request-chart")
async def get_request_count(project_id: str = None, time_range: str='6_hours'):
    # Filter DataFrame based on parameters
    start = "2023-11-01 00:00:00.000"
    result = query_request_excutor(session, project_id, start)

    # Calculate request count
    df = pd.DataFrame(result)
    df_success = df[df['event_type'] == 'PREDICT_COMPLETED']
    df_success_resampled =  df_success.set_index("timestamp").resample("10Min").sum().fillna(0).reset_index()

    df_failed = df[df['event_type'] == 'PREDICT_FAILED']
    df_failed_resampled =  df_failed.set_index("timestamp").resample("10Min").sum().fillna(0).reset_index()

    aggdf_event_success = process_dataframe_for_last(df_success_resampled, time_range)
    
    aggdf_event_failed = process_dataframe_for_last(df_failed_resampled, time_range)
    result_dict = {
        "failed": {
            'timestamp': aggdf_event_failed['timestamp_formated'].tolist(),
            'count': aggdf_event_failed['count'].tolist()
        },
         "success": {
            'timestamp': aggdf_event_success['timestamp_formated'].tolist(),
            'count': aggdf_event_success['count'].tolist()
        },
        "total": total_request(project_id)
    }

    return JSONResponse(content=result_dict)


if __name__ == "__main__":

    uvicorn.run(app, host="0.0.0.0", port=8080)
