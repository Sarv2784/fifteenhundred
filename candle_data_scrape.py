import http.client
import json

import pandas as pd
from datetime import datetime, timedelta
import pytz
from pandas import json_normalize

api_key = "organizations/0f4cb259-a82f-4776-959f-d2a42fa23cb4/apiKeys/2697ec7f-ec4f-41a5-8a01-715b6102505d"
api_secret = "-----BEGIN EC PRIVATE KEY-----\nMHcCAQEEIG4EYDoUBZkB2j5i6CrNVjlHhE7+G+Xgf95pBSwWQ9ygoAoGCCqGSM49\nAwEHoUQDQgAEAzhdAQvinxkKzhVAoCp122vgka1vwt+DisGMP2AQOEiTAxgC/V/m\nESgdCLpSKZI2ebwvqjRD1VyR95QNAbjplQ==\n-----END EC PRIVATE KEY-----\n"
base_url = 'api.coinbase.com'
api_path = "/api/v3/brokerage/market/products/BTC-USD/candles?start={0}&end={1}&granularity={2}&limit={3}"


def convert_time_est_to_unix(est_time):
    est = pytz.timezone('US/Eastern')
    localized_time = est.localize(est_time)
    utc_time = localized_time.astimezone(pytz.utc)
    return int(utc_time.timestamp())


def get_bitcoin_hourly_prices(est_start_date, est_end_date, start_time, end_time, granularity='ONE_HOUR', limit=12):
    data_list = pd.DataFrame(columns=["start", "low", "high","open","close","volume"])
    start_date = datetime.strptime(est_start_date, '%m/%d/%Y')
    end_date = datetime.strptime(est_end_date, '%m/%d/%Y')
    start_time_part = datetime.strptime(start_time, '%H:%M:%S').time()
    end_time_part = datetime.strptime(end_time, '%H:%M:%S').time()
    current_date = start_date
    payload = ''
    headers = {
        'Content-Type': 'application/json'
    }
    try:
        conn = http.client.HTTPSConnection(base_url)
        while end_date <= current_date:
            unix_start_time = convert_time_est_to_unix(datetime.combine(current_date, start_time_part))
            unix_end_time = convert_time_est_to_unix(datetime.combine(current_date, end_time_part))
            formatted_api = api_path.format(unix_start_time, unix_end_time, granularity, 100)
            conn.request("GET", formatted_api, payload, headers)
            res = conn.getresponse()
            json_string = res.read().decode('utf-8')
            json_data = json.loads(json_string)
            candles_date = json_data['candles']
            data_list = pd.concat([data_list, json_normalize(candles_date)], ignore_index=True)

            current_date -= timedelta(days=1)
    except Exception as err:
        print(str(err))

    df = pd.DataFrame(data_list)
    df['start'] = pd.to_datetime(df['start'], unit='s', utc=True)
    df['start'] = df['start'].dt.tz_convert('US/Eastern')
    df['day_name'] = df['start'].dt.day_name()
    df['open'] = pd.to_numeric(df['open'])
    df['close'] = pd.to_numeric(df['close'])
    df['pricechange'] = df['open'] - df['close']
    df['hour'] = df['start'].dt.hour

    # Step 3: Split the data by hour and save each group as a separate CSV file
    for hour, group in df.groupby('hour'):
        filename = f"/Users/Victoria/PycharmProjects/BTCData/hour_{hour:02d}.csv"
        group.to_csv(filename, index=False)
        print(f"Saved file: {filename}")


if __name__ == '__main__':
    get_bitcoin_hourly_prices('09/26/2024', '09/01/2023', '00:00:0', '23:59:59', 'ONE_HOUR', 25)
