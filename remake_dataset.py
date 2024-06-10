import pandas as pd
import os
from datetime import datetime, timedelta

def load_recent_data(base_dir, months=3):
    end_date = datetime.now()
    start_date = end_date - timedelta(days=months * 30)  # Approximately three months
    frames = []
    for month_count in range(months + 1):  # Current month + last three months
        year_month = (start_date + timedelta(days=30 * month_count)).strftime('%Y_%m')
        file_path = os.path.join(base_dir, f'O_NFCI_{year_month}_clean.xlsx')
        if os.path.exists(file_path):
            df = pd.read_excel(file_path)
            frames.append(df)
    return pd.concat(frames) if frames else pd.DataFrame()

def load_static_data(static_dir, filename):
    return pd.read_excel(os.path.join(static_dir, filename))
