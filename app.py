# Modules
import pandas as pd
import streamlit as st
import calendar

from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta, date

# Layout
st.set_page_config(page_title="Refresh Reports", page_icon=":eyeglasses:", layout='wide')

# Get days of the week
dow = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# test day and time
df = pd.DataFrame(
    {
    'Project':['Stonks Data Pipeline']
    , 'Day': ['Tuesday']
    , 'Time': ['16:05:00']
    }
)

# Execute file function
def exec_file(filepath):
    global_namespace = {
        "__file__": filepath,
        "__name__": "__main__",
    }
    with open(filepath, 'rb') as file:
        exec(compile(file.read(), filepath, 'exec'), global_namespace)

# Run job function
def run_job_bool(df: pd.DataFrame = None, minute_window: int = 1):
    curr_date = date.today()
    dow = calendar.day_name[curr_date.weekday()]

    project_name = 'Stonks Data Pipeline'
    day = df[df['Project']==project_name]['Day'].values
    sched = df[df['Project']==project_name]['Time'].values
    
    now = datetime.now()
    bot_window = (now-timedelta(minutes=minute_window)).strftime("%H:%M:%S")
    top_window = (now+timedelta(minutes=minute_window)).strftime("%H:%M:%S")

    if day==dow:
        run_today = True
    else:
        run_today = False

    if (bot_window<=sched) & (top_window>=sched):
        run_now = True
    else:
        run_now = False

    if (run_now==True) & (run_today==True):
        run_job_now = True
    else:
        run_job_now = False

    return run_job_now

# Auto-refresh this app
counter = st_autorefresh(interval = 5000, limit = 1000, key = 'test')

# Title
st.sidebar.title("Report Refresher")

# Sidebar menu items
project = st.sidebar.selectbox(label = 'Select Project:', options = ['Stonks Data Pipeline'])

# Main Content Menu Items
st.subheader(project)
# refresh_date = st.selectbox(label = 'Day of Week:', options=['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
# refresh_time = st.time_input(label = 'Time Input:')

# Debugging output
st.text('Run current job: ' + str(run_job_bool(df)))

if run_job_bool(df)==True:
    job_start = datetime.now()
    job_check = 0
    st.text('Job started: '+str(job_start))
    exec_file('../stonks_etl.py')
    # st.text('Data file checked: ')
    # st.text('Status: ')