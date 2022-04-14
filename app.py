# Modules
import pandas as pd
import streamlit as st
import calendar
import json

from streamlit_autorefresh import st_autorefresh
from datetime import datetime, timedelta, date

# Read in config
f = open('config.json')
config = json.load(f)

# Layout
st.set_page_config(page_title="Refresh Reports", page_icon=":eyeglasses:", layout='wide')

# Get days of the week
dow = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']

# test day and time
df = pd.DataFrame(
    {
    'Project':['Stonks Data Pipeline']
    , 'Day': ['Wednesday']
    , 'Time': ['08:39:00']
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
def run_job_bool(
      config: json = None
    , proj_name: str = None
    , minute_window: int = 1
    ):
    
    """
    Description

    Args:

    Returns:

    """
    df = pd.json_normalize(config[0]["Schedules"][proj_name])
    
    curr_date = date.today()
    dow = calendar.day_name[curr_date.weekday()]

    day = df['Day'].values
    sched = df['Time'].values
    
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
    #return print(str(day)+"|"+str(sched)+' | run_today: '+str(run_today)+' | run_now: '+str(run_now))

# Auto-refresh this app
counter = st_autorefresh(interval = 5000, limit = 1000, key = 'test')

# Title
st.sidebar.title("Report Refresher")

# Sidebar menu items
project = st.sidebar.selectbox(label = 'Select Project:', options = ['Stonks Data Pipeline'])

# Main Content Items
st.subheader(project)

# Debugging output
st.text('Run current job: ' + str(run_job_bool(config, 'Stonks Data Pipeline', 1)))

# Run Stonks Job
if run_job_bool(config=config, proj_name='Stonks Data Pipeline', minute_window=1)==True:
    job_start = datetime.now()
   
    st.text('Job started: '+str(job_start))
    try:
        exec_file('pipeline_scripts/stonks_etl.py')
    except:
        job_status = 'Error: Check ETL Script'
    else:
         job_status = 'No Errors'
    job_stop = datetime.now()

    log = pd.DataFrame({"Job Start": job_start, "Job Stop": job_stop, "Status":job_status})
    log.to_json("logs/stonks_data_pipeline"+str(datetime.today()))
