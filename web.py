import os
from apscheduler.schedulers.background import BackgroundScheduler
from helpers import log
from .tasks import process_berichten_in
from .tasks import process_berichten_out

RUN_INTERVAL = int(os.environ.get('RUN_INTERVAL')) #in minutes

scheduler = BackgroundScheduler()
scheduler.add_job(func=process_berichten_in, trigger="interval", minutes=RUN_INTERVAL)
log("Registered a task for fetching and processing messages from Kalliope every {} minutes".format(RUN_INTERVAL))
scheduler.add_job(func=process_berichten_out, trigger="interval", minutes=RUN_INTERVAL)
log("Registered a task for fetching and processing messages to Kalliope every {} minutes".format(RUN_INTERVAL))
scheduler.start()

