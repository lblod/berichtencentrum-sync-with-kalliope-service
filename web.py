import os
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from helpers import log
from .task_process_inzendingen_voor_toezicht import process_inzendingen
from .task_process_berichten_in import process_berichten_in
from .task_process_berichten_in_confirmation import process_confirmations
from .task_process_berichten_out import process_berichten_out

BERICHTEN_CRON_PATTERN = os.environ.get('BERICHTEN_CRON_PATTERN')
INZENDINGEN_CRON_PATTERN = os.environ.get('INZENDINGEN_CRON_PATTERN')
BERICHTEN_IN_CONFIRMATION_CRON_PATTERN = os.environ.get('BERICHTEN_IN_CONFIRMATION_CRON_PATTERN')

# DEBUGGING STARTS HERE
#import pdb; pdb.set_trace()
#process_berichten_in() # you need to call this like this, somehow else it doesn't stop in deeper breakpoints
# END DEBUGGING
scheduler = BackgroundScheduler()

scheduler.add_job(process_inzendingen, CronTrigger.from_crontab(INZENDINGEN_CRON_PATTERN))
log("Registered a task for fetching and processing inzendingen to Kalliope following pattern {}"
    .format(INZENDINGEN_CRON_PATTERN))

scheduler.add_job(process_berichten_in, CronTrigger.from_crontab(BERICHTEN_CRON_PATTERN))
log("Registered a task for fetching and processing messages from Kalliope following pattern {}"
    .format(BERICHTEN_CRON_PATTERN))

scheduler.add_job(process_berichten_out, CronTrigger.from_crontab(BERICHTEN_CRON_PATTERN))
log("Registered a task for fetching and processing messages to Kalliope following pattern {}"
    .format(BERICHTEN_CRON_PATTERN))

scheduler.add_job(process_confirmations, CronTrigger.from_crontab(BERICHTEN_IN_CONFIRMATION_CRON_PATTERN))
log("Registered a task for fetching and processing messages to Kalliope following pattern {}"
    .format(BERICHTEN_IN_CONFIRMATION_CRON_PATTERN))

scheduler.start()
