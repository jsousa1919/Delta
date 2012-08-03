#!/usr/bin/python

import threading
from datetime import datetime, timedelta
import pytz
import sys
import threading
import pickle
import ConfigParser, os
from tasks import tasks
import base64
from database.sqlite import database
import logging
import traceback

INTERVAL = 60

class Agent:
    wakes = 0
    tasks_completed = 0
    start_time = None
    last_status = None

    def __init__(self, config_file):
        config = ConfigParser.ConfigParser()
        config.readfp(open(config_file))
        self.log_file = config.get("Logging", "file")
        logging.basicConfig(filename = self.log_file, level = logging.DEBUG, format = "%(levelname)8s || %(asctime)23s || Module %(module)15s:%(lineno)3d || %(message)s")
        self.db_params = [config.get("SQLite", "database")]
        self.db = database(self.db_params)
        self.wake_interval = int(config.get("Agent", "wake_every"))
        self.local_timezone = pytz.timezone ("America/New_York")
        self.woke_at = self.utc_for(datetime.now())
        self.tasks = dict([(t.taskName, t) for t in tasks.TaskTypes]) 
        self.ST_user_cache = []
        self.ST_TN_cache = []
        self.expire = False
        self.blackboard = {}
        self.start_time = datetime.now()
        self.last_status = datetime.now()
        logging.info("Initialized agent with config: %s", config_file)
    
    def utc_for(self, dt):
        local_dt = dt.replace (tzinfo = self.local_timezone)
        return local_dt.astimezone(pytz.utc)        
        
    def wake(self):
        self.db = database(self.db_params)
        self.woke_at = self.utc_for(datetime.now())
        logging.debug("Woke at: %s", self.woke_at)

        self.process_tasks()
        if not self.expire: 
            threading.Timer(self.wake_interval, self.wake).start()

        self.db.commit()
        self.db.close()
        self.db = None
        self.wakes += 1

        now = datetime.now()
        diff = (now - self.last_status).total_seconds()
        if diff > INTERVAL:
            minutes = (now - self.start_time).total_seconds() / 60
            logging.info("Wakes per minute: %f" % (self.wakes / minutes))
            logging.info("Tasks per minute: %f" % (self.tasks_completed / minutes))
            self.last_status = now
            
    ###############################################################
    ## Blackbaord
    ## For tasks to store data in
    ## 

    def stash(self, key, obj):
       self.blackboard[key] = obj    
    
    def fetch(self, key):
       return self.blackboard[key] 
    
    ###############################################################
    ## Task Processing
    ## Ideally this would be nicely split out into separate code files
    ## but for the time being it will all be globbed into this function

    def unfinished_tasks(self):
        sql = "SELECT tid, task, after, delta, reschedule, args FROM tasks WHERE complete = 0 AND after < ? ORDER BY after ASC"
        params = [datetime.now()]
        return self.db.query(sql, params)

    def process_tasks(self):
        for task in self.unfinished_tasks():
            self.process_task(task)

    def process_task(self, task):
        try:
            taskType = self.tasks[task[1]]
        except KeyError:
            logging.error("Unknown Task: %s", str(task))
            return

        logging.info("Initializing task %d: %s", task[0], taskType.taskName)
        taskImpl = taskType(self, pickle.loads(base64.b64decode(task[5])))
        taskImpl.tid = task[0]
        taskImpl.after = datetime.strptime(task[2], "%Y-%m-%d %H:%M:%S.%f")
        taskImpl.delta = task[3]
        taskImpl.reschedule = task[4]

        try:
            logging.debug("Executing...")
            taskImpl.execute()
            self.tasks_completed += 1
        except Exception as ex:
            self.db.rollback()
            exc_type, exc_value, exc_traceback = sys.exc_info()
            trace = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.critical("Unknown Exception: %s", str(ex))
            logging.critical("Task Details: %d - %s %s", taskImpl.tid, taskImpl.taskName, str(taskImpl.args))
            for tb in trace:
                logging.critical(tb)

        taskImpl.complete()

def main():
    agent = Agent("config.txt")
    agent.wake()

if __name__ == "__main__":
    main()


