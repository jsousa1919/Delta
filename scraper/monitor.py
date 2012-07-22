#!/usr/bin/python

from optparse import OptionParser
import ConfigParser
from agent import Agent
from tasks import tasks
import pickle
import base64
import logging
from datetime import date, datetime, timedelta

class Monitor:
    def __init__(self, configFile):
        config = ConfigParser.ConfigParser()
        config.readfp(open(configFile))
        self.agent = Agent(configFile)
    
    def list_tasks(self):
        logging.info("Listing Tasks")
        print "============================================================================"
        print "| TASKS                                                                    |"
        print "============================================================================"
        sql = "SELECT tid,task,after,delta,args FROM tasks WHERE complete=0 ORDER BY after;"
        res = self.agent.db.query(sql)
        print '| {0:3} | {1:10} | {2:29} | {4:6} |'.format("TID", "Task", "After", "Delta")
        print "----------------------------------------------------------------------------"
        for row in res:
            print '| {0:3} | {1:10} | {2:29} | {4:6} |'.format(row[0], row[1], row[2], row[3])
            args = ""
            if (len(row[4]) > 0):
                args = str(pickle.loads(base64.b64decode(row[4])))
            print '| args: {0:66} |'.format(args)
        print "----------------------------------------------------------------------------"
        self.agent.db.end()
    
    def add_task(self, task, delay, every, starting, args):
        try:
            taskType = self.agent.tasks[task]
            taskImpl = taskType(self.agent, args)
        except KeyError:
            logging.error("Unknown Task: %s", task)
            return

        if starting:
            delay = True
            try:
                starting = datetime.strptime(starting, "%Y-%m-%d %H:%M:%S")
            except:
                try:
                    starting = datetime.strptime(starting, "%Y-%m-%d")
                except:
                    starting = datetime.combine(date.today(), datetime.strptime(starting, "%H:%M:%S"))
                    if datetime.now() > starting:
                        starting += timedelta(days=1)
            taskImpl.starting = starting

        if every:
            log = "Adding Task: %s every %s, starting %s, delay is %s" % (task, every, starting.strftime("%Y-%m-%d %H:%M:%S") if starting else 'now', str(delay))
            logging.info(log)
            print log

            taskImpl.delta = every
            taskImpl.go_now = not delay
            taskImpl.schedule()

        else:
            log = "Adding Task: %s starting %s," % (task, starting.strftime("%Y-%m-%d %H:%M:%S") if starting else 'now')
            logging.info(log)
            print log

            taskImpl.go_now = True
            taskImpl.schedule()

def main():
    monitor = Monitor("config.txt")

    parser = OptionParser()
    parser.add_option("-l", "--list", dest="list", action="store_true", default=False, help="List the stored tasks")
    parser.add_option("-a", "--add", dest="task", default=None, help="Add a task", metavar="TASK")
    parser.add_option("-e", "--every", dest="every", nargs=2, default=None, help="Execute a task every <number of> (second|minute|hour|day|week|month), can be used with --starting, cannot be used with --repeat", metavar="TIMEUNIT")
    parser.add_option("-d", "--delay", dest="delay", action="store_true", default=False, help="Schedule the task to run only after its given delay, but not immediately.  Cannot be used with --starting")
    parser.add_option("-s", "--starting", dest="starting", default=None, help="Specify when this task should run first, string representing date and/or time in <YYYY-MM-DD hh:mm:ss> format", metavar="DATETIME")
    parser.add_option("-o", "--options", dest="options", default=None, help="Extra options to be passed to the task as a string", metavar="OPTIONS")
    (options, args) = parser.parse_args()

    if options.list:
        monitor.list_tasks()
    elif options.task:
        args = options.options.split(' ') if options.options else []
        monitor.add_task(options.task, options.delay, options.every, options.starting, args)
    else:
        parser.print_help()
                    
if __name__ == "__main__":
   main()
