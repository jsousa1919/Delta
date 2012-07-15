#!/usr/bin/python

from optparse import OptionParser
import ConfigParser
from agent import Agent
from tasks import tasks
import pickle
import base64
import logging

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
        sql = "SELECT tid,task,after,reschedule,delta,args FROM tasks WHERE complete=0 ORDER BY after;"
        res = self.agent.db.query(sql)
        print '| {0:3} | {1:10} | {2:29} | {3:12} | {4:6} |'.format("TID", "Task", "After", "Reschedule?", "Delta")
        print "----------------------------------------------------------------------------"
        for row in res:
            print '| {0:3} | {1:10} | {2:29} | {3:12d} | {4:6} |'.format(row[0], row[1], row[2], row[3], row[4])
            args = ""
            if (len(row[5]) > 0):
                args = str(pickle.loads(base64.b64decode(row[5])))
            print '| args: {0:66} |'.format(args)
        print "----------------------------------------------------------------------------"
        self.agent.db.end()

    def drop_tasks(self):
        sql = "DROP TABLE tasks"
        print sql
        self.agent.db.query(sql)
    
    def add_task(self, task, repeat, delta, args=[]):
        logging.info("Adding Task: %s (repeat=%d, delta=%d)", task, repeat, delta)
        print "Adding Task: %s (repeat=%i, delta=%i)" % (task,repeat,delta)

        try:
            taskType = self.agent.tasks[task]
        except KeyError:
            logging.error("Unknown Task: %s", str(task))
            return

        taskImpl = taskType(self.agent, args)
        taskImpl.reschedules = bool(repeat)
        taskImpl.delta = delta 
        go = repeat is 1
        taskImpl.schedule(go_now = go)

def main():
    monitor = Monitor("config.txt")

    parser = OptionParser()
    parser.add_option("-t", "--tasks", dest="tasks", help="perform a TASK action", metavar="TASK")
    (options, args) = parser.parse_args()
    print "Options " + str(options)
    print "Args " + str(args)
    print ""
    if options.tasks:
        if options.tasks == "list":
            monitor.list_tasks()
        elif options.tasks == "drop":
            monitor.drop_tasks()
        elif options.tasks == "add":
            if len(args) < 1:
                print "Usage: --tasks add repeat? delta"
                print "Valid task types are: " + ", ".join(map(lambda x : x.taskName, tasks.TaskTypes))
            else:
                if (len(args) == 1):
                    monitor.add_task(args[0], 0, 0)
                elif (len(args) < 4):
                    monitor.add_task(args[0], int(args[1]), int(args[2])) 
                else:
                    monitor.add_task(args[0], int(args[1]), int(args[2]), args[3:]) 
                    
if __name__ == "__main__":
   main()
