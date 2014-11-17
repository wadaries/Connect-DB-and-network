from asyncnotify import AsyncNotify
from twisted.internet import reactor

dbname = 'testdb'
host = 'localhost'
user = 'fan'
password = 'password'

dsn = 'dbname=%s host=%s user=%s password=%s' % (dbname, host, user, password)
controllerIP = '192.17.193.222'

def errorHandler(error):
    print str(error)
    notifier.stop()
    reactor.stop()

def shutdown(notifier):
    print 'Shutting down the reactor'
    reactor.stop()

def tableUpdated(notify, pid):
    tablename, op = notify.split('_')
    print '%s just occured on %s from process ID %s' % (op, tablename, pid)

class myAsyncNotify(AsyncNotify):

    def gotNotify(self, pid, notify, data):
        if notify == 'quit':
            print 'Stopping the listener thread.'
            self.stop()
        elif notify.split('_')[0]  in ('table3', 'table4', 'table5'):
            tableUpdated(notify, pid)
            print "Table got notification '%s' from process id '%s which has data %s'" % (notify, pid, data)
           
            flow1 = {
                        'switch':"00:00:00:00:00:00:00:01",
                        "name":"flow-mod-1",
                        "cookie":"0",
                        "priority":"32768",
                        "ingress-port":"1",
                        "active":"true",
                        "actions":"output=flood"
            }
                    
            flow2 ={
                        'switch':"00:00:00:00:00:00:00:01",
                        "name":"flow-mod-2",
                        "cookie":"0",
                        "priority":"32768",
                        "ingress-port":"2",
                        "active":"true",
                        "actions":"output=flood"
            }
                    
            self.controller.set(flow1);
            self.controller.set(flow2);


        else:
            print "got notification '%s' from process id '%s which has data %s'" % (notify, pid, data)

notifier = myAsyncNotify(dsn, controllerIP)

# Start listening for subscribed notifications in a deferred thread.
listener = notifier.run()

# What to do when the AsyncNotify stop method is called to
listener.addCallback(shutdown)
listener.addErrback(errorHandler)

# Call the gotNotify method when any of the following notifies are detected.
notifier.addNotify('test3')
notifier.addNotify('test4')
notifier.addNotify('table3_insert')
notifier.addNotify('table3_update')
notifier.addNotify('table3_delete')
notifier.addNotify('table4_insert')
notifier.addNotify('table4_update')
notifier.addNotify('table4_delete')
notifier.addNotify('quit')

notifier.addNotify('test5')
notifier.addNotify('table5_insert')
notifier.addNotify('table5_update')
notifier.addNotify('table5_delete')


# Unsubscribe from one
#reactor.callLater(15, notifier.removeNotify, 'test4')

reactor.run()


