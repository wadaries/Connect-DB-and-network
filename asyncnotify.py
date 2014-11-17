from flow import StaticFlowPusher

class AsyncNotify():
    
    def __init__(self, dsn, ip):
        '''Connect to PostgreSQL and Floodlight controller. This class requires the psycopg2 driver.'''
        import psycopg2
        self.conn = psycopg2.connect(dsn)
        self.conn.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
        self.curs = self.conn.cursor()
        self.__listening = False
        self.controller = StaticFlowPusher(ip)
    
    def __listen(self):
        from select import select
        if self.__listening:
            return 'already listening!'
        else:
            self.__listening= True
            while self.__listening:
                # Asyncronously listen to the channel
                if select([self.conn],[],[],60)!=([],[],[]):
                    # Get data from channel if it's avaiable
                    self.conn.poll()
                    if self.conn.notifies:
                        notify = self.conn.notifies.pop()
                        #print "Got Notify:", notify.pid, notify.channel, notify.payload
                        self.gotNotify(notify.pid, notify.channel, notify.payload)

    
    def addNotify(self, notify):
        '''Subscribe to a PostgreSQL NOTIFY'''
        sql = "LISTEN %s" % notify
        self.curs.execute(sql)
    
    def removeNotify(self, notify):
        '''Unsubscribe a PostgreSQL LISTEN'''
        sql = "UNLISTEN %s" % notify
        self.curs.execute(sql)
    
    def stop(self):
        '''Call to stop the listen thread'''
        self.__listening = False
    
    def run(self):
        '''Start listening in a thread and return that as a deferred
           For details about deferred objects, refer to http://twistedmatrix.com/documents/12.1.0/core/howto/gendefer.html
        '''
        from twisted.internet import threads
        return threads.deferToThread(self.__listen)
    
    def gotNotify(self, pid, channel, data):
        ''' This method is overwritten in translator.py'''
        pass
