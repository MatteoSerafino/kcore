import tornado.web
import tornado.ioloop
import tornado.gen, tornado.concurrent

import concurrent.futures as con

from queue import Queue, Empty

import threading
import traceback

from CredMgmt import CredMgmt
from OnDemandAnalysis import OnDemandAnalysis

import ujson as json
import time


class Dispatcher(threading.Thread) :
    """
    Accept new work, dispatch to your processes for completion. Manage results
    and optimize flow.
    """
    
    def __init__(self) :
        # Superclass constructor
        threading.Thread.__init__(self)
        self.log_file = 'logs/dispatcher_errors.log'
        
        # Initialize our process pool
        self.ex = con.ProcessPoolExecutor()
        
        # Generates API objects with appropriate credentials for our queries
        self.creds = CredMgmt()
        
        # Init futures dictionary so we don't waste any work
        self.lock = threading.Lock()
        self.fdict = {}  # ALWAYS LOCK WHILE USING THIS DICT
        
        # Init query work queue so we can process queries smoothly
        self.queries = Queue()
        
        # Initialize workpool to watch ProcessPool futures
        # Future should return a (query, analysis) tuple
        self.workpool = []
    
    def log_error(self, exc, query=None) :
        with open(self.log_file, 'a+') as fout :
            fout.write('ERROR REPORT:\n')
            if query is not None :
                fout.write('QUERY: ' + str(query) + '\n')
            fout.write(time.strftime("%m-%d-%Y %H:%M:%S (UTC %z)"))
            fout.write('\n-----------\n')
            traceback.print_stack(file=fout)
            fout.write('\n-----------\n')
            traceback.print_exc(file=fout)
            fout.write('\n-----------\n\n\n\n\n')
    
    def run(self) :
        while True :
            
            # Dispatch everything in your queue
            while True :
                
                # Take a query from the queue
                try :
                    query = self.queries.get(block=True, timeout=0.1)
                except Empty :
                    break  # Nothing left in the queue
                
                # Dispatch query to a process
                job = self.ex.submit(OnDemandAnalysis, query, my_api=self.creds.giveAPI())
                
                # Pool the future from that process
                self.workpool.append(job)
            
            # Resolve any connection futures whose computation has completed
            self.checkForCompletedWork()
    
    def pushRequest(self, query, qfuture) :
        """
        Push a new query to this Dispatcher
        :param query:   Keyword from the GET request 
        :param qfuture:  Future attached to the GET request
        :return: 
        """
        
        # Push the query into your futures dictionary
        self.lock.acquire()
        if query not in self.fdict :
            self.fdict[query] = [qfuture]
            self.queries.put(query)  # Also need a single copy in work queue
        else :
            self.fdict[query].append(qfuture)
        self.lock.release()
    
    def checkForCompletedWork(self) :
        """
        Check if any of your workpool jobs are done, resolve appropriate futures
        :return: 
        """
        completed = []; this_query = None
        for job in self.workpool :
            try :
                if job.done() :
                    res = job.result()
                    query, analysis = res; this_query = query
                    self.resolveFutures(query, analysis)
                    completed.append(job)
                    this_query = None
            except Exception as exc :
                print('Error checking on analysis future!')
                self.log_error(exc, this_query)
                this_query = None
                completed.append(job)
        
        # Clean up
        for job in completed :
            self.workpool.remove(job)
    
    def resolveFutures(self, query, analysis) :
        """
        Resolve the futures attached to query in our fdict.
        :param query: 
        :param analysis: 
        :return: 
        """
        self.lock.acquire()
        if query in self.fdict :
            cx_futures = self.fdict.pop(query)
            for cx_future in cx_futures :
                cx_future.set_result(analysis)
        self.lock.release()


class BaseServer(tornado.web.RequestHandler) :
    def __init__(self, application, request, **kwargs) :
        super(BaseServer, self).__init__(application, request, **kwargs)
        print('A new Server was created')
        self.req = request
        self.req_str = str(request)
    
    def get(self) :
        self.write('Analytics server is ONLINE')


class DebugServer(BaseServer) :
    def get(self) :
        self.write('You requested:</br></br>')
        self.write(self.req_str)


class AnalyticsServer(BaseServer) :
    dispatcher = None
    query_fail_log = 'logs/failed_queries.log'
    promised_time = 60.0
    
    @tornado.gen.coroutine
    def get(self) :
        
        # Extract query
        query = self.get_query_argument('query')
        time_issued = time.strftime("%m-%d-%Y %H:%M:%S (UTC %z)")
        t_issued = time.time()
        
        # Make query safe
        translator = str.maketrans({key : None for key in '%()*,/:;<=>?[\\]^`{|}~'})
        q2 = sorted(query.split(' OR '))
        query = ' OR '.join(query.translate(translator).lower() for query in q2)

        if query == '' :
            self.write(json.dumps({
                'failure' : True,
                'error_code' : 1,
                'error_text' : 'Empty or invalid query'
            }))
            return
        
        # Log failed queries so we can dissect the mess
        black_box_recorder = tornado.ioloop.IOLoop.current().call_later(900.0, self.log_failed_query, query, time_issued)
        
        ## # Get the number of tweets to collect (count is in hundreds of tweets)
        ## try :
        ##    tweet_count = max(int(self.get_query_argument('power', default=6000)) / 100, 1)
        ## except :
        ##    tweet_count = 60
        
        cx_future = tornado.concurrent.Future()  # Prepare a future to wait on
        AnalyticsServer.dispatcher.pushRequest(query, cx_future)  # Pass future and query to Dispatcher thread
        analysis = yield cx_future  # Await results
        self.get_analysis(analysis)
        
        # Query didn't fail; cancel that report
        tornado.ioloop.IOLoop.current().remove_timeout(black_box_recorder)
        
        # Log if the query took longer than we promised
        total_time = time.time() - t_issued
        if total_time > AnalyticsServer.promised_time :
            with open(AnalyticsServer.query_fail_log, 'a+') as logfile :
                logfile.write('SLOW:   \"' + query + '\" issued at ' + time_issued + ' took %.2f minutes to resolve.\n' % (total_time/60.0))
        
        # All done!
        print('QUERY: \"' + query + '\" has been analyzed.')
    
    def get_analysis(self, analysis) :
        
        if analysis is not None :
            influencers = analysis[0]
            graph = analysis[1]
        else :
            influencers = None
            graph = None
        
        try :
            analysis = {'influencers' : influencers,
                        'graph' : graph}
            analysis['failure'] = False
        except :
            analysis = {'failure' : True}
        
        self.write(json.dumps(analysis))

    def log_failed_query(self, query, time_issued) :
        with open(AnalyticsServer.query_fail_log, 'a+') as logfile :
            logfile.write('FAILED: \"'+ query + '\" issued at ' + time_issued + ' has failed.\n')


def make_app() :
    return tornado.web.Application([
        (r"/", BaseServer),
        (r"/debug.*", DebugServer),
        (r"/influencers.json.*", AnalyticsServer),
    ])


if __name__ == "__main__" :
    SERVERPORT = 1137
    
    QUERY_FAIL_LOG = 'logs/failed_queries.log'
    QUERY_PROMISE_TIME = 60.0
    
    with open(QUERY_FAIL_LOG, 'a+') as qfl :
        qfl.write('\n\n\nANALYSIS SERVER LAUNCHED @ ' + time.strftime("%m-%d-%Y %H:%M:%S (UTC %z)") + '\n\n')
    
    
    print('Starting dispatcher thread...')
    michael = Dispatcher()                  # I didn't know what to name it.
    michael.start()
    AnalyticsServer.dispatcher = michael
    AnalyticsServer.query_fail_log = QUERY_FAIL_LOG
    AnalyticsServer.promised_time = QUERY_PROMISE_TIME
    print('Dispatcher thread started.')
    
    print('\nStarting Tornado webserver...')
    app = make_app()
    app.listen(SERVERPORT)
    tornado.ioloop.IOLoop.current().start()
    print('\nEnd of line.')