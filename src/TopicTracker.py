import traceback

import requests

from TwAPIer import TwiAPIer
from TwArchive import TweetArchive

import time


class RatelimitError(requests.exceptions.HTTPError) :
    """Twitter wants us to chill out and wait a little while before trying to reconnect."""

class TopicTracker(object) :
    def __init__(self, fout='tweetstream_JSON.taj') :
        '''
        Twitter Streaming API collection entity. Writes all valid tweets collected to fout in JSON format,
        one-per-line. Invoke collect() on your TweetStreamCollector with your desired parameters & filter_params to
        begin collection. Collection can be stopped with a keyboard interrupt or by terminating the application.
        :param fout: 
        '''
        # Error flags
        self.tcp_err_ctr = 0
        self.http_err_ctr = 0
        self.http_eyc_ctr = 0
        self.other_errors = 0
        # Output files
        self.fout = fout
        self.error_logs = 'logs/topic_tracking_errors.log'
    
    # Error handling for stream connection
    def reset_errors(self) :
        self.tcp_err_ctr = 0
        self.http_err_ctr = 0
        self.http_eyc_ctr = 0
        self.other_errors = 0
    
    def signal_TCP_err(self) :
        self.tcp_err_ctr += 1
        err_count = min([self.tcp_err_ctr, 64])
        with open(self.error_logs, 'a+') as error_log :
            error_log.write('REST TIME: ' + str((0.25 * err_count)) + ' seconds for TCP.\n')
        time.sleep(0.25 * err_count)
    
    def signal_HTTP_err(self) :
        self.http_err_ctr += 1
        err_count = min([self.http_err_ctr, 7])
        with open(self.error_logs, 'a+') as error_log :
            error_log.write('REST TIME: ' + str((5.0 * 2 ** (err_count - 1))) + ' seconds for HTTP.\n')
        time.sleep(5.0 * 2 ** (err_count - 1))
    
    def signal_ratelimit_err(self) :
        self.http_eyc_ctr += 1
        with open(self.error_logs, 'a+') as error_log :
            error_log.write('REST TIME: ' + str((60.0 * 2 ** (self.http_eyc_ctr - 1))) + ' seconds for ratelimit.\n')
        time.sleep(60.0 * 2 ** (self.http_eyc_ctr - 1))
    
    def signal_other_error(self) :
        self.other_errors += 1
        err_count = min([self.tcp_err_ctr, 10])
        with open(self.error_logs, 'a+') as error_log :
            error_log.write('REST TIME: ' + str((5.0 * err_count)) + ' seconds for unexpected error.\n')
        time.sleep((5.0 * err_count))
    
    def log_error(self, exc) :
        with open(self.error_logs, 'a+') as error_log :
            error_log.write('EXCEPTION: ' + str(exc) + '\n')
            error_log.write(time.strftime("%m-%d-%Y %H:%M:%S (UTC %z)"))
            error_log.write('\n-----------\n')
            traceback.print_exc(file=error_log)
            error_log.write('\n\n\n')
    
    
    # Start stream connection
    def collect(self, query_list, archive_dir=None, sample_evenness=float('inf'),
                lang='en') :
        
        # Start with no errors
        self.reset_errors()
        connection_has_succeeded = False
        
        # Collect indefinitely
        while True :
            
            ############################
            # Exception Handling Block #
            ############################
            try :
                
                # Connect & collect
                print('Initializing collection...')
                
                #########################
                # REST Collection Block #
                #########################
                
                try :
                    ctr  = 0
                    while True :
                        ctr += 1
                        print('Collecting chunk ' + str(ctr))
                        self.collectTopics(query_list, archive_dir, sample_evenness, 
                                           lang=lang)
                        self.reset_errors()
                        connection_has_succeeded = True
                
                # User kills the stream program
                except (KeyboardInterrupt, SystemExit) :  # Tidy up if murdered
                    print('End of line.')
                    break
                    
                    # Catch & wait on HTTP/network errors
            # We've been trying to collect data from Twitter's servers too quickly
            except RatelimitError :
                self.signal_ratelimit_err()
                continue
            # Trouble communicating with the API
            except requests.exceptions.HTTPError as exc :  # HTTP Errors
                self.log_error(exc)
                if connection_has_succeeded :  # Speed bump errors
                    self.signal_HTTP_err()  # Wait before continuing
                    continue
                else :
                    raise
            # Trouble with our connection to Twitter's servers
            except ConnectionError :
                self.signal_TCP_err()
                continue
            # Unexpected error
            except Exception as exc :  # Catch & log unexpected errors
                print('Unhandled exception in Streaming API connection block!')
                self.log_error(exc)
                if connection_has_succeeded :
                    self.signal_other_error()
                    continue
                else :
                    raise

    def collectTopics(self, query_list, archive_dir=None, sample_evenness=1.0,
                      lang='en') :
        '''
        Collect an even sampling of tweets across several topics, up to all available Tweets.

        NOTE: This function uses application-only auth and has no user context. It uses the credentials returned by
        your api_secrets.py oauth2() function. Ensure that user_idx in api_secrets.py is adjusted to return the bearer
        token that you wish to use!!

        :param query_list: List of strings specifying search queries
        :param archive_dir: Override your default archive_dir if desired
        :param sample_evenness: Increase for shorter collection intervals on each topic. (Warning: If set too high, this
        may cause you to undershoot your rate-limit because there is a small time-overhead in changing query topics.)
        :return:
        '''
    
        # Initialization
        if sample_evenness < 1.0 : sample_evenness = 1.0
        if sample_evenness > 450 / len(query_list) : sample_evenness = 450.0 / float(len(query_list))
        MAX_QUERIES = 450 / (len(query_list) * sample_evenness)
        time_alloc = 15.0 * 60.0 / (float(len(query_list)) * sample_evenness)
    
        # Connect to Twitter API
        api = TwiAPIer()
    
        # Iterate through keywords
        for query in query_list :
        
            # Begin cycle
            qstart = time.time()
            print('\n\n\nProcessing: ' + query + '\nStart time: ' + str(qstart))
        
            # Connect to API
            try :
                api.connect(user=False)
            except :
                print('Error connecting to Twitter API!')
                raise
        
            # Initialize archive
            try :
                ARX = TweetArchive(query, archive_dir=archive_dir)
            except :
                print('Exception during archive initialization!')
                raise
        
            # Collect new tweets and append them to our archive
            try :
                DONE_READING, RATE_LIMITED = api.archiveSearch(ARX, MAX_QUERIES, wait_on_rate_limit=True,
                                                               auto_exhaust=True, lang=lang)
                if RATE_LIMITED :
                    print('Warning! Rate limit reached. Verify that you aren\'t collecting too quickly.')
            except :
                print('Exception during archive search!')
                raise
        
            # Disconnect API session
            try :
                api.disconnect()
            except :
                print('Error disconnecting from Twitter API!')
                api.tcp_sess = None
        
            # Preserve even query spacing, don't exceed rate-limit
            qfin = time.time()
            print('End time: ' + str(qfin))
            t_interval = qfin - qstart
            if t_interval < time_alloc :
                print('Finished early. Resting for ' + str(time_alloc - t_interval) + ' seconds.')
                time.sleep(time_alloc - t_interval)

# use  topics_automation/run_topictracker.py instead
#if __name__ == "__main__" :
#    
#    queries = [
#        'trump OR donaldtrump OR realdonaldtrump',
#        'hillary OR clinton OR hillaryclinton'
#    ]
#    
#    tw_tracker = TopicTracker()
#    tw_tracker.collect(queries, '/media/geofurb/trump/archives_noveau/')