"""
Created on Oct 21, 2015

@author: geofurb
"""

# rauth is our OAuth client for authenticating to Twitter API
# Tweet.py is for parsing Tweets and Twitter responses

from rauth import OAuth1Service
import requests
import api_secrets
import Tweet, config_sys
from TwArchive import TweetArchive
import time

class TwiAPIer(object):
    """
    Twitter API interface object
    """
    
    def __init__(self, api_keys=None):
        """
        Constructor
        """
        self.ARCHIVE_DIR = config_sys.DATAROOT
        self.MAX_QUERIES = 60           # Max requests to use in one search
        self.tcp_sess = None
        self.secrets = api_keys
    
    def getSecrets(self, user=True) :
        if self.secrets is None :
            if user :
                return api_secrets.oauth()
            else :
                return api_secrets.oauth2()
        else :
            return self.secrets
    
    def generateUserSession(self, gzip=True, verbose=False) :
        """
        Generate a user-specific OAuth session for Twitter 
        """
        # Load your API keys
        secrets = self.getSecrets()
        if verbose : print('\nSecrets loaded')
        
        # Create rauth service
        twitter = OAuth1Service(
                                consumer_key= secrets['consumer_key'],
                                consumer_secret= secrets['consumer_secret'],
                                name='twitter',
                                access_token_url='https://api.twitter.com/oauth/access_token',
                                authorize_url='https://api.twitter.com/oauth/authorize',
                                request_token_url='https://api.twitter.com/oauth/request_token',
                                base_url='https://api.twitter.com/1.1/')
        if verbose: print('oAuth service started')
        
        # Get a session
        session = twitter.get_session(
                    (secrets['token_key'],secrets['token_secret']))
        if verbose : print('oAuth session created')
        
        if gzip :
            session.headers.update({'Accept-Encoding' : 'gzip'})
        
        return session
    
    def generateAppSession(self, gzip=True, verbose=False) :
        """
        Generate an application-only session for Twitter 
        """
        # Load bearer token
        user_agent_string, bearer_token = api_secrets.oauth2()
        
        # Generate session
        sess = requests.Session()
        
        # Fill out headers with auth token
        headers = {
            'Host' : 'api.twitter.com',
            'User-Agent' : user_agent_string,
            'Authorization' : 'Bearer ' + bearer_token
        }
        
        if gzip :
            headers['Accept-Encoding'] = 'gzip'
        
        # Update headers in session object
        sess.headers.update(headers)
        
        return sess
    
    # Extract the tweets from a given query
    def getTweets(self, response) :
        """
        Get the tweets contained in a response.
        """
        
        # Parse the json data
        if response is not None :
            data = response.json()
        else :
            return []
        
        #Find the tweets if they exist
        tweets = []
        if 'statuses' in data \
        and data['statuses'] is not None :
            tweets = data['statuses']
        
        tweets = self.siftTweets(tweets)
        
        return tweets
    
    # Sort and log tweets
    def siftTweets(self, tweets) :
        """
        Sort tweets by ID and append to filename
        """
        
        '''
        # List for decorated tweets
        t_dec = []
        
        # Decorate tweets with their ID
        for tweet in tweets :
            t_dec.append((Tweet.getTweetID(tweet),tweet))
        
        # Sort decorated list by ID
        t_dec.sort(reverse=True)
        
        # Write tweets to file in order by ID
        ctr = 0
        for tweet in t_dec :
            tweets[ctr] = tweet[1]
            ctr += 1
        
        #self.buildTweeterDB(tweets)
        '''
        
        tweets = sorted(tweets, key=Tweet.getTweetID, reverse=True)
        return tweets

    # Retrieve screen names from user ID
    def idToScreenname(self, ids, verbose=False) :
        
        try :
            '''
            Returns a list of screennames corresponding to
            the queried ids. Max 100 values; additional are truncated.
             
            '''
            
            # Load your API keys
            secrets = self.getSecrets(user=True)
            if verbose : print('\nSecrets loaded')
            
            # Create rauth request
            twitter = OAuth1Service(
                                    consumer_key= secrets['consumer_key'],
                                    consumer_secret= secrets['consumer_secret'],
                                    name='twitter',
                                    access_token_url='https://api.twitter.com/oauth/access_token',
                                    authorize_url='https://api.twitter.com/oauth/authorize',
                                    request_token_url='https://api.twitter.com/oauth/request_token',
                                    base_url='https://api.twitter.com/1.1/')
            if verbose: print('oAuth service started')
            
            # Get a session
            session = twitter.get_session(
                        (secrets['token_key'],secrets['token_secret']))
            if verbose : print('oAuth session created')
            
            # Fill out query parameters
            query = ''; ctr = 0
            for uid in ids :
                query += str(uid) + ','
                ctr += 1
                if ctr == 100 : break   # Max 100 queries per request
            if len(query) > 0 :
                query = query[0:len(query)-1]
            params = {'user_id': query,
                      'include_entities' : 'false'}
                
            # Send the request and return results
            if verbose :
                print('\nSending id:screen_name request...')
                print('If this takes a long time, be sure to check availability:')
                print('https://dev.twitter.com/overview/status\n')
            TWITTER_URL = 'https://api.twitter.com/1.1/users/lookup.json'
            reply = session.post(TWITTER_URL,data=params)
            data = reply.json()
            phonebook = Tweet.getScreennames(data)
            screennames = []
            for uid in ids :
                if uid in phonebook :
                    screennames.append(phonebook[str(uid)])
                else :
                    screennames.append('@???????')
            return screennames
        except Exception :
            print('Error retrieving screen names.')
            raise

    # Retrieve screen names from user ID
    def resolveUsers(self, ids, verbose=False) :
    
        try :
            '''
            Returns information Twitter has on users corresponding to
            the queried ids. Max 100 values; additional are truncated.

            '''
        
            # Load your API keys
            secrets = self.getSecrets(user=True)
            if verbose : print('\nSecrets loaded')
        
            # Create rauth request
            twitter = OAuth1Service(
                consumer_key=secrets['consumer_key'],
                consumer_secret=secrets['consumer_secret'],
                name='twitter',
                access_token_url='https://api.twitter.com/oauth/access_token',
                authorize_url='https://api.twitter.com/oauth/authorize',
                request_token_url='https://api.twitter.com/oauth/request_token',
                base_url='https://api.twitter.com/1.1/')
            if verbose : print('oAuth service started')
        
            # Get a session
            session = twitter.get_session(
                (secrets['token_key'], secrets['token_secret']))
            if verbose : print('oAuth session created')
        
            # Fill out query parameters
            query = ''
            ctr = 0
            for uid in ids :
                query += str(uid) + ','
                ctr += 1
                if ctr == 100 : break  # Max 100 queries per request
            if len(query) > 0 :
                query = query[0 :len(query) - 1]
            params = {'user_id' : query,
                      'include_entities' : 'false'}
        
            # Send the request and return results
            if verbose :
                print('\nSending id:screen_name request...')
                print('If this takes a long time, be sure to check availability:')
                print('https://dev.twitter.com/overview/status\n')
            TWITTER_URL = 'https://api.twitter.com/1.1/users/lookup.json'
            reply = session.post(TWITTER_URL, data=params)
            data = reply.json()
            phonebook = {}
            for user in data :
                if 'id' in user and user['id'] is not None and str(user['id']) in ids :
                    phonebook[str(user['id'])] = user
                    phonebook[str(user['id'])]['id'] = str(user['id'])
                
            for uid in ids :
                if uid not in phonebook :
                    phonebook[uid] = {'id' : uid, 'screen_name' : '@???????', 'followers_count' : 0}
                
            return phonebook
        
        
        except Exception :
            print('Error resolving users.')
            raise

    """
    API Tools for the new TwArchive storage utility
    """
    def connect(self, user=True, verbose=False) :
        """
        Connect to Twitter API
        :return: rauth session object for Twitter Search API
        """
        
        '''
        # Load your API keys
        secrets = api_secrets.oauth()
        if verbose : print('\nSecrets loaded')

        # Create rauth request
        twitter = OAuth1Service(
                                consumer_key= secrets['consumer_key'],
                                consumer_secret= secrets['consumer_secret'],
                                name='twitter',
                                access_token_url='https://api.twitter.com/oauth/access_token',
                                authorize_url='https://api.twitter.com/oauth/authorize',
                                request_token_url='https://api.twitter.com/oauth/request_token',
                                base_url='https://api.twitter.com/1.1/')
        if verbose: print('oAuth service started')
        
        # Get a session
        session = twitter.get_session(
                    (secrets['token_key'],secrets['token_secret']))
        if verbose : print('oAuth session created')
        '''
        
        if user :
            session = self.generateUserSession(verbose=verbose)
        else :
            session = self.generateAppSession(verbose=verbose)

        # If you don't have a session yet, this is your default
        if self.tcp_sess is None :
            self.tcp_sess = session

        return session

    def disconnect(self, session=None) :
        """
        Disconnect the Twitter session specified. If no session is provided, the default
        session will be ended, if it exists.
        """
        if session is None :
            if self.tcp_sess is not None :
                self.tcp_sess.close()
                self.tcp_sess = None
        else :
            session.close()

    def searchQuery(self, query, bounds, lang='en', filters=None, session=None, verbose=True) :
        """
        Make a query to the Twitter Search API and return the response
        """

        # Parse bounds
        since_id = bounds[0]; max_id = bounds[1]

        # Fill out query parameters
        params = {'q': query,
                  'result_type' : 'recent',
                  'lang' : lang,
                  'count': '100'}
        self.applyFilters(params, filters)
        if max_id is not None :
            params['max_id'] = max_id
        if since_id is not None:
            params['since_id'] = since_id

        # Send the request and return results
        if verbose :
            print('\nSending search request...')
            print('If this takes a long time, be sure to check availability:')
            print('https://dev.twitter.com/overview/status\n')
        TWITTER_URL = 'https://api.twitter.com/1.1/search/tweets.json'

        # Send the request to Twitter and give the result
        if session is None :

            # If no session specified, use your default
            if self.tcp_sess is None :
                return self.connect().get(TWITTER_URL, params=params)
            # If your default isn't running, start it
            else :
                return self.tcp_sess.get(TWITTER_URL, params=params)

        # Use the specified session
        else :
            return session.get(TWITTER_URL, params=params)

    def getUserTimeline(self, user, screenname=True,bounds=(None, None), lang='en', filters=None, session=None, verbose=True) :
        """
        Make a query to the Twitter GET statuses/user_timeline endpoint and return the response
        """
    
        # Parse bounds
        since_id = bounds[0]
        max_id = bounds[1]
    
        # Fill out query parameters
        params = {'result_type' : 'recent',
                  'lang' : lang,
                  'count' : '200'}
        if screenname :
            params['screen_name'] = user
        else :
            params['user_id'] = user
        self.applyFilters(params, filters)
        if max_id is not None :
            params['max_id'] = max_id
        if since_id is not None :
            params['since_id'] = since_id
    
        # Send the request and return results
        if verbose :
            print('\nSending search request...')
            print('If this takes a long time, be sure to check availability:')
            print('https://dev.twitter.com/overview/status\n')
        TWITTER_URL = 'https://api.twitter.com/1.1/statuses/user_timeline.json'
    
        # Send the request to Twitter and give the result
        if session is None :
        
            # If no session specified, use your default
            if self.tcp_sess is None :
                return self.connect().get(TWITTER_URL, params=params)
            # If your default isn't running, start it
            else :
                return self.tcp_sess.get(TWITTER_URL, params=params)
    
        # Use the specified session
        else :
            return session.get(TWITTER_URL, params=params)

    def applyFilters(self, params, filters) :
        """
        Apply the given filters to your parameters
        """
        if filters is None : return
        for key in filters :
            params[key] = filters[key]

    def searchQuerySafe(self, query, bounds, lang='en', filters=None, session=None, retry_on_rate_limit=False, verbose=True) :
        """
        Wrapper for sendQuery to handle exceptions and rate-limiting by Twitter API.
        """
        # Watch network errors and wait if timed out
        failhard = False; ctr = 0
        WAIT_INTERVAL = 60; MAX_TRIES = 900 / WAIT_INTERVAL
        while not failhard :

            # Make requests until one succeeds or we surrender
            try :
                
                # Query until we get valid JSON
                brokentweetctr = 0
                while True :
                    reply = self.searchQuery(query, bounds, lang, filters=filters, session=session, verbose=False)
                    try :
                        data = reply.json()
                    except ValueError :
                        if brokentweetctr < 3 :
                            brokentweetctr += 1
                            continue
                        else :
                            print('WARNING: Mangled tweet in desired range.')
                            raise
                    break
                    

                # If we're being rate limited
                if reply.status_code == 429 or 'statuses' not in data :
                    if verbose :
                        print('HTTP Code : ' + str(reply.status_code) + ' - Rate limited!')
                    ctr += 1

                    # If we haven't waited a full time period, keep waiting
                    if ctr < MAX_TRIES and retry_on_rate_limit :
                        if verbose : print('Attempt ' + str(ctr+1) + ' in ' + str(WAIT_INTERVAL) + ' seconds...\n')
                        time.sleep(WAIT_INTERVAL)
                    else :
                        failhard = True

                # Not rate limited; not handling other HTTP errors yet
                else :
                    return reply, False
            
            except ConnectionError :
                if verbose : print('Connection terminated: Reconnecting...')
                self.disconnect(session)
                self.connect()
                if verbose : print('Reconnection successful.')
                continue
            
            # Unmitigated network or data error
            except Exception :
                if verbose : print('Exception in search!')
                raise
            
        # Waited a full time period; give up.
        if verbose and retry_on_rate_limit : print('Failed hard! Not getting new rate-limiting periods!')
        return None, failhard

    def archiveSearch(self, arx, req_limit=0, wait_on_rate_limit=True, exhaust_on_ratelimit=False, auto_exhaust=False) :
        """

        Search for a query and journal the results in your Tweet archive.
        
        :param arx: Your Tweet archive
        :param req_limit:
        :param wait_on_rate_limit:
        :param exhaust_on_ratelimit:
        :param auto_exhaust:
        :return:
        """
        
        # You can't take unlimited queries AND keep waiting for new ones
        if req_limit == 0 : wait_on_rate_limit = False
        
        # Verify archive index provided
        if not isinstance(arx, TweetArchive) :
            print('Invalid arx specified!\n')
        else :
            print('Archive index validated.\n')
        
        
        query = arx['query']
        filters = arx['filters']

        # Pick the language for your search
        if filters is not None and 'lang' in filters :
            lang = filters['lang']
            filters.pop('lang', None)
        else :
            lang = 'en'
        
        
        bounds = arx.getBounds()

        tweets = []
        exhausted = False
        rate_limited = False

        # Say hi to Twitter
        if self.tcp_sess is None :
            self.connect()
            print('Session connected.')

        # Catch up on things, see what's new
        try :
            ctr = 0; exhaustion = 0
            while ctr < req_limit or req_limit == 0 :

                # Send Twitter a query
                resp, rate_limited = self.searchQuerySafe(query, bounds, lang=lang, filters=filters, retry_on_rate_limit=wait_on_rate_limit)
                
                # If you got a bad answer or gave up waiting for rate-limit
                if resp is None :
                    if wait_on_rate_limit : print('No response! Breaking collection.')
                    break

                # We've used a request
                ctr += 1

                # Parse the reply
                resp_code = resp.status_code
                twpart = self.siftTweets(self.getTweets(resp))
                
                # Rectify bounds so we don't collect the same set of tweets over and over again
                if len(twpart) > 0 :
                    bounds = (bounds[0],Tweet.getTweetID(twpart[-1])-1,bounds[2],Tweet.getDate(twpart[-1]))
                
                # If we got tweets in response
                if resp_code == 200 :

                    # Add these to your tweets
                    tweets += twpart

                    # If you didn't get too many
                    if len(twpart) < 10 :
                        
                        # Start to worry we're running out of tweets
                        exhaustion += 1
                        
                        # We're done collecting; nothing more to find
                        if exhaustion == 3 or len(twpart) == 0:
                            exhausted = True
                            break

                    # We're not running out of tweets
                    else :
                        exhaustion = 0


        # If the discussion gets out of hand, end it
        except Exception :
            self.disconnect()
            raise
        
        # We might want to ignore earlier tweets if we know we can't keep up
        if exhaust_on_ratelimit :
            effective_exhausted = exhausted or rate_limited
        else :
            effective_exhausted = exhausted
        effective_exhausted = effective_exhausted or auto_exhaust
        
        # Archive what we collected
        arx.appendTweets(tweets, effective_exhausted)
        return exhausted, rate_limited