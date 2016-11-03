'''
Created on Feb 24, 2016

@author: geofurb
'''


import os
import ujson as json
import config_sys
from networkx.classes.digraph import DiGraph
from networkx.readwrite import json_graph
import time
from uuid import uuid4
from datetime import datetime
from dateutil.parser import parse as parsedate
import pytz

class EmptyGraph(Exception):
    pass

class BeforeFirstTweet(Exception):
    pass

class AfterLastTweet(Exception):
    pass

def enildaer(filename, buf_size=8388608):
    '''
    A generator that returns the lines of a file in reverse order
    '''
    fh = filename
    segment = None
    offset = 0
    file_size = fh.seek(0, os.SEEK_END)
    total_size = remaining_size = fh.tell()
    while remaining_size > 0:
        offset = min(total_size, offset + buf_size)
        fh.seek(file_size - offset)
        buffer = fh.read(min(remaining_size, buf_size))
        remaining_size -= buf_size
        lines = buffer.split('\n')
        # the first line of the buffer is probably not a complete line so
        # we'll save it and append it to the last line of the next buffer
        # we read
        if segment is not None:
            # if the previous chunk starts right from the beginning of line
            # do not concact the segment to the last line of new chunk
            # instead, yield the segment first 
            if buffer[-1] is not '\n':
                lines[-1] += segment
            else:
                yield segment
        segment = lines[0]
        for index in range(len(lines) - 1, 0, -1):
            if len(lines[index]):
                yield lines[index]
    # Don't yield None if the file was empty
    if segment is not None:
        yield segment

def addEdgeWithTweetID(G, u, v, tweet_id):
    """ add directed edge u,v to graph G and add tweet_id to the edge attribute
        or update existing edge by adding tweet_id to the set of tweet_ids  """
    
    if G.has_edge(u,v):
        G.edge[u][v]['tweet_id'].add(tweet_id)
    else:
        G.add_edge(u, v, {'tweet_id': {tweet_id}})

def addEdgesFromWithTweetID(G, H):
    """ add edges from H to graph G and update the set of tweet_ids of the 
        merged edges """
    
    for u, v, id_set in H.edges_iter(data='tweet_id'):
        if G.has_edge(u,v):
            G.edge[u][v]['tweet_id'].update(id_set)
        else:
            G.add_edge(u, v, {'tweet_id': id_set})

class TweetArchive(object):
    '''
    Twitter file archiver
    
    USAGE INSTRUCTIONS:
    1. Create an archive to open its index. Pass in your filter dictionary!!
    2. Call getBounds() to get the range to collect
    3. Collect tweets
    4. Commit tweets to the archive using appendTweets(tweets, exhausted)
    5. Call getBounds() to get the next range to collect
    etc.
    
    The 'exhausted' flag tells the archive if you've run out of tweets in
    the bounds it gave you. This is NOT an indicator that you've been
    rate-limited!!
    
    While it makes sense to collect tweets until 'exhausted' every time, users
    may be unable to do so due to real constraints. (e.g. rate-limiting)
    
    JSON File Types:
    .arx - Archive Index, JSON format
    .taj - Tweet Archive JSON, one JSON tweet object per line
    .jnld - JSON Node Link Data, JSON formatted NetworkX graph
    '''

    def __init__(self, query, filters=None, archive_dir=None,
                 tweet_format='Twitter') :
        '''
        Constructor:
        Load the archive file specified by the given query and optional filters
        '''
		
        self.SIZE_LIMIT = 400 * 1024 * 1024
        self.arx = None

        if tweet_format == 'Gnip' or query[:4] == 'gnip':
            self.Tweet = __import__('GnipTweet')
        else:
            self.Tweet = __import__('Tweet')
        
        
        if archive_dir is None :
            archive_dir = config_sys.ARCHIVE_DIR

        # Set your archive directory
        if archive_dir[-1] != '/' :
            archive_dir += '/'
            
        self.ARCHIVE_DIR = archive_dir

        # Make the directory if it doesn't exist
        try:
            os.makedirs(self.ARCHIVE_DIR + query + '/', exist_ok=True)
        except (FileNotFoundError, FileExistsError):
            pass

        
        # Add filters to the name of your index file so multiple indexes can coexist
        filter_string = self.getFilterString(filters)
        
        # Name your index file
        index_file = self.ARCHIVE_DIR + query + '/index' + filter_string + '.arx'
        
        # Load the index file if it exists, else create it
        with open(index_file, 'a+') as fopen :
            fopen.seek(0)
            data = fopen.read()
            if len(data) > 0 :
                arx = json.loads(data)
                self.arx = arx
            else :
                arx = {'query' : query,
                       'filters' : filters,
                       'unfinished' : None,
                       'finished' : []
                }
                self.arx = arx
                fopen.write(json.dumps(self.arx, sort_keys=True, indent=4))

    def __getitem__(self, item) :
        if self.arx is not None and item in self.arx :
            return self.arx[item]
        else :
            return None
    
    def __str__(self) :
        if self.arx is not None :
            return self.arx['query']
        else :
            return '' 
            
    def getTweetFormat(self):
        return self.Tweet.tweet_format

    def commitArchive(self) :
        '''
        Overwrite the appropriate arx file with your new archive
        '''
        
        query = self.arx['query']
        filters = self.arx['filters']
        
        # Make the directory if it doesn't exist
        try:
            os.makedirs(self.ARCHIVE_DIR + query + '/', exist_ok=True)
        except (FileNotFoundError, FileExistsError):
            pass
        
        # Add filters to the name of your index file so multiple indexes can coexist
        filter_string = self.getFilterString(filters)
        
        # Name your index file
        index_file = self.ARCHIVE_DIR + query + '/index' + filter_string + '.arx'
        
        # Dump your archive index to file
        with open(index_file, 'w+') as fopen :
            # Write it to be relatively human-readable
            fopen.write(json.dumps(self.arx, sort_keys=True, indent=4))
    
    def getFilterString(self, filters) :
        '''
        Filename-safe representation of our filters
        '''
        filter_string = ''
        if filters is not None and 'lang' in filters :
            filter_string += filters['-lang']
        return filter_string
        
    def getBounds(self) :
        '''
        Returns a 4-tuple of min_bound, max_bound, min_timestamp, max_timestamp
        for the range this archive wants to collect next.
        '''
        bounds = [None, None]
        timestamps = [None, None]
        
        # Min bounds are the max bounds of your finished data
        finlen = len(self.arx['finished'])
        if finlen > 0 :
            bounds[0] = self.arx['finished'][finlen - 1][2]
            timestamps[0] = self.arx['finished'][finlen - 1][4]
        
        # Max bounds are the min bounds of your unfinished data
        if self.arx['unfinished'] is not None :
            bounds[1] = self.arx['unfinished'][1]
            timestamps = self.arx['unfinished'][3]
        
        return (bounds[0], bounds[1], timestamps[0], timestamps[1])
    
    def appendTweets(self, tweets, exhausted=False, verbose=True, autogen_graphs=True) :
        '''
        Write the given tweets into the gap you wish to fill in your archive.
        This assumes the tweets belong into that gap! Use responsibly.
        
        The 'exhausted' flag should be set to True if no more tweets are
        available in this range. (We've 'exhausted' this well, have to move on.)
        '''
        
        # If you didn't get any tweets, you don't have anything to do
        if len(tweets) == 0 and not exhausted : return
        
        # Read bounds
        (min_bound, max_bound, min_time, max_time) = self.getBounds()
        
        # Prepare variables
        dirpath = self.ARCHIVE_DIR + self.arx['query'] + '/'
        
        tweets_recent = max_bound is None
        
        # "Unfinished" archive file for temporary tweet storage
        unfin_exists = self.arx['unfinished'] is not None
        if unfin_exists :
            unfin_file = dirpath + self.arx['unfinished'][0]
        
        # Latest "Finished" file, for permanent tweet storage
        fin_len = len(self.arx['finished'])
        fin_exists = fin_len > 0
        if fin_exists :
            fin_file = dirpath + self.arx['finished'][fin_len - 1][0]
            fin_full = os.path.getsize(fin_file) > self.SIZE_LIMIT
        else :
            # No more room in the "finished" file if it doesn't exist!!
            fin_full = True
        
        
        # We can't go further back on this subject
        if exhausted :
            
            # If our latest "finished" file is full
            if fin_full :
                # Start new "finished" file, register it in the arx
                fin_file_name = 'tweets-' + str(uuid4()) + '.taj'
                must_create_fin = True
                fin_file = dirpath + fin_file_name
            else :
                must_create_fin = False
            
            # Write the new tweets to the "finished" file
            if verbose : print('Writing to finished file...')
            with open(fin_file, 'a+') as fopen :

                if len(tweets) != 0 :
                    # Look for our new bounds
                    max_bound = None; max_time = None
                    for tweet in tweets :
                        if max_bound is None : max_bound = self.Tweet.getTweetID(tweet)
                        if max_time  is None : max_time  = self.Tweet.getDate(tweet)
                        if max_bound is not None and max_time is not None :
                            break
                            
                    # We must write tweets old-to-new
                    tweet_strings = []; buffer_len = 100; ctr = 0
                    for n, tweet in enumerate(reversed(tweets)) :
                        
                        ctr += 1
                        
                        # Look for our min bounds
                        if min_bound is None : min_bound = self.Tweet.getTweetID(tweet)
                        if min_time  is None : min_time  = self.Tweet.getDate(tweet)
                        
                        # Copy tweet to our write buffer
                        tweet_strings.append(json.dumps(tweet) + '\n')
                        
                        if n % 4000 == 0 and n > 0 :
                            print('Processed ' + str(n) + ' tweets.')
                        
                        if ctr > buffer_len : 
                            # Write our buffer to the "finished" file
                            fopen.write(''.join(tweet_strings))
                            tweet_strings = []
                            ctr = 0
                            
                    # Append remaining tweets
                    fopen.write(''.join(tweet_strings))
                    tweet_strings = []
                
                # Update the data file's contents in your archive index
                if must_create_fin :
                    
                    # Generate graphs for the old finished file
                    if len(self.arx['finished']) > 0 :
                        self.loadGraphForTAJ(self.arx['query'], self.arx['finished'][-1][0])
                    
                    # Add new data file to index
                    self.addNewDataFileReg(fin_file_name, min_bound, min_time)
                    
                self.updateLastDataFileReg(min_bound, min_time, max_bound, max_time, len(tweets))
                
            # You've exhausted your tweet supply in this gap;
            # time to finish the unfinished file data if it exists.
            if not tweets_recent :
                self.finalizeUnfinishedData()
        
        # We must have more tweets in this interval to collect after these
        else :
            
            # If these are the latest tweets, we're starting a new file
            if tweets_recent :
                
                # Create "unfinished" file
                unfin_file_name = 'new-tweets-' + str(uuid4()) + '.taj'
                must_create_unfin = True
                unfin_file = dirpath + unfin_file_name
            else :
                must_create_unfin = False
                
            # Write the new tweets to the "unfinished" file
            if verbose : print('Writing to unfinished file...')
            with open(unfin_file, 'a+') as fopen :
                
                # Look for our new bounds
                min_bound = None; min_time = None
                for tweet in reversed(tweets) :
                    if min_bound is None : min_bound = self.Tweet.getTweetID(tweet)
                    if min_time  is None : min_time  = self.Tweet.getDate(tweet)
                    if min_bound is not None and min_time is not None :
                        break
                
                # Write our tweets new-to-old
                tweet_strings = []
                for tweet in tweets :
                    
                    # Look for max bounds
                    if max_bound is None : max_bound = self.Tweet.getTweetID(tweet)
                    if max_time  is None : max_time  = self.Tweet.getDate(tweet)
                    
                    # Copy tweet to our write buffer
                    tweet_strings.append(json.dumps(tweet) + '\n')
                    
                # Write our buffer to the "unfinished" file
                fopen.write(''.join(tweet_strings))
                tweet_strings = []
                
            # Update the data file's contents in your archive index
            if must_create_unfin :
                self.addUnfinishedFileReg(unfin_file_name, max_bound, max_time)
            self.updateUnfinishedFileReg(min_bound, min_time, len(tweets))
            
        # Commit changes to the archive index
        self.commitArchive()
        if verbose : print('Writing complete.')
        
    def addNewDataFileReg(self, name, min_bound, min_time) :
        '''
        Register a new data file in your arx
        '''
        self.arx['finished'].append((name, min_bound, min_bound, min_time, min_time, 0))
    
    def updateLastDataFileReg(self, min_bound, min_time, max_bound, max_time, num_new_tweets) :
        '''
        Update the bounds on the last data file in your arx
        '''
        ind = len(self.arx['finished'])-1
        name = self.arx['finished'][ind][0]
        
        my_min_bound = self.arx['finished'][ind][1]
        if my_min_bound is None :
            my_min_bound = min_bound
        my_min_time = self.arx['finished'][ind][3]
        if my_min_time is None :
            my_min_time = min_time
            
        if max_bound is None :
            max_bound = self.arx['finished'][ind][2]
        if max_time is None :
            max_time = self.arx['finished'][ind][4]
            
        num_tweets = num_new_tweets + self.arx['finished'][ind][5]
        self.arx['finished'][ind] = (name, my_min_bound, max_bound, my_min_time, max_time, num_tweets)
    
    def addUnfinishedFileReg(self, name, max_bound, max_time) :
        '''
        Register a new unfinished file in your arx
        '''
        if self.arx['unfinished'] is None :
            self.arx['unfinished'] = (name, max_bound, max_bound, max_time, max_time, 0)
        else :
            print('Exception: Unfinished file already exists in index!')
            raise TypeError
    
    def updateUnfinishedFileReg(self, min_bound, min_time, num_new_tweets) :
        '''
        Update the bounds on the unfinished data file in your arx
        '''
        name = self.arx['unfinished'][0]
        max_bound = self.arx['unfinished'][2]
        max_time = self.arx['unfinished'][4]
        num_tweets = num_new_tweets + self.arx['unfinished'][5]
        self.arx['unfinished'] = (name, min_bound, max_bound, min_time, max_time, num_tweets)
    
    def finalizeUnfinishedData(self, buffer_len=1000) :
        '''
        Push your "unfinished" file to finished file(s) as appropriate. Will
        create new finished files as necessary once the first one fills, and
        deletes the unfinished file upon completion.
        
        You must have a "finished" and an "unfinished" file to call this!!
        '''
        
        # Look up your unfinished and latest "finished" files
        unfin_tuple = self.arx['unfinished']
        fin_tuple = self.arx['finished'][-1]
        
        # Build their respective filepaths
        unfin_file = self.ARCHIVE_DIR + self.arx['query'] + '/' + unfin_tuple[0]
        fin_file = self.ARCHIVE_DIR + self.arx['query'] + '/' + fin_tuple[0]
        
        # Copy the tweets from your unfinished file to finished ones
        with open(unfin_file, 'r') as fopen :
            
            # Append to your latest "finished" file
            fwrite = open(fin_file, 'a+')
            
            # Read the file line by line in reverse order (end-to-start)
            ctr = 0; tweets = ''
            for line in enildaer(fopen) :
                
                # Read some tweets
                tweets += line + '\n'
                ctr += 1
                
                if ctr % 1000 == 0 :
                    print('Loaded ' + str(ctr) + ' tweets.')
                
                # Periodically copy them to the finished file
                if ctr > buffer_len :
                    fwrite.write(tweets)
                    tweets = ''
                    
                    # If the finished file is full, start a fresh one
                    if os.path.getsize(fin_file) > self.SIZE_LIMIT :
                        
                        # Find our new bounds for "finished" file
                            # Max bounds
                        last_bound = None; last_time = None
                        for l2 in enildaer(fwrite) :
                            try :
                                tweet = json.loads(l2)
                                if last_bound is None : last_bound = self.Tweet.getTweetID(tweet)
                                if last_time is None : last_time = self.Tweet.getDate(tweet)
                                if last_bound is not None and last_time is not None : break
                            except ValueError :
                                continue
                            except TypeError :
                                continue
                        max_bound = last_bound; max_time = last_time
                        
                            # Min bounds
                        fwrite.close()
                        fwrite = open(fin_file, 'a+')
                        last_bound = None; last_time = None
                        for l2 in fwrite.readline() :
                            try :
                                tweet = json.loads(l2)
                                if last_bound is None : last_bound = self.Tweet.getTweetID(tweet)
                                if last_time is None : last_time = self.Tweet.getDate(tweet)
                                if last_bound is not None and last_time is not None : break
                            except ValueError :
                                continue
                            except TypeError :
                                continue
                        min_bound = last_bound; min_time = last_time
                        
                        # Close the finished file
                        self.updateLastDataFileReg(min_bound, min_time, max_bound, max_time, ctr)
                        ctr = 0
                        fwrite.close()
                        
                        # Create a fresh one starting at the last's end bounds
                        fin_file = 'tweets-' + str(uuid4()) + '.taj'
                        self.addNewDataFileReg(fin_file, max_bound, max_time)
                        fin_file = self.ARCHIVE_DIR + self.arx['query'] + '/' + fin_file 
                        fwrite = open(fin_file, 'a+')
                        
            # Write the remaining tweets to file
            fwrite.write(tweets)
        
        # Dunno why I have to reopen it; seeking didn't work
        with open(unfin_file, 'r') as fopen :    
            # Get the end bounds for the finished file we just finished writing
            max_bound = None; max_time = None
            for tweet in fopen :
                try :
                    tweet = json.loads(tweet)
                    if max_bound is None : max_bound = self.Tweet.getTweetID(tweet)
                    if max_time is None : max_time = self.Tweet.getDate(tweet)
                    if max_bound is not None and max_time is not None : break
                except ValueError :
                    continue
                except TypeError :
                    continue
        
        # Close the file
        self.updateLastDataFileReg(None, None, max_bound, max_time, ctr)
        fwrite.close()
        
        # Delete the unfinished file, remove it from the archive index
        os.remove(unfin_file)
        self.arx['unfinished'] = None
        
    def writeJSON(self, graph, filename, pretty_print=False) :
        '''
        Super stupid, naive function for writing JSON graphs to file
        '''
        with open(filename,'w+') as fout :
            data = json_graph.node_link_data(graph)
            if pretty_print :
                json.dump(data, fout, sort_keys=True, indent=4)
            else :
                json.dump(data, fout)
                
    def loadJSON(self, filename) :
        '''
        Super stupid, naive function for reading JSON graphs from file
        '''
        with open(filename, 'r') as fin :
            data = json.load(fin)
        return json_graph.node_link_graph(data)
    
    def generateJSONFromTAJ(self, query, taj_name, verbose=True) :
        '''
        Generate a JSON file for the given TAJ file and return the influence graph
        '''
        
        # Parse the file
        with open(self.ARCHIVE_DIR + query + '/' + taj_name, 'r') as fopen :
            
            # Strip the .taj file extension
            taj_name = taj_name[0:len(taj_name)-4]
            
            t1 = time.time()
            
            # Initialize graphs
            retweet_graph = DiGraph()
            reply_graph = DiGraph()
            mention_graph = DiGraph()
            
            # Read tweet file, parse relations
            for line in fopen :
                line = line.strip()
                if line :
                    try :
                        tweet = json.loads(line)
                    except ValueError :
                        continue
                    tweeter, influencers = self.Tweet.getRetweetInfluencers(tweet)
                    for influencer in influencers :
                        retweet_graph.add_edge(influencer, tweeter)
                    tweeter, influencers = self.Tweet.getReplyInfluencers(tweet)
                    for influencer in influencers :
                        reply_graph.add_edge(influencer, tweeter)
                    tweeter, influencers = self.Tweet.getMentionInfluencers(tweet)
                    for influencer in influencers :
                        mention_graph.add_edge(influencer, tweeter)
            
            if verbose : print('Building graphs took ' + str(time.time() - t1) + ' to complete.')
            with open('logs/performance.log','a+') as fout :
                fout.write('\nBuilding graphs took ' + str(time.time() - t1) + ' to complete.')
            t1 = time.time()
                                    
        # Merge the graphs and store them
            # Create directory
        try:
            os.makedirs(self.ARCHIVE_DIR + query + '/graphs/' + taj_name + '/', exist_ok=True)
        except (FileNotFoundError, FileExistsError):
            pass

            # Retweet Graph
        self.writeJSON(retweet_graph, self.ARCHIVE_DIR + query + '/graphs/' + taj_name + '/retweet_graph.jnld')
        graph = retweet_graph   # Pass by reference
        retweet_graph = None    # Just a human touch for readability
            # Reply Graph
        self.writeJSON(reply_graph, self.ARCHIVE_DIR + query + '/graphs/' + taj_name + '/reply_graph.jnld')
        graph.add_edges_from(reply_graph.edges(data=True))
        reply_graph = None      # Free up memory
            # Mention Graph
        self.writeJSON(mention_graph, self.ARCHIVE_DIR + query + '/graphs/' + taj_name + '/mention_graph.jnld')
        graph.add_edges_from(mention_graph.edges(data=True))
        mention_graph = None    # Free up memory
            # Merged Graph
        self.writeJSON(graph, self.ARCHIVE_DIR + query + '/graphs/' + taj_name + '/influence_graph.jnld')
                
            # Performance logging
        if verbose : print('Storing graphs took ' + str(time.time() - t1) + ' to complete.')
        with open('logs/performance.log','a+') as fout :
            fout.write('\nStoring graphs took ' + str(time.time() - t1) + ' to complete.')
        
        # Return your influence graph
        return graph
    
    def loadGraphForTAJ(self, query, taj_name, graph_type='influence', create_if_missing=True, update_latest=True) :
        '''
        Load the graph for a given query and its specified taj file. This will load the graph from a JSON file if it
        already exists, or else it will create a JSON file for the graph before returning it.
        '''
        
        # Strip .taj extension
        taj_name = taj_name[0:len(taj_name)-4]
        
        # Generate graph name
        if graph_type == 'influence' :
            graph_type_str = '/influence_graph.jnld'
        elif graph_type == 'retweet' :
            graph_type_str = '/retweet_graph.jnld'
        elif graph_type == 'reply' :
            graph_type_str = '/reply_graph.jnld'
        elif graph_type == 'mention' :
            graph_type_str = '/mention_graph.jnld'
        else :
            graph_type = 'influence'
            graph_type_str = '/influence_graph.jnld'
        
        # Read that graph if it exists, otherwise create graphs and return your graph
        #  Make the directory if it doesn't exist
        try:
            os.makedirs(self.ARCHIVE_DIR + query + '/graphs/' + taj_name + '/', exist_ok=True)
        except (FileNotFoundError, FileExistsError):
            pass

        try :
            
            # If we're not going to update the graph, just pull it out
            if not update_latest :
                return self.loadJSON(self.ARCHIVE_DIR + query + '/graphs/' + taj_name + graph_type_str)
            
            # If this is a graph that might have been updated, regenerate it
            else :
                
                # Get names of our graphs that might be updated
                if self.arx['unfinished'] is not None :
                    unfin = self.arx['unfinished'][0]
                else :
                    unfin = ''
                if len(self.arx['finished']) > 0:
                    fin = self.arx['finished'][-1][0]
                else :
                    fin = ''
                
                # Check for match
                if taj_name + '.taj' == unfin or taj_name + '.taj' == fin :
                    
                    # Might have been updated; regenerate graph
                    graph = self.generateJSONFromTAJ(query, taj_name+'.taj')
                    if graph_type != 'influence' :
                        return self.loadJSON(self.ARCHIVE_DIR + query + '/graphs/' + taj_name + graph_type_str)
                    else :
                        return graph
                else :
                    # Graph should not have been updated; load it
                    return self.loadJSON(self.ARCHIVE_DIR + query + '/graphs/' + taj_name + graph_type_str)
        except :
            # Make our graph if it doesn't exist yet
            if create_if_missing :
                graph = self.generateJSONFromTAJ(query, taj_name + '.taj')
                if graph_type != 'influence' :
                    return self.loadJSON(self.ARCHIVE_DIR + query + '/graphs/' + taj_name + graph_type_str)
                else :
                    return graph
            else :
                return None
                
    def getBoundsFromTo(self, tweet_id_start=None, tweet_id_stop=None):
        """
        Returns a list of bounds containing all tweets from tweet_id_start (included)
        to tweet_id_stop (included)
        
        If tweet_id_start (tweet_id_stop) is not specified, returns bounds form the 
        first one (until the last finished one)

        for each bound :
        bound[0] : filename
        bound[1] : first tweet ID (minID)
        bound[2] : last tweet ID (maxID)
            where the tweet IDs (ID) in the file are such as minID < ID <= maxID
        bound[3] and bound[4] are the timestamp corresponding to minID and maxID
        """
        
        bounds = []
        if len(self.arx['finished']) > 0 :
            bounds.extend(self.arx['finished'])
        else:
            return []

        first_tids = [bound[1] for bound in bounds]    
        last_tids = [bound[2] for bound in bounds]
        
        if tweet_id_start is None:
            min_bound = 0
        else:
            if tweet_id_start <= min(first_tids):
                min_bound = 0
            else:
                min_bound = max([i for i, first_tid in enumerate(first_tids) if first_tid < tweet_id_start])
        
        if tweet_id_stop is None:
            max_bound = -2
        else:
            if tweet_id_stop > max(last_tids):
                max_bound = -2
            else:                
                max_bound = min([i for i, last_tid in enumerate(last_tids) if last_tid >= tweet_id_stop])
        
        
        return bounds[min_bound:max_bound+1]
        
        
            
    def buildGraph(self, min_bound=None, max_bound=None, graph_type='influence', 
                   min_date=None, max_date=None, force_reparse=False,
                   save_tweet_ids=False):
        '''
        Load a NetworkX graph for the tweet range specified. This may require re-parsing
        the first and last graphs in the series if your tweet bounds fall within a graph.
        
        min and max date are datetime object        
        
        force_reparse : forces the reparsing the taj file
        save_tweet_ids : add the tweet_ids as edge attributes the graph
        
        returns a networkx DiGraph
        '''
        
        # Tweets can't possibly exist
        if min_bound is None :
            min_bound = 0
        if max_bound is None :
            max_bound = float('inf')
        if min_bound > max_bound :
            raise ValueError
        
        # Init
        graph = DiGraph(graph_type=graph_type)
        bounds = []
        min_ptr = -1; max_ptr = -1
        reparse_min = False; reparse_max = False
        
        # Add the range of each file to our bounds list
        """ for each bound :
        bound[0] : filename
        bound[1] : first tweet ID (minID)
        bound[2] : last tweet ID (maxID)
            where the tweet IDs (ID) in the file are such as minID < ID <= maxID
        bound[3] and bound[4] are the timestamp corresponding to minID and maxID
        """
        
        if len(self.arx['finished']) > 0 :
            bounds.extend(self.arx['finished'])
        if self.arx['unfinished'] is not None :
            bounds.append(self.arx['unfinished'])
            
        
        if (min_date is not None) and (max_date is not None):
            # use date range instead of tweet ids
            
            try:
                taj_min = self.getTAJinfos(min_date)[0]
            except BeforeFirstTweet:
                taj_min = bounds[0][0]
                min_date = parsedate(bounds[0][3])                
            except AfterLastTweet:
                # there is no tweet during this time period
                raise EmptyGraph('Min date is after the last tweet.' + \
                                '\n last tweet : ' + bounds[-1][4])
                                
            try:
                taj_max = self.getTAJinfos(max_date)[0]
            except BeforeFirstTweet:
                # there is no tweet during this time period
                raise EmptyGraph('Max date is before the first tweet.' + \
                                '\n first tweet : ' + bounds[0][3])                
            except AfterLastTweet:
                taj_max = bounds[-1][0]
                max_date = parsedate(bounds[-1][4])                 
            
            file_min = os.path.join(self.ARCHIVE_DIR, self.arx['query'], taj_min)
            file_max = os.path.join(self.ARCHIVE_DIR, self.arx['query'], taj_max)
            
            # read these files and get all (tweet IDs, timestamp) pairs
            tid2timestamp = self.Tweet.getTweetIDtoTimestampDict(list({file_min, file_max}))
            
            # convert to tweet id
            # smallest tweet id with corresponding timestamp
            if min_date <= min(list(tid2timestamp.values())):
                min_bound = min(list(tid2timestamp.keys()))
            else:
                min_bound = min([tid for tid, timestamp in tid2timestamp.items() if timestamp >= min_date])
            # largest tweet id with corresponding timestamp
            if max_date >= max(list(tid2timestamp.values())):
                max_bound = max(list(tid2timestamp.keys()))
            else:
                max_bound = max([tid for tid, timestamp in tid2timestamp.items() if timestamp <= max_date])
          
        # Find first and last TAJ files to parse
        for n, bound in enumerate(bounds) :
            
            # Unpack
            # tweet numbers
            taj_min = bound[1] 
            taj_max = bound[2]
            
            if taj_min is None and taj_max is None :
                continue
            
            assert taj_min <= taj_max
            
            # Check min bound
            if min_ptr == -1 :
                if min_bound < taj_min :
                    min_ptr = n
                elif min_bound < taj_max :
                    min_ptr = n; reparse_min = True
            
            # Check max bound
            if max_ptr == -1 :
                if max_bound < taj_min :
                    if n > 0 :
                        max_ptr = n
                    else :
                        return graph
                elif max_bound <= taj_max :
                    max_ptr = n+1; reparse_max = True
            
            # Once you find both bounds, stop looking
            if min_ptr != -1 and max_ptr != -1 :
                break
        
        # Eliminate bounds outside collection range
        if min_ptr == -1 : min_ptr = 0
        if max_ptr == -1 : max_ptr = len(bounds)
        bounds = bounds[min_ptr:max_ptr]
        
        # Built TAJ list
        taj_list = [bound[0] for bound in bounds]
        
        # Load TAJ files, reparse as necessary
        for n, taj in enumerate(taj_list) :
            
            # Check if we need to reparse this TAJ's graph
            reparse_me = False
            if n == 0 and reparse_min :
                reparse_me = True
            elif n == len(taj_list) - 1 and reparse_max :
                reparse_me = True
            
            # Create graph for this TAJ
            sub_g = DiGraph()
            if reparse_me or force_reparse:
                
                # Check if tweets are new-to-old or old-to-new
                newest_first = taj[0:3] == 'new'
                
                # Surrender the thread
                time.sleep(0)
                
                # Parse the file
                with open(self.ARCHIVE_DIR + self.arx['query'] + '/' + taj, 'r') as fopen :
                    
                    # Set the kind of influence you want to parse
                    if graph_type == 'retweet' :
                        getAppropriateInfluencers = self.Tweet.getRetweetInfluencers
                    elif graph_type == 'reply' :
                        getAppropriateInfluencers = self.Tweet.getReplyInfluencers
                    elif graph_type == 'mention' :
                        getAppropriateInfluencers = self.Tweet.getMentionInfluencers
                    elif graph_type == 'quote' :
                        getAppropriateInfluencers = self.Tweet.getQuoteInfluencers
                    elif graph_type == 'influence' :
                        getAppropriateInfluencers = self.Tweet.getInfluencers
                    else:
                        raise ValueError('Inappropriate graph_type')
                    
                    # Parse influencers
                    for line in fopen :
                        line = line.strip()
                        if line :
                            try :
                                tweet = json.loads(line)
                            except ValueError :
                                continue                            
                            tweet_id = self.Tweet.getTweetID(tweet)
                                                        
                            # Skip tweets that occur before the ones you want
                            if newest_first and tweet_id > max_bound \
                                or not newest_first and tweet_id < min_bound :
                                continue
                                
                            # Stop looking when you pass the tweets you want
                            if newest_first and tweet_id < min_bound \
                                or not newest_first and tweet_id > max_bound :
                                break                            
                                
                            # Collect the tweets you want
                            tweeter, influencers = getAppropriateInfluencers(tweet)
                            for influencer in influencers:
                                if save_tweet_ids:
                                    addEdgeWithTweetID(sub_g, influencer, tweeter, tweet_id)                                    
                                else:    
                                    sub_g.add_edge(influencer, tweeter)
                        
            else :
                # Load pregenerated graph
                sub_g = self.loadGraphForTAJ(self.arx['query'],taj)
            
            # Merge to total graph
            if save_tweet_ids:
                addEdgesFromWithTweetID(graph, sub_g)
            else:
                graph.add_edges_from(sub_g.edges(data=True))
            
        # add information about start and end time to the graph
        if min_bound < float('inf') :
            graph.graph['first_tweet_id'] = min_bound
        else :
            graph.graph['first_tweet_id'] = None
            
        if max_bound < float('inf') :
            graph.graph['last_tweet_id'] = max_bound
        else :
            graph.graph['last_tweet_id'] = None
            
        if (min_date is not None) and (max_date is not None):
            graph.graph['first_tweet_time'] = tid2timestamp[min_bound]
            graph.graph['last_tweet_time'] = tid2timestamp[max_bound]
            
        # Return the influencers graph
        return graph
    
    def getTAJinfos(self, timestamp):
        """ Returns a tuple with: 
        
             (filename, minID, maxID, min timestamp, max timestamp, num_tweets)
             
            where the tweet IDs (ID) in the file are such as minID < ID <= maxID,
        
            of the taj file containing this tweet timestamp.
        
            timestamp is a datetime object
        """
        bounds = []
        
        if len(self.arx['finished']) > 0 :
            bounds.extend(self.arx['finished'])
        if self.arx['unfinished'] is not None :
            bounds.append(self.arx['unfinished'])

        d2tid_min = {parsedate(bound[3]) : bound[1] for bound in bounds}
        
        if timestamp < min(list(d2tid_min.keys())):
            raise BeforeFirstTweet('No tweets at this date.\nFirst tweet was at ' \
            + min(list(d2tid_min.keys())).strftime('%Y-%m-%d %Hh%Mm%Ss %Z%z'))
            
        if timestamp > max(parsedate(bound[4]) for bound in bounds):
            raise AfterLastTweet('No tweets at this date.\nLast tweet was at ' \
            + max(parsedate(bound[4]) for bound in bounds).strftime('%Y-%m-%d %Hh%Mm%Ss %Z%z'))
                                       
        # find the min_tweet_id bound of the file containing the timestamp
        timestamp_min_bound = max([date for date in list(d2tid_min.keys()) if date < timestamp])
        
        # return the filename
        return [bound for bound in bounds if bound[1] == d2tid_min[timestamp_min_bound]][0]

    
    def getNumTweets(self, min_date=None, max_date=None):
        """return the number of tweets collected between min_date and max_date such that 
        
            min_date <= tweet_time < max_date
            
            /!\ does not distinguish all duplicated tweets
            
        """        
        
        bounds = []
        
        if len(self.arx['finished']) > 0 :
            bounds.extend(self.arx['finished'])
        if self.arx['unfinished'] is not None :
            bounds.append(self.arx['unfinished'])
            
        # find taj files corresponding to min_date and max_date

        try:
            taj_min = self.getTAJinfos(min_date)[0]
        except BeforeFirstTweet:
            taj_min = bounds[0][0]
            min_date = parsedate(bounds[0][3])
            
        except AfterLastTweet:
            # there is no tweet during this time period
            return 0
            
        try:
            taj_max = self.getTAJinfos(max_date)[0]
        except BeforeFirstTweet:
            # there is no tweet during this time period
            return 0
        except AfterLastTweet:
            taj_max = bounds[-1][0]
            max_date = parsedate(bounds[-1][4])            
            
            
        ind_min, ind_max = [i for i, bound in enumerate(bounds) if bound[0] == taj_min or bound[0] == taj_max]
        
        intermediate_tweets = 0
        
        if ind_max - ind_min > 1:
            
            intermediate_tweets = sum(bound[5] for bound in bounds[ind_min+1:ind_max])
                

        tid2timestamp = self.Tweet.getTweetIDtoTimestampDict(list({os.path.join(self.ARCHIVE_DIR,
                                                                 self.arx['query'], 
                                                                 taj_min), 
                                                                 os.path.join(self.ARCHIVE_DIR,
                                                                 self.arx['query'], 
                                                                 taj_max)}))
        bound_tweets = len([tid for tid, dt in tid2timestamp.items() if \
                    dt >= min_date and dt < max_date])

        return bound_tweets + intermediate_tweets


    def verifyIndex(self) :
        """
        Fix any damaged entries in your ARX index
        :return: 
        """
        damaged = False
        if self.arx['unfinished'] is not None :
            unfin = self.arx['unfinished']
            for bound in unfin :
                if bound is None :
                    damaged = True
                    break
        if damaged :
            new_bounds = self.findTAJbounds(unfin[0], finished_file=False)
            unfin[1] = new_bounds[0]
            unfin[2] = new_bounds[1]
            unfin[3] = new_bounds[2]
            unfin[4] = new_bounds[3]
        
        for taj in self.arx['finished'] :
            damaged = False
            taj_file = taj[0]
            for bound in taj :
                if bound is None :
                    damaged = True
                    break
            if damaged :
                new_bounds = self.findTAJbounds(taj[0], finished_file=False)
                taj[1] = new_bounds[0]
                taj[2] = new_bounds[1]
                taj[3] = new_bounds[2]
                taj[4] = new_bounds[3]
        
        # Commit fixes to disk
        self.commitArchive()
    
    
    def findTAJbounds(self, taj_filename, finished_file=True) :
        old_to_new = finished_file
        min_date = None;
        max_date = None
        min_id = None;
        max_id = None
        
        # Feed forward
        with open(taj_filename) as taj :
            for line in taj :
                tweet = json.loads(line)
                
                # Get date and ID bounds from the top of the file
                if old_to_new :
                    if min_date is None or min_id is None :
                        if min_date is None :
                            min_date = self.Tweet.getTimeStamp(tweet)
                        if min_id is None :
                            min_id = self.Tweet.getTweetID(tweet)
                    else :
                        break
                else :
                    if max_date is None or max_id is None :
                        if max_date is None :
                            max_date = self.Tweet.getTimeStamp(tweet)
                        if max_id is None :
                            max_id = self.Tweet.getTweetID(tweet)
                    else :
                        break
        
        # Feed in reverse
        with open(taj_filename) as taj :
            for line in enildaer(taj) :
                tweet = json.loads(line)
                
                # Get date and ID bounds from the bottom of the file
                if not old_to_new :
                    if min_date is None or min_id is None :
                        if min_date is None :
                            min_date = self.Tweet.getTimeStamp(tweet)
                        if min_id is None :
                            min_id = self.Tweet.getTweetID(tweet)
                    else :
                        break
                else :
                    if max_date is None or max_id is None :
                        if max_date is None :
                            max_date = self.Tweet.getTimeStamp(tweet)
                        if max_id is None :
                            max_id = self.Tweet.getTweetID(tweet)
                    else :
                        break
        # Return bounds
        return (min_id, max_id, min_date, max_date)
    
    
    def iterTweets(self, min_bound=None, max_bound=None,
                   min_date=None, max_date=None, reverse=False) :
        '''
        Generator to iterate over the tweet range specified.
    
        min and max date are datetime object
    
        yields tweet JSON objects
        '''
        
        # Tweets can't possibly exist
        if min_bound is None :
            min_bound = 0
        if max_bound is None :
            max_bound = float('inf')
        if min_bound > max_bound :
            raise ValueError
        
        # Init
        bounds = []
        min_ptr = -1
        max_ptr = -1
        
        # Add the range of each file to our bounds list
        """ for each bound :
        bound[0] : filename
        bound[1] : first tweet ID (minID)
        bound[2] : last tweet ID (maxID)
            where the tweet IDs (ID) in the file are such as minID < ID <= maxID
        bound[3] and bound[4] are the timestamp corresponding to minID and maxID
        """
        
        if len(self.arx['finished']) > 0 :
            bounds.extend(self.arx['finished'])
        if self.arx['unfinished'] is not None :
            bounds.append(self.arx['unfinished'])
        
        if (min_date is not None) and (max_date is not None) :
            # use date range instead of tweet ids
            
            try :
                taj_min = self.getTAJinfos(min_date)[0]
            except BeforeFirstTweet :
                taj_min = bounds[0][0]
                min_date = parsedate(bounds[0][3])
            except AfterLastTweet :
                # there is no tweet during this time period
                return
            
            try :
                taj_max = self.getTAJinfos(max_date)[0]
            except BeforeFirstTweet :
                # there is no tweet during this time period
                return
            except AfterLastTweet :
                taj_max = bounds[-1][0]
                max_date = parsedate(bounds[-1][4])
            
            file_min = os.path.join(self.ARCHIVE_DIR, self.arx['query'], self.getTAJinfos(min_date)[0])
            file_max = os.path.join(self.ARCHIVE_DIR, self.arx['query'], self.getTAJinfos(max_date)[0])
            
            # read these files and get all (tweet IDs, timestamp) pairs
            tid2timestamp = self.Tweet.getTweetIDtoTimestampDict(list({file_min, file_max}))
            
            # convert to tweet id
            # smallest tweet id with corresponding timestamp
            if min_date <= min(list(tid2timestamp.values())) :
                min_bound = min(list(tid2timestamp.keys()))
            else :
                min_bound = min([tid for tid, timestamp in tid2timestamp.items() if timestamp >= min_date])
            # largest tweet id with corresponding timestamp
            if max_date >= max(list(tid2timestamp.values())) :
                max_bound = max(list(tid2timestamp.keys()))
            else :
                max_bound = max([tid for tid, timestamp in tid2timestamp.items() if timestamp <= max_date])
        
        # Find first and last TAJ files to parse
        for n, bound in enumerate(bounds) :
            
            # Unpack
            # tweet numbers
            taj_min = bound[1]
            taj_max = bound[2]
            
            if taj_min is None and taj_max is None :
                continue
            
            assert taj_min <= taj_max
            
            # Check min bound
            if min_ptr == -1 :
                if min_bound < taj_min :
                    min_ptr = n
                elif min_bound < taj_max :
                    min_ptr = n
            
            # Check max bound
            if max_ptr == -1 :
                if max_bound < taj_min :
                    if n > 0 :
                        max_ptr = n
                    else :
                        return
                elif max_bound <= taj_max :
                    max_ptr = n + 1
            
            # Once you find both bounds, stop looking
            if min_ptr != -1 and max_ptr != -1 :
                break
        
        # Eliminate bounds outside collection range
        if min_ptr == -1 : min_ptr = 0
        if max_ptr == -1 : max_ptr = len(bounds)
        bounds = bounds[min_ptr :max_ptr]
        
        # Built TAJ list
        taj_list = [bound[0] for bound in bounds]
        
        # Load TAJ files
        if reverse : taj_list = reversed(taj_list)
        for n, taj in enumerate(taj_list) :
            
            # Check if tweets are new-to-old or old-to-new
            newest_first = taj[0 :3] == 'new'
            
            # Surrender the thread
            time.sleep(0)
            
            # Parse the file
            with open(self.ARCHIVE_DIR + self.arx['query'] + '/' + taj, 'r') as fopen :
                
                # Iterate through tweets
                if reverse and not newest_first or \
                                not reverse and newest_first :
                    file_iter = enildaer(fopen)
                else :
                    file_iter = fopen
                for line in file_iter :
                    line = line.strip()
                    if line :
                        try :
                            tweet = json.loads(line)
                        except ValueError :
                            continue
                        tweet_id = self.Tweet.getTweetID(tweet)
                        
                        # Skip tweets that occur before the ones you want
                        if tweet_id < min_bound :
                            continue
                        
                        # Stop looking when you pass the tweets you want
                        if tweet_id > max_bound :
                            break
                        
                        yield tweet
