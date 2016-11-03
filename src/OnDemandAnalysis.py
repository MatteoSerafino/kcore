'''
Created on Nov 15, 2015

@author: geofurb
'''
from TwAPIer import TwiAPIer
from TwArchive import TweetArchive
import TwAnalytics
import time
import traceback
import shutil
import networkx as nx
from networkx.readwrite import json_graph

import CredMgmt

def OnDemandAnalysis(query,num_queries=60,old_CI=False,ball_radius=2,my_api=None,
                     archive_dir=None) :
    
    KEYWORD = query
    MAX_QUERIES = num_queries
    RUNLEVEL = ball_radius
    
    try :
        
        # Connect to Twitter API
        if my_api is None :
            print('No API fed in!')
            api = TwiAPIer()
        else :
            api = my_api
        api.connect()
        
        # Iterate through keywords
        ARX = TweetArchive(KEYWORD, archive_dir=archive_dir)
        
        cycle_start = time.time()
        
        # Start clock
        print('\n\n\nProcessing: \"' + KEYWORD + '\"\n@ Level ' + str(RUNLEVEL) + '\n')
        with open('logs/performance.log','a+') as fout :
            fout.write('\n\n\nProcessing: \"' + KEYWORD + '\"\n@ Level ' + str(RUNLEVEL) + '\n')
            fout.write(time.strftime("%m-%d-%Y %H:%M:%S (UTC %z)"))
            fout.write('\n-----------\n')
        
        
        # Read Tweets
        DONE_READING = False
        # Acquire data NUM_PASSES times
        print('Begin archive search of ' + KEYWORD)
        
        elapsed = time.time()
        DONE_READING, RATE_LIMITED = api.archiveSearch(ARX, MAX_QUERIES,wait_on_rate_limit=False)
    
        elapsed = time.time() - elapsed
        print('\nQuery took ' + str(elapsed) + ' to complete.\n')
        elapsed = time.time()
        
        analytics = TwAnalytics.TwiAnalytics(num_ci_threads=1)
        analytics.CP = not old_CI
        graph = ARX.buildGraph()
        
        elapsed = time.time() - elapsed
        print('\nLoading the graph took ' + str(elapsed) + ' to complete.\n')
        elapsed = time.time()
        
        # Python CI Computation
        influencers, ndeg, nCI = analytics.siteCI(graph, RUNLEVEL, directed=True, treelike=True)
        
        # Sift influencers
        total_influence = sum(nCI)
        
        # If there are NO INFLUENCERS
        if total_influence < 1 :
            # Return high-order nodes
            pass
        
        # Pretty-print influencers, figure out influence share
        ctr = 0; inf_ratio = []
        for inf in nCI :
            if ndeg[ctr] - 1 > 0 :
                inf_ratio.append("%.2f" % (nCI[ctr] / ((ndeg[ctr] - 1) * (ndeg[ctr]))))
            else :
                inf_ratio.append("%.2f" % nCI[ctr])
            nCI[ctr] = (inf * 100.0 / total_influence)
            ctr += 1
        ctr = 0
        while ctr < len(influencers) :
            influencers[ctr] = str(influencers[ctr])
            ctr += 1
        if len(influencers) >= 100 :
            top_inf = influencers[0:100]
        else :
            top_inf = influencers

        # Garbage collect old graph, hopefully
        graph = None
        time.sleep(0)
        
        # Rebuild graph so you can make the influencer subgraph
        graph = ARX.buildGraph()
            
        usr_info = api.resolveUsers(top_inf)
        screen_names = []; follower_counts = []; mapping = {}
        for user in top_inf :
            screen_names.append(usr_info[user]['screen_name'])
            follower_counts.append(usr_info[user]['followers_count'])
            mapping[int(user)] = usr_info[user]['screen_name']
        
        nx.relabel_nodes(graph, mapping, copy=False)
        graph = graph.subgraph(screen_names)
        graph_data = json_graph.node_link_data(graph)
        
        #top_inf = api.idToScreenname(top_inf)
        top_inf = screen_names
        influencers[0:len(top_inf)] = top_inf
        
        # Build influencer dictionary
        infdict = {}
        #for rank,influencer in enumerate(influencers) :
        for rank, influencer in enumerate(top_inf) :
            infdict[influencer] = {}
            infdict[influencer]['rank'] = rank+1
            infdict[influencer]['influence'] = "%.2f" % nCI[rank]
            infdict[influencer]['magnification'] = inf_ratio[rank]
            infdict[influencer]['connections'] = ndeg[rank]
            infdict[influencer]['followers'] = follower_counts[rank]
            
            # Pass in CI scores
            graph.node[influencer]['CI'] = nCI[rank] / total_influence
            if rank < 10 :
                graph.node[influencer]['group'] = 2
            elif rank < 100 :
                graph.node[influencer]['group'] = 1
            else :
                graph.node[influencer]['group'] = 0
            
    
        with open('logs/performance.log','a+') as fout :
            fout.write('\nPyCI-non-treelike took ' + str(time.time()-elapsed) + ' to complete.\n' )
        shutil.copy(analytics.ALGORITHM_DB + 'alpha_influencers_directed.txt',analytics.ALGORITHM_DB+'results/directed/weblike/'+KEYWORD.lower()+'.txt')
    
        elapsed = time.time() - elapsed
        print('\nPyCI took ' + str(elapsed) + ' to complete.')
        elapsed = time.time()
        
        print('\nCycle took ' + str(time.time()-cycle_start) + ' to complete.')

        # Kill Twitter API connection
        api.disconnect()
        
        return query, (infdict, graph_data)
        
    except Exception :
        print('\n\n<<EXCEPTION OCCURRED DURING EXECUTION!>>\n')
        with open('logs/automation_errors.log','a+') as fout :
            fout.write('ERROR REPORT:\n')
            fout.write('QUERY: ' + str(query) + '\n')
            fout.write(time.strftime("%m-%d-%Y %H:%M:%S (UTC %z)"))
            fout.write('\n-----------\n')
            traceback.print_stack(file=fout)
            fout.write('\n-----------\n')
            traceback.print_exc(file=fout)
            fout.write('\n-----------\n\n\n\n\n')