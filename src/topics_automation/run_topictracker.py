import sys
import os
PACKAGE_PARENT = '..'
SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))

from TopicTracker import TopicTracker

queries_file = 'queries.list'

ARCHIVE_DIR = '/home/alex/kcore_twitter/archives/'

if __name__ == "__main__" :
    
    queries = []
    with open(queries_file, 'r') as fopen:
        for line in fopen:
            queries.append(line.strip('\n'))

    
    tw_tracker = TopicTracker()
    tw_tracker.collect(queries, ARCHIVE_DIR)

