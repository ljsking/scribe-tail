ps -ef | grep scribe_tail | grep -v grep | awk '{print $2}' | xargs kill -9
