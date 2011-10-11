import logging
import multiprocessing
import time
import sys
import signal

from worker import work

logging.basicConfig(
	format='%(asctime)s|%(process)d|%(message)s', 
	filename='bmt.log',
	level=logging.INFO)
logger = logging.getLogger('master')

ch = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(message)s')
ch.setLevel(logging.WARN)
ch.setFormatter(formatter)

logger.addHandler(ch)

contexts = []

finish = False

now_workers = 0
server_index = 0
max_workers = 100
rampup_time_interval = 60*3
rampup_step = 1

def handler(signum, frame):
	global finish
	logger.info('got a signal')
	finish = True

def initialize():
	logger.info('initialize master with %d processes'%concurrency)
	signal.signal(signal.SIGTERM, handler)
	signal.signal(signal.SIGINT, handler)
	for i in range(max_workers):
		p = multiprocessing.Process(
			target=work, args=(str(i),))
		contexts.append((p, i))

def start(from_nu, to_nu):
	for context in contexts[from_nu:to_nu]:
		logger.info('%s process start'%(context[1]))
		context[0].start()

def main(concurrency):
	global max_workers, now_workers
	max_workers = concurrency
	initialize()
	logger.info('start loop!')
	while True:
		if finish:
			logger.exception('finish jobs')		
			[c[0].terminate() for c in contexts[:now_workers]]
			[c[0].join() for c in contexts[:now_workers]]
			sys.exit(0)
		logger.info('now:%d max:%d'%(now_workers-rampup_step, max_workers))
		if now_workers <= max_workers:
			now_workers += rampup_step
			logger.warn('rampup %d/%d'%(now_workers, max_workers))
			start(now_workers-rampup_step, now_workers)

		time.sleep(rampup_time_interval)

if __name__ == "__main__":
	concurrency = int(sys.argv[1])
	main(concurrency)
