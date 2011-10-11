import subprocess
import logging
import signal
from datetime import datetime
import time
import os
from thrift.transport import TTransport, TSocket
from thrift.protocol import TBinaryProtocol
from scribe import scribe

logger = logging.getLogger('worker')
line = '''SUCCESS iiiiiiioiii 5/466 Rule=Subject(7) - 114.207.112.51 WHITE kjnkjsd@oekrj.erg "glR" kjnkjsd@oekrj.erg SpamRate=NO(SR:8.33) spf=none UT=N MSz=1792 AC=0 IC=0 VOL=nasm1904,tix24-2.nm.nhnsystem.com RESTORE=- Subject="subjectaaaa  sdfsfasdfasfd sdfasdfa sadfasdf" mtaip=trcvmail16-1.nm.naver.com rcptto=iiiiiiioiii@naver.com'''

finish = False

host = '10.25.84.67'
port = 1463

interval = 0.003
logs = 50

def handler(signum, frame):
	global finish
	logger.info('got a signal')
	finish = True

def init_worker():
	logger.info('initialize worker')
	signal.signal(signal.SIGTERM, handler)

def work(id):
	hostname = os.uname()[1]
	wrote = 0
	init_worker()
	socket = TSocket.TSocket(host=host, port=port)
	transport = TTransport.TFramedTransport(socket)
	protocol = TBinaryProtocol.TBinaryProtocol(trans=transport, strictRead=False,strictWrite=False)
	client = scribe.Client(iprot=protocol, oprot=protocol)
	category = 'bmt_%s'%id
	transport.open()
	while True:
		if finish:
			logger.info('finish work')
			return 0
		started = datetime.now()
		buffer = []
		for i in range(logs):
			wrote += 1
			msg = "%s %s-%s %d %s\n"%(datetime.now(), hostname, id, wrote, line)
			log_entry=scribe.LogEntry(category=category, message=msg)
			buffer.append(log_entry)
		started = datetime.now()
		result = client.Log(buffer)
		elapsed = datetime.now()-started
		logger.info('elapsed time to write %d logs/%d: %s ms'%(logs, wrote, elapsed))
		if interval > 0:
			time.sleep(interval)
