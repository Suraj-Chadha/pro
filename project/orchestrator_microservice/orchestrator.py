from flask import Flask
import os
import flask
import json as j
import pika
import uuid
import sqlalchemy as sql
import sqlalchemy.orm as orm
import flask_sqlalchemy_session as fss
from flask import abort, Response, request
import docker
import time
import threading
import logging
from kazoo.client import KazooClient
from kazoo.client import KazooState
from apscheduler.schedulers.background import BackgroundScheduler


GLOBAL_CORR_ID = 0
GLOBAL_starttimer = False
GLOBAL_timerStarted = False
numberofreq = 0
GLOBAL_containerList = {} #globalIndex : #container
pidToNameMap = {} #globalIndex : {pid:name}
 
global_dict_index = 0

client = docker.from_env()
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
x_client = docker.APIClient(base_url='unix://var/run/docker.sock')


#--------------------------------------------------------------ZOOKEEPER PART---------------------------------------------------------------
### 1.)  connect to zookeeper container
###
###

time.sleep(10)

zk = KazooClient(hosts='zoo1:2181')

def my_listener(state):
    if state == KazooState.LOST:
        logging.warning("ZooKeeper connection Lost")
    elif state == KazooState.SUSPENDED:
        #Handle being disconnected from Zookeeper
        logging.warning("ZooKeeper connection Suspended")
    else:
        #Handle being connected/reconnected to Zookeeper
        logging.info("ZooKeeper Connected")

zk.add_listener(my_listener)
zk.start()

if(zk.exists("/Workers")):
	zk.delete("/Workers",recursive = True)

#zk.ensure_path("/Workers/")
if(zk.exists("/MasterSlave")):
	zk.delete("/MasterSlave",recursive = True)

zk.ensure_path("/MasterSlave/")


@zk.ChildrenWatch("/Workers/", send_event = True)
def watch_parent_node(children, event):
    print("Event Occurred")
    if event == None:
        pass
    elif event.type is DELETED:
        print("slave deleted spawning asynchoronously")
    elif event.type is CREATED:
        #slave leader election
        leaderNode = None
        for i in children:
            pid = int(i.split('-')[1])
            if leaderNode is None:
                leaderNode = pid
            elif pid < leaderNode:
                leaderNode = pid
       
        if(zk.exists("/MasterSlave")):
            zk.delete("/MasterSlave")
            zk.create("/MasterSlave/"+'worker-'+str(leaderNode),str(leaderNode).encode('utf-8'))





#-------------------------------DYNAMICALLY SPAWNING 1 MASTER------------------------------------- Uncomment after testing

#client.containers.run("project_master:latest", command =["python3","ms0.py","1"],detach=True,network = 'project_default',volumes = {'/var/run/docker.sock':{'bind':'/var/run/docker.sock'}})
#,volumes = {'/home/kaustubh/project/appdata':{'bind': '/app/pdb'}}

#-------------------------------DYNAMICALLY SPAWNING 1 SLAVE-------------------------------------- Uncomment after testing

############################################################################################################################


GLOBAL_containerList[global_dict_index] = client.containers.run("project_slave1:latest", command =["python3","ms.py","1"], \
detach=True,network = 'project_default',volumes = {'/home/ubuntu/project/appdata':{'bind': '/app/pdb','mode': 'ro'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'}})

Name = GLOBAL_containerList[global_dict_index].name

Pid = x_client.inspect_container(Name)['State']['Pid']
pidToNameMap[global_dict_index] = {'Name':Name, 'Pid': Pid}
global_dict_index +=1

zk.create('/MasterSlave/' + 'worker-'+str(Pid),str(Pid).encode('utf-8'))

############################################################################################################################

#--------------Connection to read/write queue-------------------------------------

#def callbackResponse(ch, method, properties, body):



#time.sleep(10)
def startSlave():
	#-------------------------------
	
	global GLOBAL_containerList
	global pidToNameMap
	global global_dict_index
	global client
	global x_client
	
	#-------------------------------
	
	GLOBAL_containerList[global_dict_index] = client.containers.run("project_slave1:latest", command =["python3","ms.py","1"], \
	detach=True,network = 'project_default',volumes = {'/home/ubuntu/project/appdata':{'bind': '/app/pdb','mode': 'ro'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'}})

	Name = GLOBAL_containerList[global_dict_index].name

	Pid = x_client.inspect_container(Name)['State']['Pid']
	pidToNameMap[global_dict_index] = {'Name':Name, 'Pid': Pid}
	global_dict_index +=1


def starttimer():
	
	global GLOBAL_timerStarted
	global GLOBAL_starttimer
	
	if(not (GLOBAL_timerStarted) and (GLOBAL_starttimer)):
		scheduler = BackgroundScheduler()
		scheduler.add_job(func=timerfunction, trigger="interval", minutes=2)
		scheduler.start()
		GLOBAL_timerStarted = True

def timerfunction():
	#-----------size of queue------------
	print('timer')
	global numberofreq
	global GLOBAL_containerList
	global pidToNameMap
	global global_dict_index
	global client
	global x_client
	#-------check size of queue----------
	no_of_slaves = int(numberofreq/20) if (numberofreq%20 == 0) else int(numberofreq/20) + 1 
	#get the number of current slave compare it with no_of_slaves and scale up and down accordingly
	if(no_of_slaves == 0):
		no_of_slaves = 1
	if(len(GLOBAL_containerList.keys()) <= no_of_slaves):
		t = len(GLOBAL_containerList.keys())
		for i in range(t,no_of_slaves):
			
			GLOBAL_containerList[global_dict_index] = client.containers.run("project_slave1:latest", command =["python3","ms.py","1"], \
			detach=True,network = 'project_default',volumes = {'/home/ubuntu/project/appdata':{'bind': '/app/pdb','mode': 'ro'},'/var/run/docker.sock':{'bind':'/var/run/docker.sock'}})
			time.sleep(5)
			
			try:
				Name = GLOBAL_containerList[global_dict_index].name
			except:
				Name = GLOBAL_containerList[global_dict_index].Name
			
			Pid = x_client.inspect_container(Name)['State']['Pid']
			pidToNameMap[global_dict_index] = {'Name':Name, 'Pid': Pid}
			global_dict_index +=1
	
	elif(len(GLOBAL_containerList.keys()) > no_of_slaves):
		#GLOBAL_containerList[0].stop()
		#GLOBAL_containerList[0].remove()
		while len(GLOBAL_containerList.keys()) > no_of_slaves:
			temp = list(GLOBAL_containerList.keys())
			minPidInd = temp[0]
			for i in range(0,global_dict_index):
				if(i in GLOBAL_containerList.keys() and x_client.inspect_container(GLOBAL_containerList[i].name)['State']['Pid']\
                                >x_client.inspect_container(GLOBAL_containerList[minPidInd].name)['State']['Pid'] ):
					minPidInd = i
			
			if(minPidInd is not -1):
				GLOBAL_containerList[minPidInd].stop()
				GLOBAL_containerList[minPidInd].remove()				
				
				del GLOBAL_containerList[minPidInd]
				del pidToNameMap[minPidInd]
			else:
				break
	
	print(GLOBAL_containerList)
	
		
	numberofreq = 0
	#timer = threading.Timer(2, timerfunction) # Call `timer function` in 2*60 seconds.
	#timer.start()


class RPCClient(object):

	rpcServer = 'readQ'
	body_to_send = j.dumps({})
	corr_id = None
	
	def __init__(self,body,rpcServer):
		
		self.rpcServer = rpcServer
		self.body_to_send = body
		
		self.connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
		
		self.channel = self.connection.channel()
		result = self.channel.queue_declare(queue='responseQ')
		
		self.channel.basic_qos(prefetch_count=0)

		self.channel.basic_consume(on_message_callback=self.callbackResponse, queue='responseQ')
		
	def callbackResponse(self,ch, method, props, body):
		if self.corr_id == props.correlation_id:
			body_ = j.loads(body)
			code = body_["code"]
			if(body_["code"]==400 or body_["code"]==500):
				#self.connection.close()
				#return(body_["code"],body_["msg"])
				self.response = {"code":body_["code"],"msg":body_["msg"]}
			if(code == 200):
				if(body_["action"] == "get_user"):
					self.response = j.dumps({"username":body_["username"],"pswd":body_["pswd"]})
				
				elif(body_["action"] == "upcoming_rides"):
					self.response = body_["response"]
					
				elif(body_["action"] == "ride_info"):
					self.response = body_["response"]
					
				elif(body_["action"] == "list_all_users"):
					self.response = body_["response"]
					
				else:
					self.response = body
					
			elif(code == 201):
				if(body_["action"] == "write_ride"):
					self.response = body_["response"]
				else:
					self.response = body
					
			elif(code == 204):
				self.response = {"code":204}
			
			#self.response = body #fill this big boi
			#print(body)
				
	def call(self):
		self.response = None
		self.corr_id = str(uuid.uuid4())
		self.channel.basic_publish(exchange="", routing_key=self.rpcServer, properties = pika.BasicProperties(reply_to = 'responseQ',correlation_id = self.corr_id),body = self.body_to_send)
		
		while self.response is None:
            		self.connection.process_data_events()
		
		self.connection.close()

		return self.response
		
#-------------------------------------------------------------------------------


#----------------------------------------------------------------------------------------------------------
app = Flask(__name__)

@app.route('/')
def hello():
	return 'Hello World! I have been seen'

@app.route("/api/v1/db/read" , methods={'POST'})
def readAllReq():
	
	global GLOBAL_starttimer 
	global numberofreq 
	GLOBAL_starttimer = True
	numberofreq +=1
	starttimer()
	
	body_ = request.get_json()
	action = body_["action"]

   	#-------------------------HANDLING USER AND RIDE ACTIONS HERE----------------------------------#

	if(action == "get_user"):
		body_["connect_api"] = "user"
		newClient = RPCClient(j.dumps(body_),'readQ')
		res = newClient.call()
		#rpcClientRead(body_)
		if(type(res) == dict):
			if(res["code"]==204):
				return Response(None,status = 204,mimetype='application/json')
			elif(res["code"] == 400 or res["code"] == 400):
				abort(res["code"],res["msg"])
		
		
		return Response(res,status=200, mimetype='application/json')
		
	elif(action == "delete_user"):
		body_["connect_api"]= "user"
		newClient = RPCClient(j.dumps(body_),'readQ')
		res = newClient.call()
		if(type(res) == dict):
			if(res["code"]==204):
				return Response(None,status = 204,mimetype='application/json')
			elif(res["code"] == 400 or res["code"] == 400):
				abort(res["code"],res["msg"])
		
		
		return Response(None,status = 200,mimetype='application/json')
		
	elif(action == "upcoming_rides"):
		body_["connect_api"]= "ride"
		newClient = RPCClient(j.dumps(body_),'readQ')
		res = newClient.call()
		if(type(res) == dict):
			if(res["code"]==204):
				return Response(None,status = 204,mimetype='application/json')
			elif(res["code"] == 400 or res["code"] == 400):
				abort(res["code"],res["msg"])
		
		
		return Response(res,status=200, mimetype='application/json')
		
	elif(action == "ride_info"):
		body_["connect_api"]= "ride"
		newClient = RPCClient(j.dumps(body_),'readQ')
		res = newClient.call()
		if(type(res) == dict):
			if(res["code"]==204):
				return Response(None,status = 204,mimetype='application/json')
			elif(res["code"] == 400 or res["code"] == 400):
				abort(res["code"],res["msg"])
		
		
		return Response(res,status=200 , mimetype='application/json')
		
	elif(action == 'list_all_users'):
		body_["connect_api"]= "user"
		newClient = RPCClient(j.dumps(body_),'readQ')
		res = newClient.call()
		if(type(res) == dict):
			if(res["code"]==204):
				return Response(None,status = 204,mimetype='application/json')
			elif(res["code"] == 400 or res["code"] == 400):
				abort(res["code"],res["msg"])
		
		
		return Response(res,status = 200 , mimetype='application/json')
		


@app.route("/api/v1/db/write" , methods={'POST'})
def writeAllReq():
	body_ = request.get_json()
	action = body_["action"]
	body_["connect_api"]= "user"
	
	#-------------------------HANDLING USER AND RIDE ACTIONS HERE----------------------------------#
	#return Response("Nigga")
	if(action == "deldbuser"):
		body_["connect_api"]= "user"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(None , status=200 , mimetype='application/json')

	elif(action == "deldbride"): #add in ride aws
		body_["connect_api"]= "ride"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(None , status=200 , mimetype='application/json')
		
	elif(action == "write_user"):
		body_["connect_api"]= "user"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(None,mimetype="application/json", status=201)
		
	elif(action == "delete_user"):
		body_["connect_api"]= "user"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(None, mimetype="application/json", status=200)
		
	elif(action == "write_ride"):
		body_["connect_api"]= "ride"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(res, status=201, mimetype='application/json')
		
	elif(action == "join_ride"):
		body_["connect_api"]= "ride"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(None, mimetype="application/json", status=200)
		
	elif(action == "delete_ride"):
		body_["connect_api"]= "ride"
		newClient = RPCClient(j.dumps(body_),'writeQ')
		res = newClient.call()
		return Response(None, mimetype="application/json", status=200)
		
@app.route("/api/v1/db/clear" , methods = {'POST'})
def clearalldb():
	body_ = {}
	body_["action"] = "deldbuser"
	body_["connect_api"]= "user"
	newClient = RPCClient(j.dumps(body_),'writeQ')
	res = newClient.call()
	return Response(None, mimetype="application/json", status=200)

@app.route("/api/v1/worker/list" , methods={'GET'})
def getWorkers():
	#-------------------------------
	
	global GLOBAL_containerList
	global global_dict_index
	global x_client
	
	#-------------------------------
	workerList = []
	for i in range(global_dict_index):
		if(i in GLOBAL_containerList.keys()):
			workerList.append(x_client.inspect_container(GLOBAL_containerList[i].name)['State']['Pid'])
	
	workerList.sort()
	return Response(j.dumps(workerList),mimetype="application/json", status=200)


@app.route("/api/v1/crash/slave" , methods={'POST'})
def crashSlave():
	#-------------------------------
	
	global GLOBAL_containerList
	global pidToNameMap
	global global_dict_index
	global client
	global x_client
	
	#-------------------------------
	ans = []
	temp = list(GLOBAL_containerList.keys())
	if len(temp) == 0:
		maxPidInd = -1
	else:
       		maxPidInd = temp[0]

	for i in range(0,global_dict_index):
		if(i in GLOBAL_containerList.keys() and x_client.inspect_container(GLOBAL_containerList[i].name)['State']['Pid'] > x_client.inspect_container(GLOBAL_containerList[maxPidInd].name)\
		['State']['Pid'] ):
			maxPidInd = i
	
	if(maxPidInd is not -1):
		ans.append(x_client.inspect_container(GLOBAL_containerList[maxPidInd].name)['State']['Pid'])
		GLOBAL_containerList[maxPidInd].stop()
		GLOBAL_containerList[maxPidInd].remove()				
		
		del GLOBAL_containerList[maxPidInd]
		del pidToNameMap[maxPidInd]
		
		
		thread = threading.Thread(target=startSlave, args=())
		thread.daemon = True                            # Daemonize thread
		thread.start() 
		#ans.append(str(maxPidInd))
		return Response(j.dumps(ans),mimetype="application/json", status=200)
	
	else:
		return Response(j.dumps(ans),mimetype="application/json", status=200)
	

if __name__ == '__main__':
	app.run(host='0.0.0.0',port=5000,debug=True,use_reloader=False)
	
