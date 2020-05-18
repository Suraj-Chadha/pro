import pika
import json
import os

import sqlalchemy as sql
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session,sessionmaker,aliased,relationship
import time
import socket
import logging
import docker
from kazoo.client import KazooClient
from kazoo.client import KazooState
from datetime import datetime

#-------------WAIT FOR RABBIT MQ TO START-------------------------
try:
	credentials = pika.PlainCredentials('guest', 'guest')
	parameters = pika.ConnectionParameters('rabbitmq') #'127.0.0.1',5672,'/',credentials
	connection = pika.BlockingConnection(parameters)
	channel = connection.channel()
	channel2 = connection.channel()
	channel.queue_declare(queue='writeQ')
	channel2.queue_declare(queue='syncQ')
except:
	time.sleep(30)
finally:
	credentials = pika.PlainCredentials('guest', 'guest')
	parameters = pika.ConnectionParameters('rabbitmq') #'127.0.0.1',5672,'/',credentials
	connection = pika.BlockingConnection(parameters)
	channel = connection.channel()
	channel2 = connection.channel()
	channel.queue_declare(queue='writeQ')
	channel2.queue_declare(queue='syncQ')
#-----------------------------------------------------------------

#---------------------------------------ZOOKEEPER STUFFF MY NIGGA-------------------------------------------------
##################################################################################################################
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

#if(zk.exists("/Workers")):
#	zk.delete("/Workers",recursive = True)

zk.ensure_path("/Master/")



#------------------------GETTING THE CONTAINER PID AND CREATE A NODE------------------

currentContainer = socket.gethostname()

client = docker.from_env()
client = docker.DockerClient(base_url='unix://var/run/docker.sock')
currContainerObject = client.containers.get(currentContainer)
currContainerName = currContainerObject.name

x_client = docker.APIClient(base_url='unix://var/run/docker.sock')	
currcontainerpid = x_client.inspect_container(currContainerName)['State']['Pid']

nodeName = 'worker-' + str(currcontainerpid)
print(nodeName)
zk.create('/Master/'+ nodeName,nodeName.encode('utf-8'))
#-----------------------------------------------------------------------------------------------------------------#
###################################################################################################################


Base = declarative_base()

class UserTable(Base):
    __tablename__ = 'user_table'
    user_id = sql.Column(sql.Integer , autoincrement  = True, primary_key = True )
    user_name = sql.Column(sql.String(40), unique = True, nullable = False)
    user_password = sql.Column(sql.String(40), nullable = False)

    User_Rides = orm.relationship("RideTable" , cascade = "all,delete")
    Ongoing_rides = orm.relationship("RideUsersTable" , cascade = "all,delete")
    
    def write_to_db(self):
        s.add(self)
        s.commit()
    
    def read_from_db(self):
        s.delete(self)
        s.commit()
    
    def delete_from_db(self):
        s.delete(self)
        s.commit()
    
    @staticmethod
    def query_db(name_user):
        #return the User_class table
        obj = s.query(UserTable)
        #returns one if username exists, otherwise zero.
        unames = obj.filter(UserTable.user_name == name_user).one_or_none() 
        return unames

    @staticmethod
    def query_me():
        return s.query(UserTable, UserTable.user_name ).all()

    @staticmethod
    def get_from_request(body):
        return UserTable(user_name=body['username'], user_password=body['password'])
    
    @staticmethod
    def get_json(body):
        if 'username' not in body:
            responseQueueFill({"code":400, "msg":'username is not present'})
        if 'password' not in body:
            responseQueueFill({"code":400, "msg":'password is not present'})
        else:
            return {"username" : body["username"] , "password" : body["password"]}


class RideTable(Base):
    __tablename__ = 'ride_user_table'
    ride_id = sql.Column(sql.Integer, primary_key=True, autoincrement=True)
    created_by = sql.Column(sql.String(80), sql.ForeignKey(
        "user_table.user_name"), nullable=False)
    source = sql.Column(sql.Integer, nullable=False)
    destination = sql.Column(sql.Integer, nullable=False)
    timestamp = sql.Column(sql.DateTime, nullable=False)
    ride_users = orm.relationship("RideUsersTable", cascade="all,delete")

    def write_to_db(self):
        s.add(self)
        s.commit()
        return self.ride_id

    def delete_from_db(self):
        s.delete(self)
        s.commit()

    @staticmethod
    def list_upcoming_rides(source, destination):
        return s.query(RideTable).filter(RideTable.source == source)\
        .filter(RideTable.destination == destination)\
        .filter(RideTable.timestamp >= datetime.now())\
        .all()

    @staticmethod
    def read_from_db(ride_id):
        return s.query(RideTable).get(ride_id)

    @staticmethod
    def read_from_username(username):
        return s.query(RideTable).filter(RideTable.created_by == username).one_or_none()

    @staticmethod
    def get_from_ride_id(ride_id):
        return s.query(RideTable).filter(RideTable.ride_id == ride_id).one_or_none()        

    @staticmethod
    def validateSrc(src):
        if(int(src) >=1 and int(src) <=198):
            return src
        else:
            return "error"
    
    @staticmethod
    def validateDst(dst):
        if(int(dst) >=1 and int(dst) <=198):
            return dst
        else:
            return "error"

    @staticmethod
    def validateTimestamp(tz):
        try:
            return datetime.strptime(tz, "%d-%m-%Y:%S-%M-%H")
        except:
            responseQueueFill({"code":400, 'msg':"invalid timestamp %s format. format is DD-MM-YYYY:SS-MM-HH" % (tz)})
        return tz

    
    @staticmethod
    def get_from_request(body):
        return RideTable(created_by=body['username'],\
            source=RideTable.validateSrc(body['source']),\
                destination=RideTable.validateDst(body['destination']),\
                    timestamp=RideTable.validateTimestamp(body['timestamp']))
    
    @staticmethod
    def get_json(body):
        if 'destination' not in body:
            responseQueueFill({"code":400, "msg": 'destination not passed in the request'})
        if 'timestamp' not in body:
            responseQueueFill({"code":400, "msg": 'timestamp not passed in the request'})
        if 'created_by' not in body:
            responseQueueFill({"code":400, "msg": 'created_by not passed in the request'})
        if 'source' not in body:
            responseQueueFill({"code":400, "msg": 'source not passed in the request'})
        else :
            RideTable.validateTimestamp(body['timestamp'])

            json_dict = {"username" : body['created_by'],\
                "source" : RideTable.validateSrc(body['source']),\
                    "destination" : RideTable.validateDst(body['destination']),\
                        "timestamp" : body['timestamp']}
				
            return json_dict



class RideUsersTable(Base):
    __tablename__ = 'ride_users_table'
    ride_users_id = sql.Column(sql.Integer, primary_key=True, autoincrement=True)
    ride_table_id = sql.Column(sql.Integer, sql.ForeignKey("ride_user_table.ride_id"), nullable=False)
    user_table_name = sql.Column(sql.String(40), sql.ForeignKey(
        "user_table.user_name"), nullable=False)

    @staticmethod
    def read_from_db(ride_id):
        return s.query(RideUsersTable).filter(RideUsersTable.ride_table_id == ride_id).all()

    def write_to_db(self):
        s.add(self)
        s.commit()
        return self.ride_table_id

    def delete_from_db(self):
        s.delete(self)
        s.commit()


def check_content_type(req):
    if not req.is_json:
        responseQueueFill({"code": 400, "msg": 'Content-Type unrecognized'})

def myconverter(o):
    if isinstance(o, datetime.datime):
        return o.__str__()

engine =  sql.create_engine("sqlite:///{}".format(os.path.join(os.path.dirname(os.path.abspath(__file__)),"riders.db")))
Base.metadata.create_all(bind=engine)
Session = sessionmaker(bind=engine)
s = Session()



#-----------------------PIKA STUFF--------------------------

def responseQueueFill(body,ch,properties,method):
	json_body = json.dumps(body)
	ch.basic_ack(delivery_tag=method.delivery_tag)
	ch.basic_publish(exchange="", routing_key='responseQ',properties=pika.BasicProperties(correlation_id = properties.correlation_id),body=json_body)
	
def syncQfill(properties,body):
	global channel2
	ch = channel2
	ch.basic_publish(exchange="", routing_key='syncQ',body = body)
	
#----------------------Reading from ReadQ--------------------





#----------------Function for opening queue-----------   R.E.A.D  ---------------------------------------------------------

def callback1(ch, method, properties, body):
	print("callback1 function working")
	body_ = json.loads(body)
	connect_api = body_["connect_api"]
	#if(connect_api=="user"):
	action = body_["action"]
	#-------------------HANDLE DB ACTIONS--------------------
	
	if(action == "get_user"):
		ret_val = UserTable.query_db(body_["username"])
		if ret_val is None:
		        bodyE = {"code":400,"msg":"user not found"} #bodyE code error
		        responseQueueFill(bodyE,ch,properties,method)
		else:
		        d = {"username" : ret_val.user_name , "pswd" : ret_val.user_password , "action":action ,"code":200}
		        responseQueueFill(d,ch,properties,method)
			
	#-------------------------------------------------------------
						
	elif(action == "delete_user"):
		ret_val = UserTable.query_db(body_["username"])
		if ret_val is None:
		        bodyE = {"code":400,"msg":"user not found"} #bodyE code error
		        responseQueueFill(bodyE,ch,properties,method)
		else:
			#UserTable.query_db(body["username"]).delete_from_db()
			json_body = {"action":"delete_user","username":ret_val.user_name , "code":200}
			responseQueueFill(json_body,ch,properties,method)
	        		
	#-------------------------------------------------------------
						
	elif(action == "upcoming_rides"):
	    rides  = RideTable.list_upcoming_rides(body_["source"] , body_["destination"])
	    response = list()
	    if(rides is not None and len(rides) > 0):
	        for ride in rides:
	            users = list()
	            for u in RideUsersTable.read_from_db(ride.ride_id):
	                users.append(u.user_table_name)
	            response.append({"rideId": ride.ride_id, "username": users,
	                             "timestamp": ride.timestamp.strftime("%d-%m-%Y:%S-%M-%H")})
	    
	        json_body = {"code":200,"action":"upcoming_rides","response":json.dumps(response, indent=4)} #already jsonified
	        responseQueueFill(json_body,ch,properties,method)
	       
	    else:
	        bodyE = {"code":204,"msg":None} #bodyE code error
	        responseQueueFill(bodyE,ch,properties,method)
	             		
	#-------------------------------------------------------------
						
	elif(action == "ride_info"):
		try:
			ride = RideTable.read_from_db(body_["ride_id"])
			if(ride is None):
			    bodyE = {"code":204,"msg":None} #bodyE code error
			    responseQueueFill(bodyE,ch,properties,method)
			else:
			    users = list()
			    all_riders = RideUsersTable.read_from_db(ride.ride_id)
			    for u in all_riders:
			        users.append(u.user_table_name)
			    #obj = RideTable.get_from_ride_id(ride.ride_id)
			    response = dict()
			    response["rideId"] = ride.ride_id
			    response["created_by"] = ride.created_by
			    response["users"] = users
			    response["timestamp"] = ride.timestamp.strftime("%d-%m-%Y:%S-%M-%H")
			    response["source"] = ride.source
			    response["destination"] = ride.destination
			    
			    responseQueueFill({"code":200, "action":"ride_info", "response":json.dumps(response , indent=4)},ch,properties,method)
		except:
	    		responseQueueFill({"code":500,"msg":"Internal Server Error"},ch,properties,method)
	    			
	#-------------------------------------------------------------
						
	elif(action == 'list_all_users'):
	    ret_val = UserTable.query_me()
	    response = list()
	    for i in ret_val:
	        response.append(i[1])
	    responseQueueFill({"code":200, "action":"list_all_users", "response":json.dumps(response , indent=4)},ch,properties,method)
	        			
	#-------------------------------------------------------------
						


	print(" [x] Received CallBack1 %r \n" % body, body_["action"])



#----------------Function for opening queue-----------   WRITE  ---------------------------------------------------------


def callback2(ch, method, properties, body):
	body_ = json.loads(body)
	connect_api = body_["connect_api"]
	action = body_["action"]
	
	syncQfill(properties,body)

	if(action == "write_user"):
		UserTable.get_from_request(body_).write_to_db()
		responseQueueFill({"msg": None, "code":201, "action":"write_user"}, ch, properties,method)
		

	elif(action == "delete_user"):
		UserTable.query_db(body_["username"]).delete_from_db()
		responseQueueFill({"msg": None, "code":200, "action":"delete_user"}, ch, properties,method)


	elif(action == "write_ride"):
		ride_request = RideTable.get_from_request(body_)
		ride_id = ride_request.write_to_db()
		ride_table_id = RideUsersTable(user_table_name=ride_request.created_by, ride_table_id=ride_id).write_to_db()
		responseQueueFill({"response": json.dumps({"ride_id": ride_id}), "code":201, "action":"write_ride"},ch,properties,method)

	elif(action == "join_ride"):
		RideUsersTable(user_table_name=body_["username"], ride_table_id=body_["ride_id"]).write_to_db()
		responseQueueFill({"msg": None, "code":200, "action":"join_ride"}, ch, properties,method)

	elif(action == "delete_ride"):
		RideTable.read_from_db(body_['ride_id']).delete_from_db()
		responseQueueFill({"msg": None, "code":200, "action":"delete_ride"}, ch, properties,method)

	elif(action == "deldbuser" or action == "deldbride"):
		num_rows_deleted = s.query(UserTable).delete()
		s.commit()
		num_rows_deleted = s.query(RideTable).delete()
		s.commit()
		num_rows_deleted = s.query(RideUsersTable).delete()
		s.commit()
		responseQueueFill({"msg": None, "code":200, "action":action},ch,properties,method)
		
	#----------------------------------------------------------------
	
	print(" [x] Received %r \n" % body, body_["action"])



channel.basic_qos(prefetch_count = 1)
channel.basic_consume(on_message_callback = callback2, queue = 'writeQ') #,  no_ack=True
print(' [*] Waiting for -----WRITE---- messages. To exit press CTRL+C')
channel.start_consuming()


