from flask import Flask
import os
import flask
import json as j
import pika
import uuid

import docker
import time

#import "./master-slave/Dockerfile" as DockerFile

# from flask_sqlalchemy_session import current_session as s
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import scoped_session,sessionmaker,aliased,relationship

# from datetime import datetime
# import requests
# import traceback
# import hashlib
# import re as regex

GLOBAL_CORR_ID = 0
GLOBAL_starttimer = False
GLOBAL_timerStarted = False
numberofreq = 0
#--------------------------------------pika stuff----------------------------------------------------------
'''def callback(ch, method, properties, body):
	ch.stop_consuming()
	print(" [x] Received CallBack1 %r \n" % body)

def responseQRead(action):
	#json_body = j.dumps(body_)
	connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq'))
	channel = connection.channel()
	res = channel.queue_declare(queue="responseQ")
	while(res.method.message_count == 0):
		print("waiting") #wait
	if(res.method.message_count > 0):
		channel.basic_consume(callbackStop, 'responseQ')'''

#--------------Connection to read/write queue-------------------------------------

#def callbackResponse(ch, method, properties, body):


client = docker.from_env()
client = docker.DockerClient()
x_client = docker.APIClient()
#x = client.containers.run("project_slave1", command =["python3","ms.py","1"], detach=True,network = 'project_default',volumes = {'/home/kaustubh/project/appdata':{'bind': '/app/pdb'}}) #, 
#time.sleep(10)

x = client.containers.run("mockserver/mockserver","echo new")
print(x)


