import os
import flask
import json as j

import sqlalchemy as sql
import sqlalchemy.orm as orm
import flask_sqlalchemy_session as fss
from flask import abort, Response, request

from flask_sqlalchemy_session import current_session as s
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session,sessionmaker,aliased,relationship

from datetime import datetime
import requests
import traceback
import hashlib
import re as regex
import pika

Base = declarative_base()
app = flask.Flask(__name__)

userCalls = 0
#s = flask_scoped_session(session_factory, app)

#e = sql.create_engine("sqlite:///{}".format(os.path.join(os.path.dirname(os.path.abspath(__file__)),"riders.db")))
#Base.metadata.create_all(e,checkfirst=True)
#sf = orm.sessionmake(bind=e)
#session = fss.flask_scoped_session(sf,app)

#scp -i assignment1.pem -r Assignment1 user@ec2-3-93-153-150.compute-1.amazonaws.com:~

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
            abort(400, 'username is not present')
        if 'password' not in body:
            abort(400, 'password is not present')

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
            abort(400, "invalid timestamp %s format. format is DD-MM-YYYY:SS-MM-HH" % (tz))
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
            abort(400, 'destination not passed in the request')
        if 'timestamp' not in body:
            abort(400, 'timestamp not passed in the request')
        if 'created_by' not in body:
            abort(400, 'created_by not passed in the request')
        if 'source' not in body:
            abort(400, 'source not passed in the request')

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
        abort(400, 'Content-Type unrecognized')

def myconverter(o):
    if isinstance(o, datetime.datime):
        return o.__str__()
@app.route("/")
def hello():
        abort(405, "Method not allowed")

@app.route("/api/v1/users", methods={'PUT'})
def add_user():
    

    check_content_type(request)

    json_format = UserTable.get_json(request.json)
    json_format["action"] = "get_user"
    r = requests.post("http://localhost:8080/api/v1/db/read" , json = json_format)

    if(r.status_code == 200):
        abort(400 , 'user %s already exists' % (json_format['username']))

    else:
        json_format["action"] = "write_user"
        password = json_format["password"]
        tryi = regex.compile("^[a-fA-F0-9]{40}$")
        if(tryi.search(password) is None):
            abort(400,"Incorrect")

        r = requests.post("http://localhost:8080/api/v1/db/write" , json = json_format)
        if(r.status_code == 201):
            userCalls +=1
            return Response(j.dumps({}), status=201, mimetype='application/json')
        else:
            abort(500 , "Internal Server Error")


@app.route("/api/v1/users/<user_name>", methods={'DELETE'})
def delete_user(user_name):

    #check_content_type(request)
    
    json_format = dict()
    json_format["username"] = user_name
    json_format["action"] = "delete_user"

    r = requests.post("http://localhost:8080/api/v1/db/read" , json = json_format)

    if(r.status_code == 400): #user was not found
        abort(400 , 'user %s does not exists' % (user_name))
    else:
        r = requests.post("http://localhost:8080/api/v1/db/write" , json = json_format)
        if(r.status_code == 200):
            userCalls +=1
            return Response(j.dumps({}), status=200, mimetype='application/json')
        else:
            abort(500 , "Internal Server Error")

@app.route("/api/v1/users" , methods = {'GET'})
def list_all_users():
    userCalls +=1

    json_format = dict()
    json_format["action"] = "list_all_users"
    r = requests.post("http://localhost:8080/api/v1/db/read" , json = json_format)
    dic = j.loads(r.text)
    return Response(j.dumps(dic , indent=4 ),  status=200 , mimetype='application/json')

@app.route("/api/v1/_count" , methods = {'GET'})
def getCount():
    returnable = [userCalls]
    return returnable

# @app.route("/api/v1/_count" , methods = {'DELETE'})
# def getCount():
#     userCalls = 0
#     return {}

@app.route("/api/v1/db/clear" , methods = {'POST'})
def del_db():
    userCalls +=1

    num_rows_deleted = s.query(UserTable).delete()
    s.commit()
    num_rows_deleted = s.query(RideTable).delete()
    s.commit()
    num_rows_deleted = s.query(RideUsersTable).delete()
    s.commit()
    return Response(None , status=200 , mimetype='application/json') 

#ALL READ WRITE API CALLS FROM HERE:

@app.route("/api/v1/db/read" , methods={'POST'})
def read():
    body = request.get_json()
    action = body["action"]
    connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='readQ')

    channel.basic_publish(exchange='', routing_key='readQ', body='Hello World')
    connection.close()

    if(action == "get_user"):
        ret_val = UserTable.query_db(body["username"])
        if ret_val is None:
            abort(400, "user not found")
        d = {"username" : ret_val.user_name , "pswd" : ret_val.user_password}
        return Response(j.dumps(d), status=200, mimetype='application/json')
    
    elif(action == "delete_user"):
        ret_val = UserTable.query_db(body["username"])
        if(ret_val is None):
            abort(400 , "not found")
        return Response(None , status= 200 , mimetype='application/json')
    
    elif(action == "upcoming_rides"):
        rides  = RideTable.list_upcoming_rides(body["source"] , body["destination"])
        response = list()
        if(rides is not None and len(rides) > 0):
            for ride in rides:
                users = list()
                for u in RideUsersTable.read_from_db(ride.ride_id):
                    users.append(u.user_table_name)
                response.append({"rideId": ride.ride_id, "username": users,
                                 "timestamp": ride.timestamp.strftime("%d-%m-%Y:%S-%M-%H")})
            return Response(j.dumps(response, indent=4), status=200, mimetype='application/json')
        else:
            return Response(None, status=204, mimetype='application/json')
    
    elif(action == "ride_info"):
        try:
            ride = RideTable.read_from_db(body["ride_id"])
            if(ride is None):
                return Response(None , status=204 , mimetype='application/json')
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
                return Response(j.dumps(response , indent=4) , status=200 , mimetype='application/json')
        except:
            abort(500 , "Internal Server error")
    
    elif(action == 'list_all_users'):
        ret_val = UserTable.query_me()
        response = list()
        for i in ret_val:
            response.append(i[1])
        return Response(j.dumps(response , indent=4) , status = 200 , mimetype='application/json')




    
@app.route("/api/v1/db/write" , methods={'POST'})
def write():
    body = request.get_json()
    action = body["action"]
    if(action == "write_user"):
        UserTable.get_from_request(body).write_to_db()
        return Response(None, mimetype="application/json", status=201)


    elif(action == "delete_user"):
        UserTable.query_db(body["username"]).delete_from_db()
        return Response(None, mimetype="application/json", status=200)


    elif(action == "write_ride"):
        ride_request = RideTable.get_from_request(body)
        ride_id = ride_request.write_to_db()#Returns the incremented ride id.
        ride_table_id = RideUsersTable(user_table_name=ride_request.created_by, ride_table_id=ride_id).write_to_db()
        return Response(j.dumps({"ride_id": ride_id}), status=201, mimetype='application/json')

    elif(action == "join_ride"):
        RideUsersTable(user_table_name=body["username"], ride_table_id=body["ride_id"]).write_to_db()
        return Response(None, status=200, mimetype='application/json')
    
    elif(action == "delete_ride"):
        RideTable.read_from_db(body['ride_id']).delete_from_db()
        return Response(None, status=200, mimetype='application/json')


e = sql.create_engine("sqlite:///{}".format(os.path.join(os.path.dirname(os.path.abspath(__file__)), "riders.db")))
sf = orm.sessionmaker(bind=e)
session = fss.flask_scoped_session(sf, app)

Base.metadata.create_all(e, checkfirst=True)

if __name__ == "__main__":
    '''e = sql.create_engine("sqlite:///{}".format(
        os.path.join(os.path.dirname(os.path.abspath(__file__)), "riders.db")))
    Base.metadata.create_all(e, checkfirst=True)    
    sf = orm.sessionmaker(bind=e)
    session = fss.flask_scoped_session(sf, app)'''
    app.run(host='127.0.0.1',port=8080 , debug=True)