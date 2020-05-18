import pika
import json
import os

import sqlalchemy as sql
import sqlalchemy.orm as orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session,sessionmaker,aliased,relationship
import time
import threading





#------------------------------------------------------------------
Base = declarative_base()

class UserTable_(Base):
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
    def query_everything():
        return s.query(UserTable).all()
        
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


class RideTable_(Base):
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

