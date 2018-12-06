import MySQLdb as sql
from pandas import DataFrame


class SqlQuerry:
    def __init__(self,host,user,password,db):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.con = None
        self.cursor = None

    def Connect(self):
        try:
            self.con = sql.connect(host=self.host, user=self.user, password=self.password, db=self.db)
            self.cursor = self.con.cursor()
            self.con.set_character_set('utf8')
            self.cursor.execute('SET NAMES utf8;')
            self.cursor.execute("SET CHARACTER SET utf8;")
            self.cursor.execute('SET character_set_connection=utf8;')
        except Exception as e:
            self.Connect()
            print('[Connect]',e)
            return
        return self

    def Querry(self,querry):
        if querry is None:
            raise ImportError("Query not None")
            return
        try:
            if self.con.open == 0:
                self.Connect()
            self.cursor.execute(querry)
            self.con.close()
        except Exception as ex:
            self.Connect()
            raise ImportWarning('[Querry]',ex)
        return self.cursor

    def QueryValues(self,query,Values,commit=True):
        if query is None:
            raise ImportError("Query not None")
            return
        try:
            if self.con.open == 0:
                self.Connect()
            self.cursor.execute(query,args=Values)
            if commit:
                self.con.commit()
            self.con.close()
        except Exception as ex:
            # self.Connect()
            raise ImportWarning('[QueryValues]',ex)
            return
        return self.cursor

    def CallProcedure(self,procedure: str, parameter=None, commit=False):
        if procedure is None:
            raise ImportError("Query not None")
            return
        try:
            if self.con.open == 0:
                self.Connect()
            if parameter:
                self.cursor.callproc(procname=procedure, args=parameter)
            else:
                self.cursor.callproc(procname=procedure)
            if commit:
                self.con.commit()
            self.con.close()
        except Exception as ex:
            # self.Connect()
            raise ImportWarning('[CallProcedure]',ex)
            return
        return self.cursor