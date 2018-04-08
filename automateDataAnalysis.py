#!/usr/bin/python
import os
import time
import pymssql
import argparse
import subprocess
import numpy as np
import pandas as pd
from sqlalchemy import create_engine


class AutomateDataAnalysis():
	def __init__(self):
		# excel file path
		self.excelPath = '/home/krishna/dev_coding/python/flask/PROJECTS/'
		self.rowList   = []
		
		# credentials
		self.hostname = 'localhost'
		self.username = 'SA'
		self.password = 'cybage@123'
		self.dbname   = 'TestDB'
		self.port     = '1433'


	def getArgument(self):
		pass


	def readExcel(self):
		path = self.excelPath

		# get all files under given path
		files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
		#print files
		for file in files:
			if 'excel' in file:
				try:
					print "excel file '{}'' reading started...".format(file)
					excelDF = pd.read_excel(os.path.join(path, file), sheetname = "Sheet1")

					# make database entry by using pandas
					#self.makeDatabseEntryByPandas(excelDF)

					#rowList = []
					for index, row in excelDF.iterrows():
						#print index
						#print row['dt']
						dt = str(row['dt'])[:10]
						indx_nm = str(row['indx_nm'])
						indx_val = row['indx_val']
						create_ts = str(row['create_ts'])[:10]
						create_user_id = str(row['create_user_id'])

						self.rowList.append((dt, indx_nm, indx_val, create_ts, create_user_id))
				except Exception as e:
					print "Exception in reading the excel file {}, e: {}".format(file, e)


	def runStoredProcedure(self):
		pass


	def makeDatabseEntryByPandas(self, excelDF):
		try:
			mydb = create_engine('mssql+pymssql://' + self.username + ':' + self.password + '@' + self.hostname + ':' + str(self.port) + '/' + self.dbname , echo=False)
			excelDF.to_sql(name="CDX", con=mydb, if_exists = 'append', index=False)
		except Exception as e:
			print "makeDatabseEntryByPandas(): e, {}".format(e)
		else:
			mydb.close()


	def makeDatabseEntry(self):
		# get database connection, cursor
		con, cur = self.getConnection()

		try:
			cur.executemany("INSERT INTO CDX VALUES (%s, %s, %d, %s, %s)", self.rowList)
			cur.execute("COMMIT")
		except Exception as e:
			print "error in databease e: {}".format(e)
		else:
			# close database connection
			con.close()


	def getConnection(self):
		con = pymssql.connect(  self.hostname,
								self.username,
								self.password,
								self.dbname,
								self.port)

		cursor = con.cursor()

		return con, cursor


	def main(self):
		print "********************************************************"
		print "starting automate process..."
		print "********************************************************"
		stime = time.time()
		self.getArgument()
		self.readExcel()
		self.makeDatabseEntry()
		self.runStoredProcedure()
		etime = time.time()
		ttime = etime - stime
		
		print "********************************************************"
		print "time consumed to complete the process: {}".format(ttime)
		print "completing automate process..."
		print "********************************************************"


if __name__ == '__main__':
	ada = AutomateDataAnalysis()
	ada.main()
