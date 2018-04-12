#!/usr/bin/python
import os
import time
import pymssql
import argparse
import subprocess
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine


class AutomateDataAnalysis():
	def __init__(self):
		# excel file path
		self.excelPath = os.getcwd()
		self.date_format = "%Y-%m-%d"
		self.days10yrs = 365 * 10
		self.rowList   = []
		self.existinprod = []
		self.notexistinprod = []
		self.datestatus = []
		self.newdates = []
		
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

		for file in files:
			if 'excel' in file:
				try:
					print "reading from excel file '{}' started...".format(file)
					excelDF = pd.read_excel(os.path.join(path, file), sheetname = "Sheet1")
					print "reading from excel file '{}' completed...".format(file)

					# make database entry by using pandas
					#self.makeDatabseEntryByPandas(excelDF)

					print "extracting fields from excel file '{}'...".format(file)
					for index, row in excelDF.iterrows():
						dt = str(row['dt'])[:10]
						indx_nm = str(row['indx_nm'])
						indx_val = row['indx_val']
						create_ts = str(row['create_ts'])[:10]
						create_user_id = str(row['create_user_id'])

						self.rowList.append((dt, indx_nm, indx_val, create_ts, create_user_id))
				except Exception as e:
					print "readExcel(), e: {}".format(e)


	def populateCDXDelta(self):
		#need to get details from table CDX to run this stored procedure
		# cur.execute("EXEC p_populate_CDX_delta @bus_dt = '{}', @liq_period = {}, @look_back_period = {}".format(bus_dt, liq_period, look_back_period))

		pass

	def compareExcelDates(self):
		print "comparing dates from excel file ..."
		excelFile = os.path.join(os.getcwd(), 'compare_file.xlsx') 
		#print "excelFile: ", excelFile
		compDF = pd.read_excel(excelFile, sheetname = "Sheet1", dayfirst=True)

		#print compDF.dtypes
		#print compDF
		
		for index, row in compDF.iterrows():
			# dateinprod scendatesgenera
			if any(compDF.scendatesgenera == row['dateinprod']):
				self.existinprod.append('Exists')
				self.datestatus.append('NA')
			else:
				self.existinprod.append('Not Exists')
				dt = str(row['dateinprod'])[:10]
				now = str(datetime.now())[:10]
				nowdate = datetime.strptime(now, self.date_format)
				back10ydate = datetime.strptime(dt, self.date_format)
				delta = nowdate - back10ydate
				

				if (delta.days > self.days10yrs):
					self.datestatus.append('Legacy')
				elif (delta.days <= self.days10yrs and delta.days > 0):
					self.datestatus.append('Retired')
				else:
					self.datestatus.append('NA')

			if any(compDF.dateinprod == row['scendatesgenera']):
				self.notexistinprod.append('Exists')
				self.newdates.append('NA')
			else:
				self.notexistinprod.append('Not Exists')
				self.newdates.append('Newdate')

		compDF['existinprod'] = self.existinprod
		compDF['notexistinprod'] = self.notexistinprod
		compDF['datestatus'] = self.datestatus
		compDF['newdates'] = self.newdates

		#print compDF
		# write to excel file
		writer = pd.ExcelWriter(excelFile, engine='xlsxwriter', date_format='mmm/dd/yyyy')
		compDF.to_excel(writer, sheet_name='Sheet1', index=False)
		writer.save()
		print "dates comparision has completed ..."


	def makeDatabseEntryByPandas(self, excelDF):
		try:
			mydb = create_engine('mssql+pymssql://' + self.username + ':' + self.password + '@' + self.hostname + ':' + str(self.port) + '/' + self.dbname , echo=False)
			excelDF.to_sql(name="CDX", con=mydb, if_exists = 'append', index=False)
		except Exception as e:
			print "makeDatabseEntryByPandas(), e: {}".format(e)
		else:
			mydb.close()


	def makeDatabseEntry(self):
		# get database connection, cursor
		print "getting database connection ..."
		con, cur = self.getConnection()

		try:
			#print "DATA:   ", self.rowList
			print "inserting records to database ..."
			cur.executemany("INSERT INTO CDX VALUES (%s, %s, %d, %s, %s)", self.rowList)
			cur.execute("COMMIT")
		except Exception as e:
			print "makeDatabseEntry(), e: {}".format(e)
		else:
			# close database connection
			con.close()


	def getConnection(self):
		con = pymssql.connect(
					self.hostname,
					self.username,
					self.password,
					self.dbname,
					self.port)

		cursor = con.cursor()

		return con, cursor


	def main(self):
		print "********************************************************"
		print "starting automate process ..."
		print "********************************************************"
		stime = time.time()
		self.getArgument()
		self.readExcel()
		self.makeDatabseEntry()
		self.populateCDXDelta()
		self.compareExcelDates()
		etime = time.time()
		ttime = etime - stime
		
		print "********************************************************"
		print "time consumed to complete the process: {}".format(ttime)
		print "completing automate process ..."
		print "********************************************************"


if __name__ == '__main__':
	ada = AutomateDataAnalysis()
	ada.main()
	