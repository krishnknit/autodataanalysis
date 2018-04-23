#!/usr/bin/python
import os
import time
import pymssql
import logging
import ConfigParser
import numpy as np
import pandas as pd


class GenerateExcel(object):
	def __init__(self):
		## excel file path
		self.configFilePath = 'genExcel.cfg'
		self.excelPath = os.getcwd()
		self.rank = []
		#self.df = pd.DataFrame(columns=['Jan_Member_name', 'C1', 'C2', 'C3', 'C4', 'C5', 'Feb_Member_name', 'c1', 'c2', 'c3', 'c4', 'c5'])
		self.df = pd.DataFrame()


	def getData(self):
		## get database connection, cursor
		logging.info("getting database connection ...")
		
		## Excute Query here
		sql = "SELECT * FROM exceltbl"
		
		try:
			#con = self.getConnection()
			con = pymssql.connect('localhost', 'SA', 'cybage@123', 'TestDB', '1443')
			self.df = pd.read_sql(sql, con)
		except Exception as e:
			logging.error("getData(), unable to read data, e: {}".format(e))
		else:
			## close database connection
			con.close()
			logging.info("data fetched successfully ...")

			## sort on column c3
			logging.info("sorting data in descending order ...")
			self.df.sort_values(by='C3', ascending=False, inplace = True)
			
		# self.df.loc[0] = ['a', '04/04/2018', 4, 6, 7, 8, 'a','04/04/2018', 4, 6, 7, 8]
		# self.df.loc[1] = ['b', '04/04/2018', 41, 61, 71, 81, 'b','04/04/2018', 44, 66, 77, 88]


	def getPercentage(self):
		## calculate % on integer coluns
		logging.info("calculating % on integer columns ...")
		self.df['c2_pct'] = (self.df['C2'] - self.df['fc2'])/self.df['C2']
		self.df['c3_pct'] = (self.df['C3'] - self.df['fc3'])/self.df['C3']
		self.df['c4_pct'] = (self.df['C4'] - self.df['fc4'])/self.df['C4']
		self.df['c5_pct'] = (self.df['C5'] - self.df['fc5'])/self.df['C5']

		## rearranging the columns
		cols = list(self.df.columns.values)
		cols.pop()
		cols.pop()
		cols.pop()
		cols.pop()
		cols.insert(7, 'c2_pct')
		cols.insert(8, 'c3_pct')
		cols.insert(9, 'c4_pct')
		cols.insert(10, 'c5_pct')

		self.df = self.df[cols]


	def getRank(self):
		## get rank
		logging.info("calculating rank ...")
		cnt = 0
		dfLength = len(self.df.index)
		self.rank = range(1, dfLength + 1)
		self.df['C6'] = self.rank
		cols = list(self.df.columns.values)
		cols.pop()
		cols.insert(6, 'C6')
		#self.df = self.df[['Jan_Member_name', 'C1', 'C2', 'C3', 'C4', 'C5', 'C6', 'Feb_Member_name', 'c1', 'c2', 'c3', 'c4', 'c5']]
		self.df = self.df[cols]


	def create_excel(self):
		logging.info("creating excel file ...")
		excelFile = os.path.join(os.getcwd(), 'sampleexcel.xlsx')

		try:
			writer = pd.ExcelWriter(excelFile, engine='xlsxwriter')
			self.df.to_excel(writer, sheet_name='Sheet1', index=False)
			writer.save()
		except Exception as e:
			logging.error("create_excel(): unable to write excel file, e: {}".format(e))
		else:
			logging.info("excel file: {} has created ...".format(excelFile))


	def getConnection(self):
		con = pymssql.connect(self.hostname, self.username, self.password, self.dbname, self.port)
		#cursor = con.cursor()
		return con


	def readConfig(self):
		configParser = ConfigParser.RawConfigParser()		
		configParser.read(self.configFilePath)

		self.hostname = configParser.get('localDatabase', 'hostname')
		self.username = configParser.get('localDatabase', 'username')
		self.password = configParser.get('localDatabase', 'password')
		self.dbname   = configParser.get('localDatabase', 'dbname')
		self.port     = configParser.get('localDatabase', 'port')

		#print self.hostname, self.username, self.password, self.dbname, self.port


	def main(self):
		logging.basicConfig(filename=os.path.join(os.getcwd(), 'generateExcel.log'),
							format='%(asctime)s: %(levelname)s: %(message)s',
							level=logging.DEBUG)

		logging.info("********************************************************")
		logging.info("starting process ...")
		logging.info("********************************************************")
		stime = time.time()

		## read configuration file
		self.readConfig()

		## read data from database
		self.getData()

		## get rank in C6
		self.getRank()

		## calculate pct
		self.getPercentage()

		## create excel file		
		self.create_excel()

		etime = time.time()
		ttime = etime - stime		
		logging.info("********************************************************")
		logging.info("completing process ...")
		logging.info("time consumed to complete the process: {}".format(ttime))
		logging.info("********************************************************")


if __name__ == '__main__':
	ada = GenerateExcel()
	ada.main()
