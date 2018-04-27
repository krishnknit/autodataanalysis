#!/usr/bin/python
import os
import time
import pymssql
import logging
import argparse
import subprocess
import numpy as np
import pandas as pd
import datetime
#from datetime import datetime, timedelta
from sqlalchemy import create_engine


class AutomateDataAnalysis(object):
	def __init__(self):
		## excel file path
		self.excelPath = os.getcwd()
		self.date_format = "%Y-%m-%d"
		self.days10yrs = 365 * 10
		self.rowList   = []
		self.existinprod = []
		self.notexistinprod = []
		self.datestatus = []
		self.newdates = []
		self.topRank = []
		self.bottomRank = []
		
		## credentials
		self.hostname = 'localhost'
		self.username = 'SA'
		self.password = 'cybage@123'
		self.dbname   = 'TestDB'
		self.port     = '1433'


	def readExcelToMakeEntryInDB(self):
		try:			
			#excelDF = pd.read_excel(os.path.join(path, file), sheetname = "Sheet1")
			readExlsFileFrmt = 'CDX_excel_'+self.formatDate()+'.xlsx'
			if os.path.exists(os.path.join(self.excelPath, readExlsFileFrmt)):
				logging.info("reading from excel file '{}' started...".format(readExlsFileFrmt))
				excelDF = pd.read_excel(os.path.join(self.excelPath, readExlsFileFrmt), sheetname = "Sheet1")
			else:
				logging.error("excel file '{}' not exists ...".format(readExlsFileFrmt))
			
			## make database entry by using pandas
			#self.makeDatabseEntryByPandas(excelDF)

			logging.info("extracting fields from excel file '{}'...".format(readExlsFileFrmt))
			for index, row in excelDF.iterrows():
				dt = str(row['dt'])[:10]
				indx_nm = str(row['indx_nm'])
				indx_val = row['indx_val']
				create_ts = str(row['create_ts'])[:10]
				create_user_id = str(row['create_user_id'])

				self.rowList.append((dt, indx_nm, indx_val, create_ts, create_user_id))
			logging.info("reading from excel file '{}' completed...".format(readExlsFileFrmt))
		except Exception as e:
			logging.error("readExcelToMakeEntryInDB(), e: {}".format(e))


	def generatePcIndxFile(self):
		rwrapper = os.path.join(self.excelPath, 'rscriptWrapper.py')

		try:
			subprocess.check_call(['python', rwrapper], shell = False)
		except Exception as e:
			logging.error("generatePcIndxFile(): unable to run R script, e: {}".format(e))
		

	def generateCompareFile(self):
		## create empty dataframe
		self.df = pd.DataFrame()

		## read from database
		self.getFirstColFrmDB()

		## read from pcfile
		self.getSecondColFrmPcFile()


	def getSecondColFrmPcFile(self):
		genPcFile = os.path.join(self.excelPath, 'april_pcfile.xlsx')
		self.genPcDf = pd.read_excel(genPcFile, sheetname = "Sheet1")
		
		self.genPcDf.reset_index(inplace = True)
		self.genPcDf.rename(columns = {"index": "date"}, inplace = True)

		self.df['scendatesgenera'] = self.genPcDf['date']


	def getFirstColFrmDB(self):
		## get database connection  
		con, cur = self.getConnection()

		## Excute Query here
		sql = "SELECT dt FROM CDX"
		
		try:
			self.df['dateinprod'] = pd.read_sql(sql, con)
		except Exception as e:
			logging.error("getFirstColFrmDB(), unable to read data, e: {}".format(e))
		finally:
			cur.close()
			del cur
			con.close()


	def populateCDXDelta(self):
		## get dates
		self.getDates()

		## format dates
		plastDate = self.lastDate.strftime(self.date_format)
		pfirstDate = self.firstDate.strftime(self.date_format)
		pback10lastDate = self.back10lastDate.strftime(self.date_format)
		pback10firstDate = self.back10firstDate.strftime(self.date_format)

		logging.info("inserting data to table CDX_delta from '{}' to '{}' ...".format(pfirstDate, plastDate))
		try:
			con, cur = self.getConnection()
			cur.execute("EXEC dbo.p_populate_CDX_delta @dt_from = '{}', @dt_to = '{}'".format(pfirstDate, plastDate))
		except Exception as e:
			logging.error("populateCDXDelta(): unable to populate CDX_delta table, e: {}".format(e))
		

	def getDates(self):
		today = datetime.date.today()
		#today = datetime.now()
		fstDateOfMonth = today.replace(day=1)
		self.lastDate = fstDateOfMonth - datetime.timedelta(days=1)
		self.firstDate = self.lastDate.replace(day=1)
		self.back10lastDate = self.lastDate.replace(self.lastDate.year - 10)
		self.back10firstDate = self.firstDate.replace(self.firstDate.year - 10)
		
		# print "firstDate: ", self.firstDate
		# print "lastDate: ", self.lastDate
		# print "back10firstDate: ", self.back10firstDate
		# print "back10lastDate: ", self.back10lastDate


	def formatDate(self):
		return self.firstDate.strftime("%b%Y")


	def compareExcelDates(self):
		compfile = 'compare_file_'+ self.formatDate() +'_scen_gen.xlsx'
		compExcelFile = os.path.join(os.getcwd(), compfile)
		logging.info("comparision of dates from excel file {} started ...".format(compfile))
		
		try:
			for index, row in self.df.iterrows():
				## dateinprod scendatesgenera
				if any(self.df.scendatesgenera == row['dateinprod']):
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

				if any(self.df.dateinprod == row['scendatesgenera']):
					self.notexistinprod.append('Exists')
					self.newdates.append('NA')
					self.topRank.append('NA')
					self.bottomRank.append('NA')
				else:
					self.notexistinprod.append('Not Exists')
					self.newdates.append('Newdate')

					## get rank for new date
					rfrmTop, rfrmBottom = self.getRank(row['dateinprod'])
					self.topRank.append(rfrmTop)
					self.bottomRank.append(rfrmBottom)
		except Exception as e:
			logging.error("compareExcelDates(), e: {}".format(e))

		## format dateinprod & scendatesgenera
		self.df['dateinprod'] = self.df['dateinprod'].dt.strftime(self.date_format)
		self.df['scendatesgenera'] = self.df['scendatesgenera'].dt.strftime(self.date_format)

		## update df with new fields
		self.df['existinprod'] = self.existinprod
		self.df['notexistinprod'] = self.notexistinprod
		self.df['datestatus'] = self.datestatus
		self.df['newdates'] = self.newdates
		self.df['Rank from Top'] = self.topRank
		self.df['Rank from Bottom'] = self.bottomRank

		## write to excel file
		try:
			writer = pd.ExcelWriter(compExcelFile, engine='xlsxwriter')
			self.df.to_excel(writer, sheet_name='Sheet1', index=False)
			writer.save()
		except Exception as e:
			logging.error("compareExcelDates(): unable to write excel file, e: {}".format(e))
		else:
			logging.info("dates comparision from excel file '{}'' has completed ...".format(compfile))


	def readIndexFile(self):
		genIndxFile = os.path.join(self.excelPath, 'april_indx_file.xlsx')
		genIndxDf = pd.read_excel(genIndxFile, sheetname = "Sheet1", parse_dates=False, convert_float=False)
		
		genIndxDf.reset_index(inplace = True)
		genIndxDf.rename(columns = {"index": "date"}, inplace = True)

		return genIndxDf


	def getRank(self, newDate):
		## read index file
		genIndxDf = self.readIndexFile()
		newDate = str(newDate)[:10] 
		try:
			driSeries = genIndxDf.loc[genIndxDf['date'] == str(newDate), 'driver']
			driver = driSeries.values[0]
			self.genPcDf.sort_values(by=str(driver).lower(), inplace = True)
		except Exception as e:
			logging.error("getRank(), no date match e: {}".format(e))

		dfLength = len(self.genPcDf.index)
		rowNum = 0

		for index, row in self.genPcDf.iterrows():
			try:
				rowNum += 1
				if str(row['date'])[:10] == newDate:
					rowNumTop = rowNum
					rowNumBottom = "-{}".format(dfLength - rowNum + 1) 
			
					return rowNumTop, rowNumBottom
			except Exception as e:
				logging.error("getRank(), no rank found e: {}".format(e))
		

	def makeDatabseEntryByPandas(self, excelDF):
		try:
			mydb = create_engine('mssql+pymssql://' + self.username + ':' + self.password + '@' + self.hostname + ':' + str(self.port) + '/' + self.dbname , echo=False)
			excelDF.to_sql(name="CDX", con=mydb, if_exists = 'append', index=False)
		except Exception as e:
			logging.error("makeDatabseEntryByPandas(), e: {}".format(e))
		else:
			mydb.close()


	def makeDatabseEntry(self):
		## get database connection, cursor
		logging.info("getting database connection ...")
		con, cur = self.getConnection()

		try:
			logging.info("inserting records to database ...")
			cur.executemany("INSERT INTO CDX VALUES (%s, %s, %d, %s, %s)", self.rowList)
			cur.execute("COMMIT")
		except Exception as e:
			logging.error("makeDatabseEntry(), e: {}".format(e))
		else:
			## close database connection
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
		logging.basicConfig(filename=os.path.join(os.getcwd(), 'automateDataAnalysis.log'),
							format='%(asctime)s: %(levelname)s: %(message)s',
							level=logging.DEBUG)

		logging.info("********************************************************")
		logging.info("starting automate process ...")
		logging.info("********************************************************")
		stime = time.time()
		self.readExcelToMakeEntryInDB()
		self.makeDatabseEntry()
		self.generatePcIndxFile()
		self.populateCDXDelta()		
		self.generateCompareFile()
		self.compareExcelDates()
		etime = time.time()
		ttime = etime - stime		
		logging.info("********************************************************")
		logging.info("time consumed to complete the process: {}".format(ttime))
		logging.info("completing automate process ...")
		logging.info("********************************************************")


if __name__ == '__main__':
	ada = AutomateDataAnalysis()
	ada.main()
	