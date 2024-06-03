''' Update Lot_Zone table Proof of Concept
	v1 - First version - Select records based on last_update_date filter working
	v1a - Unable to make tabulation work with lot layer (no OID)
'''

import logging
import sys
#logging.basicConfig(level=logging.DEBUG)
username = sys.argv[1]
logging.basicConfig(filename="log.txt",
					level=logging.DEBUG,
					format="%(asctime)s - {} - %(message)s".format(username),
					datefmt='%d/%m/%Y %H:%M:%S')
logging.debug("Importing Python Packages...")
logging.info("[START] Lot_Zone Update process started")

try:
	import arcpy
except:
	print("Error Importing arcpy module, make sure OpenVPN is connected and licences are available!")
	logging.info("[STOPPED] Unable to import arcpy module, Lot Planning update Stopped")
	sys.exit()

import os
from arcpy import env
import pandas as pd
from datetime import datetime, timedelta
import config
import cx_Oracle
import requests
import json

current_update = datetime.now()

logging.debug("Python packages imported successfully")

def loadingBar(p: int, msg: str) -> str:
	
	progress = ""
	togo = "          "
	
	togo = togo[:-p] #reduce empty space based on progress
	
	for i in range(p):
		progress += "■"
		
	
	print("[{}{}] {}                            ".format(progress, togo, msg), end="\r")
	
def connectDB(username,password):
	#Connects to GPR Database
	connection = None

	oc_attempts = 0

	while oc_attempts < 2:
		if oc_attempts == 0:
			print("Trying DPE IP: {}".format(config.dsnDPE))
			dsn = config.dsnDPE
		else:
			dsn = config.dsnDCS
			print("Trying DCS IP: {}".format(config.dsnDCS))
			
		try:
			connection = cx_Oracle.connect(
				username,
				password,
				dsn,
				encoding=config.encoding)

			# show the version of the Oracle Database
			print(connection.version," Connection Successful!")
			oc_attempts = 2
		except cx_Oracle.Error as error:
			logging.info("[ERROR] {}".format(error))
			print(error)
			oc_attempts += 1
			
	return connection

def getRESTData(baseURL, params, serviceName):
	
	retries = 0
	success = False
	r_code = 0
	#cafile = 'C:\TMP\Python\swg.dec.int.cer'
	while not success:
		try:
			#response = requests.get(url=baseURL, params=params, verify=False)
			response = requests.get(url=baseURL, params=params)
			success = True
			print("URL = {}, PARAMS = {}".format(baseURL, params))
		except requests.exceptions.RequestException as e:
			print(e)
			retries += 1
			if retries > 9:
				while True:
					select = input("\nRequest to {} service failed 10 times, Do you want to try again? y/n\n".format(serviceName))
					if select == "y":
						retries = 0
						break
					elif select == "n":
						print("Lot Zoning update process Aborted!!")
						sys.exit()
					else:
						print("Invalid selection. Please enter y or n")
		
		if response:
			r_code = response.status_code
		else:
			r_code = 0
		
		while r_code != 200 and success:
			print("Response code: {}".format(response.status_code))
			select2 = input("\nInvalid response received, run query again? y/n\n")
			if select2 == "y":
				retries = 0
				success = False
				break
			elif select2 == "n":
				print("Lot Zoning update process Aborted!!")
				logging.info("Lot Zoning update aborted by User")
				sys.exit()
			else:
				print("Invalid selection. Please enter y or n")
	
	print(response.text)
	
	return json.loads(response.text)
	
if __name__ == "__main__":
	
	#Connect to DB
	connection = connectDB(config.username,config.password)
	c = connection.cursor()
	
	#Get last update date of Lot_Zone
	c.execute("select max(update_date) from lot_zone")
	last_update_tuple = c.fetchone()
	connection.close()
	
	if last_update_tuple and last_update_tuple[0]:
		last_update = last_update_tuple[0]
	else:
		logging.info("[ERROR] Unable to obtain 'last_update' from lot_zone, please check data source")
		print("Unable to retrieve 'last_update' from lot_zone table")
		sys.exit()
	
	#Connection files
	ZoningLayer = "{}\\arcGIS\\PlanningSDE.sde\\PlanningDB.SDE.EPI\\PlanningDB.SDE.EPI_Land_Zoning".format(os.getcwd())
	LotLayer = "{}\\arcGIS\\DCDB_SDE.sde\\DCDB_DELIVERY.sde.LotAll".format(os.getcwd())
	LotUrl = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Cadastre/MapServer/0/query"
	
	#ArcPy Settings
	logging.debug("[DEBUG] Setting up ArcGIS connection to Planning SDE")
	env.overwriteOutput = True
	arcFolder = "{}\\arcGIS\\lot_zone_update.gdb".format(os.getcwd())
	LZ_to_update = "{}\\LandZoning_to_update".format(arcFolder)
	logging.debug("[DEBUG] Connected to Planning SDE")
	
	print("Test Selection...")
	logging.info("[INFO] Selecting records")
	
	print("{}".format(last_update.strftime('%Y-%m-%d %H:%M:%S')))
	
	#KEEP ZONING LAYER FOR TESTING
	# date_range_expression = "LAST_EDITED_DATE >= '{}'".format(last_update.strftime('%Y-%m-%d %H:%M:%S'))
	
	# #Copy updated records to new layer 'LandZoning_to_update'
	# arcpy.Select_analysis(ZoningLayer, "{}\\LandZoning_to_update".format(arcFolder), where_clause=date_range_expression)
	
	#GET LOTS FOR EACH ZONE SHAPE
	zoneShp = 5 #Total number of zones to extract lots each round
	count = 0 #Keep track of record count
	geoInput = '' #Initialise string for coordinates
	totalRecords = arcpy.management.GetCount(LZ_to_update) #Total Zone records to iterate
	lots = list() #Store lots that intersect with zone layers
	
	#Go through each record in LandZoning_to_update and find intersected lots
	with arcpy.da.SearchCursor(LZ_to_update,['OID@','SHAPE@']) as cursor:
		
		for row in cursor:
			sRef = row[1].extent.spatialReference.factoryCode
			if geoInput == '':
				geoInput = '[[{},{}],[{},{}],[{},{}],[{},{}],[{},{}]]'.format(row[1].extent.XMin,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMin)
			else:
				geoInput += ',[[{},{}],[{},{}],[{},{}],[{},{}],[{},{}]]'.format(row[1].extent.XMin,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMin)
			
			count += 1
			
			if count == 5 or count == totalRecords:
				print('{{"rings":[{}]}}'.format(geoInput))
				
				params = {
					'f':'json',
					'outFields':'lotidstring',
					'returnGeometry':'false',
					'inSR':sRef,
					'returnIdsOnly':'true',
					'geometry':'{{"rings":[{}]}}'.format(geoInput),
					'geometryType':'esriGeometryPolygon',
					'spatialRel': 'esriSpatialRelIntersects'
				}
				
				jsonResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
				
				print(jsonResult)
				
				#Delay calls to rest service
				time.sleep(2)
				
				#Iterate through ObjectIDs and extract lot information
				for oID in jsonResult['objectIds']:
					print(oID)
				
				geoInput = ''
				count = 0
			#geoInput = '{{"rings":[[[{},{}],[{},{}],[{},{}],[{},{}],[{},{}]]]}}'.format(row[1].extent.XMin,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMin)
	
			
			print("end of loop")
			#for vertice in range(row[1].pointCount):
				#pnt=array1.getObject(0).getObject(vertice)
				#print(row[0],pnt.X,pnt.Y)
	
	print("Last_update: {}".format(last_update))
	
	logging.info("[FINISH] Lot_Zone Update process finished")
	print("Done!")