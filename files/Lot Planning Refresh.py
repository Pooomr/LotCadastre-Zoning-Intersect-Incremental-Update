''' Update Lot_Zone table Proof of Concept
	v1 - First version - Select records based on last_update_date filter working
	v1a - Unable to make tabulation work with lot layer (no OID)
	v2 - New version, processes Zones in 30 day chunks
'''

import logging
import sys
#logging.basicConfig(level=logging.DEBUG)
username = sys.argv[1]
# logging.basicConfig(filename="log.txt",
					# level=logging.DEBUG,
					# format="%(asctime)s - {} - %(message)s".format(username),
					# datefmt='%d/%m/%Y %H:%M:%S')

#Logging settings
logger = logging.getLogger("LotPlanningLog")
logger.setLevel(logging.DEBUG)					
file_handler = logging.FileHandler('log.txt')
formatter = logging.Formatter("%(asctime)s - {} - %(message)s".format(username),'%d/%m/%Y %H:%M:%S')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.debug("Importing Python Packages...")
logger.info("[START] Lot_Zone Update process started")

try:
	import arcpy
except:
	print("Error Importing arcpy module, make sure OpenVPN is connected and licences are available!")
	logger.info("[STOPPED] Unable to import arcpy module, Lot Planning update Stopped")
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

logger.debug("Python packages imported successfully")

def loadingBar(p: int, msg: str) -> str:
	
	progress = ""
	togo = "          "
	
	togo = togo[:-p] #reduce empty space based on progress
	
	for i in range(p):
		progress += "■"
		
	
	print("[{}{}] {}                            ".format(progress, togo, msg), end="\r")

def getNextId(column: str, table: str) -> int:
	c.execute("select max({}) from {}".format(column, table))
	result = c.fetchone()
	
	#If records exist, increment next id, else start at 1
	if result[0] != None:
		nextId = result[0] + 1
	else:
		nextId = 1
	
	return nextId

def createSession(username,password):
	#Creates Session Connection pool to GPR Database

	oc_attempts = 0

	while oc_attempts < 2:
		if oc_attempts == 0:
			print("Trying DPE IP: {}".format(config.dsnDPE))
			dsn = config.dsnDPE
		else:
			dsn = config.dsnDCS
			print("Trying DCS IP: {}".format(config.dsnDCS))
			
		try:
			pool = cx_Oracle.SessionPool(
				username,
				password,
				dsn,
				min=1,
				max=3,
				increment=1,
				encoding=config.encoding)

			# show the version of the Oracle Database
			print("Connection Successful!")
			oc_attempts = 2
		except cx_Oracle.Error as error:
			logger.info("[ERROR] {}".format(error))
			print(error)
			oc_attempts += 1
			
	return pool
	
def getRESTData(baseURL, params, serviceName):
	
	retries = 0
	success = False
	r_code = 0
	response = None
	
	while not success:
		try:
			#response = requests.get(url=baseURL, params=params, verify=False)
			response = requests.get(url=baseURL, params=params)
			success = True
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
				logger.info("Lot Zoning update aborted by User")
				sys.exit()
			else:
				print("Invalid selection. Please enter y or n")
	
	return json.loads(response.text)
	
def createLotLayer(zoneId,baseURL):
	#Creates Lot feature layer via JSON results
	df_lots = pd.read_sql("select distinct lotref from LZ_LOT_SPATIAL where lz_update_log_id = {}".format(zoneId),connection)
	
	#Temporary JSON file
	tempJSON = "{}\\arcGIS\\Temp.json".format(os.getcwd())
	
	#JSON Head for Lots
	JSONHead = '{"displayFieldName": "planlabel","fieldAliases": {"lotidstring":"lotidstring" },"geometryType": "esriGeometryPolygon","spatialReference": {"wkid": 4326,"latestWkid": 4326},"fields": [{"name":"lotidstring","type":"esriFieldTypeString","alias":"lotidstring","length":50} ],"features": ['
	
	#initialise string to pass through Lot Cadastre query
	lotstring = ''
	
	#Initialise List to store all Json results
	lotResults = list()
	
	for i, row in df_lots.iterrows():
		if lotstring == '':
			lotstring += "'{}'".format(row["LOTREF"])
		else:
			lotstring += ",'{}'".format(row["LOTREF"])
		
		#Every 200 records query service
		if (i + 1) % 200 == 0 or (i + 1) == len(df_lots):
			
			params = {
				'f':'json',
				'returnGeometry':'true',
				'outSR':'4326',
				'OutFields':'lotidstring',
				'where':'lotidstring in ({})'.format(lotstring)
			}
						
			jsonResult = getRESTData(baseURL, params, "Lot Service")
					
			if jsonResult.get('features'):
				#iterate through all features in JSON response and add to Result list
				for jr in range(len(jsonResult['features'])):
					lotResults.append(jsonResult['features'][jr])
				
			lotstring = ''
			
	#If Scratch folder doesn't exist, create it
	if not os.path.exists("{}\\arcGIS\\scratch.gdb".format(os.getcwd())):
		arcpy.management.CreateFileGDB("{}\\arcGIS".format(os.getcwd()), "scratch.gdb")
		logger.debug("Scratch folder created...")
	
	writeToJSON(JSONHead, tempJSON, lotResults, 'lots_to_update')

def writeToJSON(JSONHead, tempJSON, JSONResults, layerName):
	
	JSONinput = ""
	JSONinput += "{}".format(JSONHead)
	totalSLots = len(JSONResults)
	fileNum = 1
	
	logger.debug("WRITING TO JSON... {}".format(totalSLots))
	for i, row in enumerate(JSONResults):
		
		#Add lot records to JSON
		if (i + 1) % 3000 == 1:
			JSONinput += '{}'.format(JSONResults[i]) #If first record do not add comma
		else:
			JSONinput += ',{}'.format(JSONResults[i])
		
		#If max range met, close file and open new one
		if (i + 1) % 3000 == 0 or (i + 1) == totalSLots:
			JSONinput += ']}'
			logger.debug("Writing to JSON file at {}".format(tempJSON))
			#Clear Temp JSON file and insert results
			with open(tempJSON,'w') as jsonDir:
				jsonDir.write(JSONinput.replace("None","null")) #Replace instances of 'None' with null
			
			logger.debug("Writing to scratch arcGIS project folder...")
			#Load to arcGIS folder
			arcpy.conversion.JSONToFeatures(tempJSON,"{}\\arcGIS\\scratch.gdb\\{}_{}".format(os.getcwd(),layerName, fileNum),"POLYGON")
			
			fileNum += 1
			JSONinput = "{}".format(JSONHead) #Reset
			
	#Merge all scratch files
	LayerList = ''
	logger.debug("Merge {} files".format(fileNum - 1))
	for layer in range(1, fileNum):
		logger.debug("processing {} of {}".format(layer, fileNum - 1))
		if layer == 1:
			LayerList += "{}\\arcGIS\\scratch.gdb\\{}_{}".format(os.getcwd(),layerName,layer)
		else:
			LayerList += ";{}\\arcGIS\\scratch.gdb\\{}_{}".format(os.getcwd(),layerName,layer)
	logger.debug("Run: Merge({},{})".format(LayerList,"{}\\arcGIS\\lot_zone_update.gdb\\{}".format(os.getcwd(),layerName)))	
	arcpy.management.Merge(LayerList, "{}\\arcGIS\\lot_zone_update.gdb\\{}".format(os.getcwd(),layerName))

def intersectLotZone(lzId,layerName):
	#Tabulate Intersect Lot Layer with current Zone layer
	arcpy.analysis.TabulateIntersection("{}\\lots_to_update".format(arcFolder), "lotidstring", ZoningLayer, "{}\\{}".format(arcFolder,layerName), "EPI_NAME;EPI_TYPE;SYM_CODE;LAY_CLASS", None, "10 Centimeters", "SQUARE_METERS")
	logger.debug("{}\\lots_to_update".format(arcFolder))
	logger.debug(ZoningLayer)
	logger.debug("{}\\{}".format(arcFolder,layerName))
	
	c.execute("update LZ_UPDATE_LOG set finish_date = CURRENT_TIMESTAMP where lz_update_log_id = {}".format(lzId))
	c.execute("update LZ_LOT_SPATIAL set processed = CURRENT_TIMESTAMP where lz_update_log_id = {}".format(lzId))
	c.execute("commit")
	
	logger.info("[PROCESS] Tabulate Intersection complete for lz_update_log_id: {}".format(lzId))
	
if __name__ == "__main__":
	
	# Connect to DB and create session pool
	try:
		pool = createSession(config.username, config.password)
	except RuntimeError as e:
		logger.error(str(e))
		print(str(e))
		sys.exit(1)

	# Acquire connection from the pool
	connection = pool.acquire()
	c = connection.cursor()
	
	#Connection files
	ZoningLayer = "{}\\arcGIS\\PlanningSDE.sde\\PlanningDB.SDE.EPI\\PlanningDB.SDE.EPI_Land_Zoning".format(os.getcwd())
	LotLayer = "{}\\arcGIS\\DCDB_SDE.sde\\DCDB_DELIVERY.sde.LotAll".format(os.getcwd())
	LotUrl = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Cadastre/MapServer/0/query"
	
	#ArcPy Settings
	logger.debug("[DEBUG] Setting up ArcGIS connection to Planning SDE")
	env.overwriteOutput = True
	arcFolder = "{}\\arcGIS\\lot_zone_update.gdb".format(os.getcwd())
	LZ_to_update = "{}\\LandZoning_to_update".format(arcFolder)
	logger.debug("[DEBUG] Connected to Planning SDE")
	
	#Check if there are unprocessed lots
	df_lz_to_process = pd.read_sql("select distinct lz_update_log_id from LZ_LOT_SPATIAL order by lz_update_log_id",connection)
	if len(df_lz_to_process) > 0:
		for i, to_proc in df_lz_to_process.iterrows():
			logger.info("[PROCESS] Continued processing lots for lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
			createLotLayer(to_proc["LZ_UPDATE_LOG_ID"],LotUrl)
			
			intersectLotZone(to_proc["LZ_UPDATE_LOG_ID"],"Lot_Zone_to_update")
		
	#Get last update date of Lot_Zone
	c.execute("select max(end_date) from LZ_UPDATE_LOG where finish_date is not null")
	last_update_tuple = c.fetchone()

	if last_update_tuple[0]:
		last_update = last_update_tuple[0] #Found the last updated Lot Zone phase
		logger.info("[PROCESS] Last Lot Zone log update found: {}".format(last_update))
	else:
		c.execute("select max(update_date) from lot_zone") #No Lot Zone Logs found, use last updated Lot_zone record instead
		last_update_tuple = c.fetchone()
		
		if last_update_tuple[0]:
			last_update = last_update_tuple[0]
			logger.info("[PROCESS] Last Lot Zone log update not found, using last lot_zone update instead: {}".format(last_update))
		else:
			logger.info("[ERROR] Unable to obtain 'last_update' from lot_zone, please check data source")
			print("Unable to retrieve 'last_update' from lot_zone table")
			sys.exit()
	print("LAST UPDATE IS {}".format(last_update))

	c.close()
	pool.release(connection)
	
	#Set current date
	current_date = datetime.today()
	end_period = last_update + timedelta(days=30) #Get Zoning updates in 30 day chunks
	
	print("Test Selection...")
	logger.info("[INFO] Selecting records")
	
	#SET LIMITS
	zoneShp = 5 #Total number of zones to extract lots each round
	lotLimit = 200 #Total number of lots to query each round
	
	print("{}".format(last_update.strftime('%Y-%m-%d %H:%M:%S')))
	
	#Iterate through all Updated Zone layers until done
	while last_update < current_date:
	
		#KEEP ZONING LAYER FOR TESTING
		date_range_expression = "LAST_EDITED_DATE >= '{}' AND LAST_EDITED_DATE < '{}'".format(last_update.strftime('%Y-%m-%d %H:%M:%S'),end_period.strftime('%Y-%m-%d %H:%M:%S'))
		
		#Copy updated records to new layer 'LandZoning_to_update'
		arcpy.Select_analysis(ZoningLayer, "{}\\LandZoning_to_update".format(arcFolder), where_clause=date_range_expression)
		
		totalRecords = arcpy.management.GetCount(LZ_to_update) #Total Zone records to iterate
		
		#Insert lz_update_log record and get ID
		connection = pool.acquire() #Acquire connection from pool
		c = connection.cursor()
		
		c.execute("insert into LZ_UPDATE_LOG values (SEQ_LZ_UPDATE_LOG.nextval, TO_DATE('{}', 'yyyy/mm/dd hh24:mi:ss'), TO_DATE('{}', 'yyyy/mm/dd hh24:mi:ss'), CURRENT_TIMESTAMP, null, {}, '{}')".format(last_update.strftime('%Y/%m/%d %H:%M:%S'),end_period.strftime('%Y/%m/%d %H:%M:%S'),totalRecords,username))
		c.execute("commit")
		c.execute("SELECT SEQ_LZ_UPDATE_LOG.currval FROM dual")
		lz_update_log_id = c.fetchone()[0]
		
		print("Last inserted ID:", lz_update_log_id)
		
		#GET LOTS FOR EACH ZONE SHAPE		
		count = 0 #Keep track of record count
		lcount = 0 #Keep track of lot count
		geoInput = '' #Initialise string for coordinates
		oIDInput = '' #Initialise string for Lot Object Ids
		lots = list() #Store lots that intersect with zone layers
		
		logger.info("[PROCESS] Processing Zones for {} -> {}".format(last_update, end_period))
		print("[PROCESS] Processing Zones for {} -> {}".format(last_update, end_period))	
		
		#Go through each record in LandZoning_to_update and find intersected lots
		with arcpy.da.SearchCursor(LZ_to_update,['OID@','SHAPE@']) as cursor:
			
			for row in cursor:
				sRef = row[1].extent.spatialReference.factoryCode
				if geoInput == '':
					geoInput = '[[{},{}],[{},{}],[{},{}],[{},{}],[{},{}]]'.format(row[1].extent.XMin,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMin)
				else:
					geoInput += ',[[{},{}],[{},{}],[{},{}],[{},{}],[{},{}]]'.format(row[1].extent.XMin,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMin)
				
				count += 1
				
				if count % zoneShp == 0 or count == totalRecords:
					params = {
						'f':'json',
						'outFields':'objectid',
						'returnGeometry':'false',
						'inSR':sRef,
						'returnIdsOnly':'true',
						'geometry':'{{"rings":[{}]}}'.format(geoInput),
						'geometryType':'esriGeometryPolygon',
						'spatialRel': 'esriSpatialRelIntersects'
					}
					
					jsonResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
					
					#Delay calls to rest service
					time.sleep(2)
					
					#Iterate through ObjectIDs and extract lot information
					if jsonResult.get('objectIds'):
						for oID in jsonResult['objectIds']:
							
							if oIDInput == '':
								oIDInput = '{}'.format(oID)
							else:
								oIDInput += ",{}".format(oID)
							
							lcount += 1
							
							if lcount % lotLimit == 0 or lcount == len(jsonResult['objectIds']):
								params = {
									'f':'json',
									'outFields':'lotidstring',
									'returnGeometry':'false',
									'returnDistinctValues':'true',
									'where':'objectid in ({})'.format(oIDInput)
								}
								jsonLotResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
								
								if jsonLotResult.get('features'):
									for lotref in jsonLotResult["features"]:
										#build up list of lots to insert
										lots.append(lotref["attributes"]["lotidstring"])
								else:
									print("ERROR: {}".format(jsonLotResult))
								
								oIDInput = '' #Reset
					else:
						print("ERROR: {}".format(jsonResult))
					
					#All Lots extracted, insert into table
					query = "insert all "
					c.execute("select max(lz_lot_spatial_id) from LZ_LOT_SPATIAL")
					
					#Set next LZ_LOT_SPATIAL_ID
					nextLsId = getNextId("LZ_LOT_SPATIAL_ID","LZ_LOT_SPATIAL")
						
					for i, lotref in enumerate(lots):
						query = "{} into LZ_LOT_SPATIAL values ({}, {}, '{}', CURRENT_TIMESTAMP, null)".format(query,nextLsId,lz_update_log_id,lotref)
						nextLsId += 1
						
						if (i + 1) % 1000 == 0 or (i + 1) == len(lots):
							query = "{} select 1 from dual".format(query)

							try:
								c.execute(query)
							except cx_Oracle.Error as error:
								logger.info("[ERROR] {}".format(error))
								print(error)
							
							query = "insert all "
					
					geoInput = ''
		c.execute("commit")
		
		#Create Lot Spatial Layer
		logger.info("[PROCESS] Processing lots for lz_update_log_id: {}".format(lzId))
		createLotLayer(lz_update_log_id,LotUrl)
		
		#Tabulate Intersect Lot layer with current Zone layer
		intersectLotZone(lz_update_log_id,"Lot_Zone_to_update")
		
		#Process Intersected Results
		
		#Finished Zoning chunk, set up for next 30 days
		logger.info("[PROCESS] {} Lots Intersected for {} Zones".format(lcount,count))
		print("{} Lots identified for {} Zones".format(lcount,count))
		#c.execute("update LZ_UPDATE_LOG set finish_date = CURRENT_TIMESTAMP where lz_update_log_id = {}".format(lz_update_log_id)) #Update Lot Zone Log to indicate zone is complete
		#c.execute("commit")
		c.close()
		pool.release(connection)
		last_update = end_period
		end_period = end_period + timedelta(days=30) #set up next chucnk
		
		
		print("Last_update: {}".format(last_update))
	
	logger.info("[FINISH] Lot_Zone Update process finished")
	print("Done!")