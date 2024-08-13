''' Update Lot_Zone table Proof of Concept
	v1 - First version - Select records based on last_update_date filter working
	v1a - Unable to make tabulation work with lot layer (no OID)
	v2 - New version, processes Zones in 30 (or custom via day_chunk variable) day chunks
'''

import logging
import sys
import os
import config

username = sys.argv[1]

#Set local or shared directory and Environments
#h_dir = os.getcwd() #Directory where script is is
h_dir = "C:\\TMP\\Python\\Lot_Zone" #Set directory as Local drive
f_dir = os.path.dirname(os.getcwd())
env_mode = config.env_mode #Get Environment setting

#SET LIMITS
zoneShp = 1 #Total number of zones to extract lots each round (Need to be set to 1, overlapped bbox's will exclude lots)
day_backtrack = 1 #How many days back should 'get_updated_lots' function go, this is due to the SIX Maps REST Service for lot cadastre being updated daily, lots can be missing if lot_zone process is run before lots in date range is updated to service
lotLimit = 200 #Total number of lots to query each round
day_chunk = 10 #Set to process x days at a time
int_limit = 1000 #Limit on number of lots before performing intersect query

#Logging settings
logger = logging.getLogger("LotPlanningLog")
logger.setLevel(logging.DEBUG)
file_handler = logging.FileHandler('{}\\log.txt'.format(h_dir))
formatter = logging.Formatter("%(asctime)s - {} - %(message)s".format(username),'%d/%m/%Y %H:%M:%S')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

logger.debug("Importing Python Packages...")
logger.info("[START] Lot_Zone Update process started - [{}]".format(env_mode))

try:
	import arcpy
except:
	print("Error Importing arcpy module, make sure OpenVPN is connected and licences are available!")
	logger.info("[STOPPED] Unable to import arcpy module, Lot Planning update Stopped")
	sys.exit()

import shutil
from arcpy import env
import pandas as pd
from datetime import datetime, timedelta
import cx_Oracle
import requests
import json

logger.debug("Python packages imported successfully")

# Define the maximum number of retries and delay between retries
MAX_RETRIES = 5
RETRY_DELAY = 5  # in seconds

def loadingBar(p: int, msg: str) -> str:

	progress = ""
	togo = "          "
	
	if p > 0:
		togo = togo[:-p] #reduce empty space based on progress
	
	for i in range(p):
		progress += "■"

	print("[{}{}] {}".format(progress, togo, msg), end="\r")
	#sys.stdout.write("\r[{}{}] {}".format(progress, togo, msg))
	#sys.stdout.flush()

def getNextId(column: str, table: str) -> int:
	c.execute("select max({}) from {}".format(column, table))
	result = c.fetchone()

	#If records exist, increment next id, else start at 1
	if result[0] != None:
		nextId = result[0] + 1
	else:
		nextId = 1

	return nextId

def check_and_copy_directory(source_dir, target_dir, folder_name):
	"""
	Check if a folder exists in the target directory. If it does not, copy the folder from the source directory.

	:param source_dir: The source directory where the folder is located.
	:param target_dir: The target directory where the folder should be copied.
	:param folder_name: The name of the folder to check and copy.
	"""
	source_path = os.path.join(source_dir, folder_name)
	target_path = os.path.join(target_dir, folder_name)

	# Check if the target path exists
	if not os.path.exists(target_path):
		print(f"Folder '{folder_name}' not found in target directory. Copying from source directory.")
		logger.info(f"[PROCESS] Folder '{folder_name}' not found in target directory. Copying from source directory.")
		try:
			shutil.copytree(source_path, target_path)
			print(f"Folder '{folder_name}' successfully copied to '{target_dir}'.")
			logger.info(f"[PROCESS] Folder '{folder_name}' successfully copied to '{target_dir}'.")
		except Exception as e:
			print(f"An error occurred while copying the folder: {e}")
			logger.info(f"[ERROR] An error occurred while copying the folder: {e}")
	else:
		logger.info(f"[PROCESS] Folder '{folder_name}' already exists in target directory.")

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

def execute_with_retries(connection, query, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY):
    attempt = 0
    while attempt < max_retries:
        try:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            cursor.close()
            #logger.debug("Query executed successfully")
            return
        except cx_Oracle.OperationalError as e:
            logger.error("OperationalError occurred: %s", e)
            attempt += 1
            if attempt >= max_retries:
                logger.error("Max retries reached, operation failed")
                raise
            else:
                logger.info("Retrying in %s seconds...", retry_delay)
                time.sleep(retry_delay)
                # Reconnect to the database
                connection = pool.acquire()
	
def getRESTData(baseURL, params, serviceName):

	retries = 0
	success = False
	r_code = 0
	r_code2 = 0
	response = None
	
	while not success:
		try:
			#response = requests.get(url=baseURL, params=params, verify=False)
			response = requests.get(url=baseURL, params=params)
			success = True
		except requests.exceptions.RequestException as e:
			print(e)
			logger.info(e)
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

		if success:
			try:
				r_code = response.status_code
				# Check code in json result
				response_check = json.loads(response.text)
				if response_check.get("error"):
					r_code2 = response_check["error"]["code"]
					success = False
					retries += 1
				else:
					r_code2 = 200
				
			except ValueError:
				r_code2 = 200  # Default to 200 if response is not a valid JSON
		else:
			r_code = 0
			r_code2 = 0
			success = False
			retries += 1
		
		# logger.debug("r_code = {} and r_code2 = {}".format(r_code,r_code2))
		while r_code != 200 or r_code2 != 200 and retries > 9:
			print("Response code: {} - JSON Response code: {}".format(r_code,r_code2))
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
	# logger.debug("REST response for: {}".format(baseURL))
	# logger.debug("Params are {}".format(params))
	# logger.debug("Results are {}".format(response.text))
	# logger.debug("Response code is {}".format(r_code))
	return json.loads(response.text)

def get_lot_runs(zoneId,lot_runs):
	#GET LOTS IN CURRENT INTERSECT RUN

	#Build Lot run list
	lr_query = ""
	
	for lrow in lot_runs:
		if lr_query == "":
			lr_query = "{}".format(lrow)
		else:
			lr_query = "{},{}".format(lr_query,lrow)
	
	#logger.debug("lr query is {}".format(lr_query))
	
	df_lots = pd.read_sql("select lot_run, lotref from (select min(lot_run) lot_run, lotref from lz_lot_spatial where lz_update_log_id = {} and processed is null group by lotref) where lot_run in ({}) order by lot_run".format(zoneId,lr_query),connection)

	#logger.debug("Running get lot Runs: df_lots contains {}".format(df_lots))
	
	return df_lots
	
def createLotLayer(zoneId,baseURL,lot_runs,df_lots):
	#Creates Lot feature layer via JSON results
	
	#Temporary JSON file
	tempJSON = "{}\\arcGIS\\Temp.json".format(h_dir)

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

			#TO-DO ADD RETRY MECHANISM FOR EMPTY RESULTS
			jsonResult = getRESTData(baseURL, params, "Lot Service")

			if jsonResult.get('features'):
				#iterate through all features in JSON response and add to Result list
				for jr in range(len(jsonResult['features'])):
					lotResults.append(jsonResult['features'][jr])
			else:
				print("ERROR createLotLayer: {}".format(jsonResult))
				logger.info("ERROR in create Lot Layer: {}".format(jsonResult))

			lotstring = ''

	#If Scratch folder doesn't exist, create it
	if not os.path.exists("{}\\arcGIS\\scratch.gdb".format(h_dir)):
		arcpy.management.CreateFileGDB("{}\\arcGIS".format(h_dir), "scratch.gdb")
		logger.info("Scratch folder created...")

	writeToJSON(JSONHead, tempJSON, lotResults, 'lots_to_update')
	
	return df_lots

def writeToJSON(JSONHead, tempJSON, JSONResults, layerName):

	JSONinput = ""
	JSONinput += "{}".format(JSONHead)
	totalSLots = len(JSONResults)
	fileNum = 1

	#logger.debug("Running writeToJSON: JSONResults is {}".format(JSONResults))
	
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
			arcpy.conversion.JSONToFeatures(tempJSON,"{}\\arcGIS\\scratch.gdb\\{}_{}".format(h_dir,layerName, fileNum),"POLYGON")
			
			fileNum += 1
			JSONinput = "{}".format(JSONHead) #Reset
			
	#Merge all scratch files
	LayerList = ''
	logger.debug("Merge {} files".format(fileNum - 1))
	for layer in range(1, fileNum):
		logger.debug("processing {} of {}".format(layer, fileNum - 1))
		if layer == 1:
			LayerList += "{}\\arcGIS\\scratch.gdb\\{}_{}".format(h_dir,layerName,layer)
		else:
			LayerList += ";{}\\arcGIS\\scratch.gdb\\{}_{}".format(h_dir,layerName,layer)
	logger.info("Run: Merge({},{})".format(LayerList,"{}\\{}".format(arcFolder,layerName)))	
	arcpy.management.Merge(LayerList, "{}\\{}".format(arcFolder,layerName))

def extractLots(lzId, totalRec, loading_msg):
	#Go through Zone BBOXs and extract lots
	
	geoInput = '' #Initialise string for coordinates
	oIDInput = '' #Initialise string for Lot Object Ids
	lots = list() #Store lots that intersect with zone layers
	sql = list() #Store queries to commit for insert into lz_lot_spatial
	sql2 = list() #Store quries to commit for insert into lz_lot_run
			
	df_bbox = pd.read_sql("select lz_zone_bbox_id, lz_update_log_id, spatial_ref, bbox from LZ_ZONE_BBOX where lz_update_log_id = {} and processed is null order by lz_zone_bbox_id".format(lzId),connection)
	#print(df_bbox)
	count = 0
	lcount = 0
	
	#Get Next Run ID
	runId = getNextId("LZ_LOT_RUN_ID","LZ_LOT_RUN")
	#Get Next Run number
	runNo = getNextId("LOT_RUN","LZ_LOT_RUN")
	
	#Set up sql query for Inserts to LZ_LOT_RUN
	query2 = "insert all "
	
	for index, row in df_bbox.iterrows():
		sRef = row["SPATIAL_REF"]
		bboxId = row["LZ_ZONE_BBOX_ID"]
		if geoInput == '':
			geoInput = "{}".format(row["BBOX"])
		else:
			geoInput += ",{}".format(row["BBOX"])

		#Insert Run information to audit lot extractions
		query2 = "{} into LZ_LOT_RUN (LZ_LOT_RUN_ID, LOT_RUN, LZ_ZONE_BBOX_ID, RUN_DATE) values ({},{},{},CURRENT_TIMESTAMP)".format(query2,runId,runNo,bboxId)
		runId += 1

		count += 1
		logger.debug("geoInput is {}".format(geoInput))
		#logger.debug("count is {} ({}) - zoneShp is {} - TotalRecords is {} ({})".format(count,type(count),zoneShp,totalRec,type(totalRec)))
		# logger.debug("sRef is {}".format(sRef))
		#logger.debug("count % zoneShp = {}".format(count % zoneShp))
		# logger.debug("count == totalRecords is {}".format(count == totalRec))
		if count % zoneShp == 0 or count == totalRec:
			
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

			retries_1 = 0
			success_1 = False
			retries_2 = 0
			success_2 = False
			
			while not success_1:
				jsonResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
				#logger.debug("Getting objIDs in Zone BBOX: {}".format(jsonResult))
				#Delay calls to rest service
				time.sleep(2)
				
				#Iterate through ObjectIDs and extract lot information
				if jsonResult.get('objectIds'):
					success_1 = True
					logger.debug("Total len of ObjectIDs: {}".format(len(jsonResult['objectIds'])))
					for oID in jsonResult['objectIds']:
						
						if oIDInput == '':
							oIDInput = '{}'.format(oID)
						else:
							oIDInput += ",{}".format(oID)
						
						lcount += 1
						
						#logger.debug("lcount = {} - lotLimit = {} - total Objects = {}".format(lcount,lotLimit,len(jsonResult['objectIds'])))
						
						if lcount % lotLimit == 0 or lcount == len(jsonResult['objectIds']):

							retries_2 = 0
							success_2 = False
						
							params = {
								'f':'json',
								'outFields':'lotidstring',
								'returnGeometry':'false',
								'returnDistinctValues':'true',
								'where':'objectid in ({})'.format(oIDInput)
							}
							
							while not success_2:
								jsonLotResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
								#logger.debug("Extract LOTREFS for OIDs: {}".format(oIDInput))
								if jsonLotResult.get('features'):
									success_2 = True
									for lotref in jsonLotResult["features"]:
										#build up list of lots to insert
										#logger.debug("Appending to lots(): {}".format(lotref["attributes"]["lotidstring"]))
										lots.append(lotref["attributes"]["lotidstring"])
								else:
									retries_2 += 1
									print("ERROR: {}".format(jsonLotResult))
									logger.info("[ERROR] Results do not contain features, retrying.. {}".format(jsonLotResult))
									
									#Issue with REST Call, retry
									while retries_1 > 9 or retries_2 > 9:
										select = input("\nResults from Lot Service are incorrect and failed 10 times, Do you want to try again? y/n\n")
										if select == "y":
											retries_1 = 0
											retries_2 = 0
											break
										elif select == "n":
											print("Lot Zoning update process Aborted!!")
											logger.info("[EXIT] Lot Zone Update process aborted by user")
											sys.exit()
										else:
											print("Invalid selection. Please enter y or n")
								
								oIDInput = '' #Reset
				else:
					if jsonResult.get('objectIdFieldName'):
						#Result is valid, but there are no objectIds
						logger.info("[PROCESS] No lots intersect with Zone_bbox_id: {}".format(bboxId))
						success_1 = True
					else:
						retries_1 += 1
						print("ERROR: {}".format(jsonResult))
						logger.info("[ERROR] Results do not contain objectIds, retrying.. {}".format(jsonResult))
				
				#If REST calls were successful, insert into table
				if success_1 and success_2:
					#All Lots extracted, insert into table
					query = "insert all "

					#Set next LZ_LOT_SPATIAL_ID
					nextLsId = getNextId("LZ_LOT_SPATIAL_ID","LZ_LOT_SPATIAL")

					#Debug
					#logger.debug("Total lots in Lots() is {}".format(len(lots)))

					for i, lotref in enumerate(lots):
						query = "{} into LZ_LOT_SPATIAL (LZ_LOT_SPATIAL_ID, LZ_UPDATE_LOG_ID, LOT_RUN, LOTREF, CREATE_DATE) values ({}, {}, {}, '{}', CURRENT_TIMESTAMP)".format(query,nextLsId,lzId,runNo,lotref)
						
						nextLsId += 1
						#logger.debug("SQL {} : {}".format(i, " into LZ_LOT_SPATIAL (LZ_LOT_SPATIAL_ID, LZ_UPDATE_LOG_ID, LOT_RUN, LOTREF, CREATE_DATE) values ({}, {}, {}, '{}', CURRENT_TIMESTAMP)".format(nextLsId,lzId,runNo,lotref)))
						
						if (i + 1) % 1000 == 0 or (i + 1) == len(lots):
							query = "{} select 1 from dual".format(query)
							#Add to commit queue
							sql.append(query)
							#logger.debug("INSERTED {}".format(query))
							query = "insert all "
							
							#Only add LZ_LOT_RUN record at the end of each lot run
							if (i + 1) == len(lots):
								query2 = "{} select 1 from dual".format(query2)
								sql2.append(query2)
								query2 = "insert all "

					logger.debug('LOT EXTRACT FOR: {{"rings":[{}]}}'.format(geoInput))
					logger.debug("Total lots is: {}".format(lcount))

					runNo += 1
					geoInput = ''
					lcount = 0
					lots = list()
				else:
					#Issue with REST Call, retry
					while retries_1 > 9 or retries_2 > 9:
						select = input("\nResults from Lot Service are incorrect and failed 10 times, Do you want to try again? y/n\n")
						if select == "y":
							retries_1 = 0
							retries_2 = 0
							break
						elif select == "n":
							print("Lot Zoning update process Aborted!!")
							logger.info("[EXIT] Lot Zone Update process aborted by user")
							sys.exit()
						else:
							print("Invalid selection. Please enter y or n")

		#Commit all Lot queries if there are lot queries to commit
		if len(sql) > 0:
			for q in sql:
				try:
					c.execute(q)
				except cx_Oracle.Error as error:
					logger.info("SQL error: {}".format(error))
					logger.info(q)
			c.execute("commit")
			for q in sql2:
				try:
					c.execute(q)
				except cx_Oracle.Error as error:
					logger.info("SQL2 error: {}".format(error))
					logger.info(q)
					
			#Reset all SQL Lists	
			sql = list()
			sql2 = list()
			#Update Zone BBOX record to indicate completion
			c.execute("update LZ_ZONE_BBOX set processed = CURRENT_TIMESTAMP where lz_zone_bbox_id = {}".format(bboxId))
			c.execute("commit")
			logger.debug("Committed lz_zone_bbox_id {}".format(bboxId))
		else:
			#Update Zone BBOX record to indicate completion, but do not commit in case process fails
			c.execute("update LZ_ZONE_BBOX set processed = CURRENT_TIMESTAMP where lz_zone_bbox_id = {}".format(bboxId))

		#Progress tracking
		pc = (count + 1)/len(df_bbox)*10
		pc_rd = math.floor(pc/10) + 2
		loading_msg2 = "{}% {}[{}/{}]".format(int(pc + 20),loading_msg,count,len(df_bbox))

		loadingBar(pc_rd,loading_msg2)

def get_updated_lots(lzId):
	#TO-DO Handle cases where there are no lots to update

	oIDInput = '' #Initialise string for Lot Object Ids
	lots = list() #Store lots that intersect with zone layers
	sql = list() #Store queries to commit for insert into lz_lot_spatial
			
	df_bbox = pd.read_sql("select lz_zone_bbox_id, lz_update_log_id, spatial_ref, bbox from LZ_ZONE_BBOX where lz_update_log_id = {} and processed is null order by lz_zone_bbox_id".format(lzId),connection)
	#print(df_bbox)
	lcount = 0
	
	#Get Next Run ID
	runId = getNextId("LZ_LOT_RUN_ID","LZ_LOT_RUN")
	#Get Next Run number
	runNo = getNextId("LOT_RUN","LZ_LOT_SPATIAL")
	
	#Add records from LOT layer updated within timeframe to list
	df_lots_to_add = pd.read_sql("select start_date, end_date from LZ_UPDATE_LOG where lz_update_log_id = {}".format(lzId),connection)
	
	for i, row in df_lots_to_add.iterrows():
		#print("UP TO FINAL LOT EXTRACT ROUND: {} -> {}".format(row["START_DATE"].strftime('%Y-%m-%d %H:%M:%S'),row["END_DATE"].strftime('%Y-%m-%d %H:%M:%S')))
		
		#Update start date based on 'day_backtrack' value
		og_start_date = row["START_DATE"]
		og_end_date = row["END_DATE"]
		start_date = og_start_date - timedelta(days=day_backtrack)
		end_date = og_end_date - timedelta(days=day_backtrack)
		
		params = {
			'f':'json',
			'outFields':'objectid',
			'returnGeometry':'false',
			'returnIdsOnly':'true',
			'where':"modifieddate >= TIMESTAMP '{}' AND modifieddate < TIMESTAMP '{}'".format(start_date.strftime('%Y-%m-%d %H:%M:%S'),end_date.strftime('%Y-%m-%d %H:%M:%S'))
		}

		retries_1 = 0
		success_1 = False
		
		while not success_1:
			jsonResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
			#logger.debug("Getting objIDs in Zone BBOX: {}".format(jsonResult))
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
					
					#logger.debug("lcount = {} - lotLimit = {} - total Objects = {}".format(lcount,lotLimit,len(jsonResult['objectIds'])))
					
					if lcount % lotLimit == 0 or lcount == len(jsonResult['objectIds']):
						params = {
							'f':'json',
							'outFields':'lotidstring',
							'returnGeometry':'false',
							'returnDistinctValues':'true',
							'where':'objectid in ({})'.format(oIDInput)
						}
						jsonLotResult = getRESTData(LotUrl, params, "Lot Cadastre Service")
						#logger.debug("Extract LOTREFS for OIDs: {}".format(oIDInput))
						if jsonLotResult.get('features'):
							success_1 = True
							for lotref in jsonLotResult["features"]:
								#build up list of lots to insert
								#logger.debug("Appending to lots(): {}".format(lotref["attributes"]["lotidstring"]))
								lots.append(lotref["attributes"]["lotidstring"])
						else:
							retries_1 += 1
							print("ERROR no Features: {}".format(jsonLotResult))
							logger.info("[ERROR] Results do not contain features, retrying.. {}".format(jsonLotResult))
						
						oIDInput = '' #Reset
			else:
				if jsonResult.get('objectIdFieldName'):
					#Result is valid, but there are no objectIds
					logger.info("[PROCESS] No new lots within {} - {}".format(start_date.strftime('%Y-%m-%d %H:%M:%S'),end_date.strftime('%Y-%m-%d %H:%M:%S')))
					return 
				else:
					retries_1 += 1
					print("ERROR no ObjectIds: {}".format(jsonResult))
					logger.info("[ERROR] Results do not contain objectIds, retrying.. {}".format(jsonResult))
				
			#If REST calls were successful, insert into table
			if success_1:
				#All Lots extracted, insert into table
				query = "insert all "

				#Set next LZ_LOT_SPATIAL_ID
				nextLsId = getNextId("LZ_LOT_SPATIAL_ID","LZ_LOT_SPATIAL")

				#Debug
				#logger.debug("Total lots in Lots() is {}".format(len(lots)))

				for i, lotref in enumerate(lots):
					query = "{} into LZ_LOT_SPATIAL (LZ_LOT_SPATIAL_ID, LZ_UPDATE_LOG_ID, LOT_RUN, LOTREF, CREATE_DATE) values ({}, {}, {}, '{}', CURRENT_TIMESTAMP)".format(query,nextLsId,lzId,runNo,lotref)
					
					nextLsId += 1
					#logger.debug("SQL {} : {}".format(i, " into LZ_LOT_SPATIAL (LZ_LOT_SPATIAL_ID, LZ_UPDATE_LOG_ID, LOT_RUN, LOTREF, CREATE_DATE) values ({}, {}, {}, '{}', CURRENT_TIMESTAMP)".format(nextLsId,lzId,runNo,lotref)))
					
					if (i + 1) % 1000 == 0 or (i + 1) == len(lots):
						query = "{} select 1 from dual".format(query)
						
						#Add to commit queue
						sql.append(query)

						query = "insert all "

				logger.debug('LOT EXTRACT FOR DATE RANGE : {} {}'.format(start_date.strftime('%Y-%m-%d %H:%M:%S'),end_date.strftime('%Y-%m-%d %H:%M:%S')))
				logger.debug("Total lots is: {}".format(lcount))

				runNo += 1
				geoInput = ''
				lcount = 0
				lots = list()
			else:
				#Issue with REST Call, retry
				while retries_1 > 9:
					select = input("\nResults from Lot Service are incorrect and failed 10 times, Do you want to try again? y/n\n")
					if select == "y":
						retries_1 = 0
						break
					elif select == "n":
						print("Lot Zoning update process Aborted!!")
						logger.info("[EXIT] Lot Zone Update process aborted by user")
						sys.exit()
					else:
						print("Invalid selection. Please enter y or n")
	#Commit all Lot queries
	for q in sql:
		c.execute(q)
		
	#Update LZ_UPDATE_LOG to indicate all Lot runs complete
	c.execute("update LZ_UPDATE_LOG set LOT_RUN_COMPLETE = CURRENT_TIMESTAMP where lz_update_log_id = {}".format(lzId))
	
	c.execute("commit")
			
def intersectLotZone(lzId,layerName):
	#Tabulate Intersect Lot Layer with current Zone layer
	logger.info("Intersecting Lots with Zones...")
	
	#arcpy.analysis.TabulateIntersection("{}\\lots_to_update".format(arcFolder), "lotidstring", ZoningLayer, "{}\\{}".format(arcFolder,layerName), "EPI_NAME;EPI_TYPE;SYM_CODE;LAY_CLASS", None, "10 Centimeters", "SQUARE_METERS")

	try:
		# Construct file paths
		lots_to_update_path = "{}\\lots_to_update".format(arcFolder)
		output_path = "{}\\{}".format(arcFolder, layerName)

		# Log paths for debugging purposes
		# logger.debug("Lots to update path: {}".format(lots_to_update_path))
		# logger.debug("Zoning layer: {}".format(ZoningLayer))
		# logger.debug("Output path: {}".format(output_path))
		
		#logger.debug('Trying: arcpy.analysis.TabulateIntersection("{}","lotidstring","{}","{}","EPI_NAME;EPI_TYPE;SYM_CODE;LAY_CLASS",None,"10 Centimeters","SQUARE_METERS")'.format(lots_to_update_path,ZoningLayer,output_path))
		
		# Perform the tabulate intersection
		arcpy.analysis.TabulateIntersection(
			lots_to_update_path, 
			"lotidstring", 
			ZoningLayer, 
			output_path, 
			"EPI_NAME;EPI_TYPE;SYM_CODE;LAY_CLASS", 
			None, 
			"10 Centimeters", 
			"SQUARE_METERS"
		)

	except arcpy.ExecuteError:
		logger.info("[ERROR] ArcPy error: {}".format(arcpy.GetMessages(2)))
		time.sleep(20)
		
		logger.info("[RESET] Tabulate Intersection failed, restarting script...")
		print("Tabulate Intersection Failed, Restarting...                                                                 ")
		os.system('""{}\\Lot Planning Refresh.bat""'.format(f_dir))

		sys.exit()

	except Exception as e:
		logger.info("[ERROR] Unexpected error: {}".format(str(e)))
		time.sleep(20)
		
		logger.info("[RESET] Tabulate Intersection failed, restarting script...")
		print("Tabulate Intersection Failed, Restarting...                                 ")
		os.system('""{}\\Lot Planning Refresh.bat""'.format(f_dir))

		sys.exit()
	
	connection = pool.acquire() #Acquire connection from pool
	#c = connection.cursor()

	logger.info("[PROCESS] Tabulate Intersection complete for lz_update_log_id: {}".format(lzId))

def insertToUpdate(lzId,layerName,df_lots):
	#Insert updated Lot Zone records to LZ_TO_UPDATE
	record = 0
	query = "insert all "
	lzTuId = getNextId("LZ_TO_UPDATE_ID","LZ_TO_UPDATE")
	
	fieldNames = ["OBJECTID","lotidstring","EPI_NAME","EPI_TYPE","SYM_CODE","LAY_CLASS","AREA","PERCENTAGE"]
	totRecords = int(arcpy.management.GetCount("{}\\{}".format(arcFolder,layerName))[0]) #Total Lot Zone to update records

	with arcpy.da.SearchCursor("{}\\{}".format(arcFolder,layerName),fieldNames) as cur:
		for row in cur:
			#logger.debug("into LZ_TO_UPDATE (LZ_TO_UPDATE_ID, LZ_UPDATE_LOG_ID, LOTREF, EPI_NAME, EPI_TYPE, SYM_CODE, LAY_CLASS, SUM_AREA, PERCENTAGE, CREATE_DATE) values ({},{},'{}','{}','{}','{}','{}',{},{},CURRENT_TIMESTAMP)".format(lzTuId,lzId,row[1],row[2],row[3],row[4],row[5],row[6],row[7]))
			query = "{} into LZ_TO_UPDATE (LZ_TO_UPDATE_ID, LZ_UPDATE_LOG_ID, LOTREF, EPI_NAME, EPI_TYPE, SYM_CODE, LAY_CLASS, SUM_AREA, PERCENTAGE, CREATE_DATE) values ({},{},'{}','{}','{}','{}','{}',{},{},CURRENT_TIMESTAMP)".format(query,lzTuId,lzId,row[1],row[2],row[3],row[4],row[5],row[6],row[7])
			record += 1
			lzTuId += 1
			
			if record % 1000 == 0 or record == totRecords:
				#Every 1000 records insert
				query = "{} select 1 from dual".format(query)
				
				try:
					#c.execute(query)
					execute_with_retries(connection,query)
				except cx_Oracle.Error as error:
					logger.info("[ERROR] {}".format(error))
					print(error)
					
				query = "insert all "
	
	#Update LZ_LOT_SPATIAL Status to complete
	query = ""
	for i, row in df_lots.iterrows():
		#Go through all processed lots and update processed date
		if query == "":
			query = "'{}'".format(row["LOTREF"])
		else:
			query = "{},'{}'".format(query,row["LOTREF"])
		
		if (i + 1) % 1000 == 0 or (i + 1) == len(df_lots):
			#logger.debug("Run SQL: {}".format(query))
			execute_with_retries(connection,"update LZ_LOT_SPATIAL set processed = CURRENT_TIMESTAMP where lz_update_log_id = {} and lotref in ({})".format(lzId,query))
			
			query = ""
	
	#Once all records are inserted, commit
	c.execute("commit")
	
	#Clean up Lot_Zone_to_update layer
	arcpy.Delete_management("{}\\{}".format(arcFolder,layerName))
	
def updateLotZone(lzId,pc_rd,loading_msg):
	#Go through LZ_TO_UPDATE to determine update action for LOT_ZONE table
	logger.info("Updating LOT_ZONE for lz_update_log_id: {}".format(lzId))
	
	query = ""
	query2 = ""
	query3 = ""
	
	#Check which LOT_ZONES to expire
	df_lz_expire = pd.read_sql("select lz.lot_zone_id from lot_zone lz where exists (select * from lz_to_update ltu where ltu.lotref = lz.lotref and ltu.lz_update_log_id = {} and ltu.processed is null) and not exists (select * from lz_to_update ltu where lz.lotref = ltu.lotref and lz.sym_code = ltu.sym_code and lz.lay_class = ltu.lay_class and ltu.lz_update_log_id = {} and ltu.processed is null) and lz.end_date is null".format(lzId,lzId),connection)
	
	#Expire LOT_ZONE records
	for i, row in df_lz_expire.iterrows():
		if query == "":
			query = "{}".format(row["LOT_ZONE_ID"])
		else:
			query = "{},{}".format(query,row["LOT_ZONE_ID"])

		if (i + 1) % 1000 == 0 or (i + 1) == len(df_lz_expire):
			c.execute("update LOT_ZONE set end_date = CURRENT_TIMESTAMP, update_date = CURRENT_TIMESTAMP where lot_zone_id in ({})".format(query))
			query = ""
			
	c.execute("commit")
	logger.info("Finished Expiry step")
	
	#Check which LZ_TO_UPDATE records do not need to update LOT_ZONE records ##SET ROUNDING FOR SUM_AREA AND PERCENTAGE HERE##
	df_no_update = pd.read_sql("select ltu.lz_to_update_id from lot_zone lz, lz_to_update ltu where lz.lotref = ltu.lotref and lz.sym_code = ltu.sym_code and lz.lay_class = ltu.lay_class and round(lz.percentage,0) = round(ltu.percentage,0) and round(lz.sum_area,0) = round(ltu.sum_area,0) and ltu.processed is null and ltu.update_action is null and lz.end_date is null and ltu.lz_update_log_id = {}".format(lzId),connection)
	
	#Update status for records requiring no update
	for i, row in df_no_update.iterrows():
		if query == "":
			query = "{}".format(row["LZ_TO_UPDATE_ID"])
		else:
			query = "{},{}".format(query,row["LZ_TO_UPDATE_ID"])
		
		if (i + 1) % 1000 == 0 or (i + 1) == len(df_no_update):
			c.execute("update LZ_TO_UPDATE set update_action = 'NO UPDATE', processed = CURRENT_TIMESTAMP where lz_to_update_id in ({})".format(query))
			loadingBar(pc_rd,"{} [{}/{}] No updates required          ".format(loading_msg,(i + 1),len(df_no_update)))
			#print(i + 1,"/",len(df_no_update)," No updates done", end="\r")
			query = ""
			
	c.execute("commit")
	logger.info("Finished No Update check")
	
	#Check for records where just the sum_area or Percentage need update
	df_to_update = pd.read_sql("select ltu.lz_to_update_id, ltu.sum_area, ltu.percentage, lz.lot_zone_id from lot_zone lz, lz_to_update ltu where lz.lotref = ltu.lotref and lz.sym_code = ltu.sym_code and lz.lay_class = ltu.lay_class and ltu.processed is null and ltu.update_action is null and lz.end_date is null and ltu.lz_update_log_id = {}".format(lzId),connection)
	
	sa_query = ""
	pc_query = ""
	lzId_query = ""
	ltu_query = ""
	
	#Update LOT_ZONE and LZ_TO_UPDATE (SUM AREA and PERCENTAGE)
	for i, row in df_to_update.iterrows():
		
		#Build CASE statements for sum_area update
		sa_query = "{} when {} then {} ".format(sa_query,row["LOT_ZONE_ID"],row["SUM_AREA"])
		
		#Build CASE statements for percentage update
		pc_query = "{} when {} then {} ".format(pc_query,row["LOT_ZONE_ID"],row["PERCENTAGE"])
		
		if lzId_query == "":
			lzId_query = "{}".format(row["LOT_ZONE_ID"])
		else:
			lzId_query = "{},{}".format(lzId_query,row["LOT_ZONE_ID"])
			
		if ltu_query == "":
			ltu_query = "{}".format(row["LZ_TO_UPDATE_ID"])
		else:
			ltu_query = "{},{}".format(ltu_query,row["LZ_TO_UPDATE_ID"])
		
		if (i + 1) % 1000 == 0 or (i + 1) == len(df_to_update):
			c.execute("update LOT_ZONE set sum_area = case lot_zone_id {} end, percentage = case lot_zone_id {} end, update_date = CURRENT_TIMESTAMP where lot_zone_id in ({})".format(sa_query,pc_query,lzId_query))
			c.execute("update LZ_TO_UPDATE set update_action = 'UPDATE', processed = CURRENT_TIMESTAMP where lz_to_update_id in ({})".format(ltu_query))
			loadingBar(pc_rd,"{} [{}/{}] Updates done             ".format(loading_msg,(i + 1),len(df_to_update)))
			#print(i + 1,"/",len(df_to_update)," Updates done", end="\r")
			sa_query = ""
			pc_query = ""
			lzId_query = ""
			ltu_query = ""
			
	c.execute("commit")
	logger.info("Finished Update step")
	
	#Check for Records from LZ_TO_UPDATE to Insert
	df_to_insert = pd.read_sql("select ltu.lz_to_update_id, ltu.lotref, ltu.epi_name, ltu.epi_type, ltu.sym_code, ltu.lay_class, ltu.sum_area, ltu.percentage from lz_to_update ltu where not exists (select * from lot_zone lz where lz.lotref = ltu.lotref and lz.sym_code = ltu.sym_code and lz.lay_class = ltu.lay_class and lz.end_date is null) and ltu.lz_update_log_id = {}".format(lzId),connection)
	
	query = "insert all "
	
	#Insert new records to LOT_ZONE and update LZ_TO_UPDATE
	lz_id = getNextId("LOT_ZONE_ID","LOT_ZONE")
	for i, row in df_to_insert.iterrows():
		
		query = "{} into LOT_ZONE (LOT_ZONE_ID, LOTREF, EPI_NAME, EPI_TYPE, SYM_CODE, LAY_CLASS, SUM_AREA, PERCENTAGE, CREATE_DATE, UPDATE_DATE) values ({},'{}','{}','{}','{}','{}',{},{},CURRENT_TIMESTAMP,CURRENT_TIMESTAMP)".format(query,lz_id,row["LOTREF"],row["EPI_NAME"],row["EPI_TYPE"],row["SYM_CODE"],row["LAY_CLASS"],row["SUM_AREA"],row["PERCENTAGE"])
		
		if query2 == "":
			query2 = "{}".format(row["LZ_TO_UPDATE_ID"])
		else:
			query2 = "{},{}".format(query2,row["LZ_TO_UPDATE_ID"])

		lz_id += 1
		
		if (i + 1) % 1000 == 0 or (i + 1) == len(df_to_insert):
			query = "{} select 1 from dual".format(query)
			try:
				c.execute(query)
			except cx_Oracle.Error as error:
				logger.info("[ERROR] {}".format(error))
				print(error)
				
			query2 = "update LZ_TO_UPDATE set processed = CURRENT_TIMESTAMP, update_action = 'INSERT' where lz_to_update_id in ({})".format(query2)
			try:
				c.execute(query2)
			except cx_Oracle.Error as error:
				logger.info("[ERROR] {}".format(error))
				print(error)
			
			loadingBar(pc_rd,"{} [{}/{}] Inserts done        ".format(loading_msg,(i + 1),len(df_to_insert)))
			#print(i + 1,"/",len(df_to_insert)," Inserts done", end="\r")
			query = "insert all "
			query2 = ""
			
	c.execute("commit")
	logger.info("Finished Insert step")
			
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
	
	#If set to local, check to see if arcGIS folder is present
	if h_dir == "C:\\TMP\\Python\\Lot_Zone":
		source_dir = r"G:\Strategy\GPR\10. Support Applications & Tools\Python\Planning Update by exception\files"
		target_dir = r"C:\TMP\Python\Lot_Zone"
		folder_name = "arcGIS"
		
		check_and_copy_directory(source_dir, target_dir, folder_name)
	
	#Connection files
	ZoningLayer = "{}\\arcGIS\\PlanningSDE.sde\\PlanningDB.SDE.EPI\\PlanningDB.SDE.EPI_Land_Zoning".format(h_dir)
	LotLayer = "{}\\arcGIS\\DCDB_SDE.sde\\DCDB_DELIVERY.sde.LotAll".format(h_dir)
	LotUrl = "https://maps.six.nsw.gov.au/arcgis/rest/services/sixmaps/Cadastre/MapServer/0/query"
	
	#ArcPy Settings
	logger.info("[PROCESS] Setting up ArcGIS connection to Planning SDE")
	env.overwriteOutput = True
	if env_mode == "PROD":
		arcFolder = "{}\\arcGIS\\lot_zone_update.gdb".format(h_dir)
	elif env_mode == "UAT":
		arcFolder = "{}\\arcGIS\\test.gdb".format(h_dir)
	LZ_to_update = "{}\\LandZoning_to_update".format(arcFolder)
	logger.info("[PROCESS] Connected to Planning SDE")
	
	loadingBar(0,"0% - Checking for unfinished updates...")
	
	#Check if there are unprocessed Zones (to get list of lots from)
	df_zone_to_process = pd.read_sql("select lz_update_log_id, count(*) total_records from LZ_ZONE_BBOX where processed is null group by lz_update_log_id order by lz_update_log_id",connection)
	if len(df_zone_to_process) > 0:
		loadingBar(2,"20% [Resuming previous update] - Processing unfinished zones...")
		for i, to_proc in df_zone_to_process.iterrows():
			
			logger.info("[PROCESS] Continued processing Zones for lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
			
			extractLots(to_proc["LZ_UPDATE_LOG_ID"],int(to_proc["TOTAL_RECORDS"]),"[Resuming previous update] - Processing unfinished zones...")
	
	#Check if Lot run for timeframe has been done
	df_update_lots = pd.read_sql("select lz_update_log_id from LZ_UPDATE_LOG where lot_run_complete is null order by lz_update_log_id",connection)
	if len(df_update_lots) > 0:
		loadingBar(3,"30% [Resuming previous update] - Extracting lots updated within time period...       ")
		for i, to_proc in df_update_lots.iterrows():
			logger.info("[PROCESS] Continued processing recently updated lots for lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
			get_updated_lots(to_proc["LZ_UPDATE_LOG_ID"])
	
	#Check if there are unprocessed lots
	df_lz_to_process = pd.read_sql("select distinct lz_update_log_id from LZ_LOT_SPATIAL where processed is null order by lz_update_log_id",connection)
	if len(df_lz_to_process) > 0:
		loadingBar(4,"40% [Resuming previous update] - Extracting spatial data for lots...")
		for ii, to_proc in df_lz_to_process.iterrows():
			logger.info("[PROCESS] Continued processing lots for lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
			
			df_lots_to_create = pd.read_sql("select lot_run, count(*) total_count from (select min(lot_run) lot_run, lotref from lz_lot_spatial where lz_update_log_id = {} and processed is null group by lotref) group by lot_run order by lot_run".format(to_proc["LZ_UPDATE_LOG_ID"]),connection)
		
			lot_runs = list() #Initialise list of Runs
			total_lr = 0 #Track total lots so far
			
			for i, row in df_lots_to_create.iterrows():
				
				lot_runs.append(row["LOT_RUN"]) #Add Lot run to current list
				total_lr += row["TOTAL_COUNT"]
				
				#Progress tracking
				pc = (i + 1)/len(df_lots_to_create)*40
				pc_rd = math.floor(pc/10) + 4
				loading_msg = "{}% [Resuming previous update] - Extracting spatial data and performing intersects...[{}/{}]".format(int(40 + pc),(i + 1),len(df_lots_to_create))
				
				loadingBar(pc_rd,loading_msg)
				
				#When limit is reached, continue to intersect process
				if total_lr > int_limit or (i + 1) == len(df_lots_to_create):
					
					if (i + 1) == len(df_lots_to_create):
						loadingBar(pc_rd,"{}% [Resuming previous update] - Processing final intersect...                                ".format(int(40 + pc)))
					
					total_lztu = 0 #Track how many lz_to_update records need to be inserted
					df_lots = get_lot_runs(to_proc["LZ_UPDATE_LOG_ID"],lot_runs)
					LZ_to_insert = "Lot_Zone_to_update_{}_{}_{}".format(to_proc["LZ_UPDATE_LOG_ID"],lot_runs[0],lot_runs[-1])
					
					#Check to see if there are lz_to_update records to be inserted
					if arcpy.Exists(LZ_to_insert):
						#Check total number of records
						total_lztu = int(arcpy.management.GetCount("{}\\{}".format(arcFolder,LZ_to_insert))[0])
						logger.info("{}\\Lot_Zone_to_update_{}_{}_{} Exists with {} records".format(arcFolder,to_proc["LZ_UPDATE_LOG_ID"],lot_runs[0],lot_runs[-1],total_lztu))

					if total_lztu == 0:
						logger.info("[PROCESS] Processing lots for lz_update_log_id: {} - Lot runs {} - {}".format(to_proc["LZ_UPDATE_LOG_ID"],lot_runs[0],lot_runs[-1]))
						#Intersection results not available yet, run intersection
						createLotLayer(to_proc["LZ_UPDATE_LOG_ID"],LotUrl,lot_runs,df_lots)
						
						intersectLotZone(to_proc["LZ_UPDATE_LOG_ID"],LZ_to_insert)
						
						# Acquire connection from the pool
						connection = pool.acquire()
						c = connection.cursor()
					else:
						#Not a new Intersect data set, resuming from previous unfinished process
						logger.info("Continued inserting records to LZ_TO_UPDATE for lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
					
					insertToUpdate(to_proc["LZ_UPDATE_LOG_ID"],LZ_to_insert,df_lots)
					
					#Reset
					lot_runs = list()
					total_lr = 0
		
	#Check if there are unprocessed 'To update' records
	df_lz_to_update = pd.read_sql("select distinct lz_update_log_id from LZ_TO_UPDATE where processed is null order by lz_update_log_id",connection)
	if len(df_lz_to_update) > 0:
		loading_msg = "90% [Resuming previous update] - Updating Lot Zone table..."
		for i, to_proc in df_lz_to_update.iterrows():
			logger.info("[PROCESS] Continued processing LOT_ZONE updates for lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
			#sys.exit() #Developing chunk processing for Lot JSON Creation
			updateLotZone(to_proc["LZ_UPDATE_LOG_ID"],9,loading_msg)
			
			#Update Log record as complete
			c.execute("update LZ_UPDATE_LOG set finish_date = CURRENT_TIMESTAMP where lz_update_log_id = {}".format(to_proc["LZ_UPDATE_LOG_ID"])) #Update Lot Zone Log to indicate zone is complete
			c.execute("commit")
			logger.info("Finished Resumption of lz_update_log_id: {}".format(to_proc["LZ_UPDATE_LOG_ID"]))
			
		print("[■■■■■■■■■■] 100% [Resuming previous update]                                                                ")
		
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

	c.close()
	pool.release(connection)
	
	#Set current date
	current_date = datetime.today()
	end_period = last_update + timedelta(days=day_chunk) #Get Zoning updates in 30 day chunks
	
	#Make sure end period is not beyond the current date
	if end_period > current_date:
		end_period = current_date
	
	print("Starting Lot-Zone updates for {} -> {}                      ".format(last_update.strftime('%d-%m-%Y'),end_period.strftime('%d-%m-%Y')))
	
	loadingBar(0,"1% - Time frame set...")
	#print("{}".format(last_update.strftime('%Y-%m-%d %H:%M:%S')))
	
	#Iterate through all Updated Zone layers until done
	while last_update < current_date:
	
		#Set Date Range for Zone selection
		date_range_expression = "LAST_EDITED_DATE >= '{}' AND LAST_EDITED_DATE < '{}'".format(last_update.strftime('%Y-%m-%d %H:%M:%S'),end_period.strftime('%Y-%m-%d %H:%M:%S'))
		
		loadingBar(0,"5% - Copying selected Zoning shapes...")
		
		#Copy updated records to new layer 'LandZoning_to_update' (Only in PROD)
		if env_mode == "PROD":
			arcpy.Select_analysis(ZoningLayer, "{}\\LandZoning_to_update".format(arcFolder), where_clause=date_range_expression)
		
		totalRecords = int(arcpy.management.GetCount(LZ_to_update)[0]) #Total Zone records to iterate
		
		#Insert lz_update_log record and get ID
		connection = pool.acquire() #Acquire connection from pool
		c = connection.cursor()
		
		c.execute("insert into LZ_UPDATE_LOG (LZ_UPDATE_LOG_ID,START_DATE,END_DATE,CREATE_DATE,FINISH_DATE,TOTAL_RECORDS,RUN_USER) values (SEQ_LZ_UPDATE_LOG.nextval, TO_DATE('{}', 'yyyy/mm/dd hh24:mi:ss'), TO_DATE('{}', 'yyyy/mm/dd hh24:mi:ss'), CURRENT_TIMESTAMP, null, {}, '{}')".format(last_update.strftime('%Y/%m/%d %H:%M:%S'),end_period.strftime('%Y/%m/%d %H:%M:%S'),totalRecords,username))
		c.execute("commit")
		c.execute("SELECT SEQ_LZ_UPDATE_LOG.currval FROM dual")
		lz_update_log_id = c.fetchone()[0]
		
		loadingBar(1,"10% - Extracting BBOX coordinates for each zone...")
		#Store all Zone Bounding Boxes
		if totalRecords > 0:
			nextBiD = getNextId("LZ_ZONE_BBOX_ID","LZ_ZONE_BBOX")
			with arcpy.da.SearchCursor(LZ_to_update,['OID@','SHAPE@','EPI_NAME','LAY_CLASS','SYM_CODE']) as cursor:
				logger.info("Storing Zone BBOX...")
				zcount = 0
				query = "insert all "
				for row in cursor:
					zoneInfo = "{}|{}|{}".format(row[2],row[3],row[4])
					sRef = row[1].extent.spatialReference.factoryCode
					bbox = '[[{},{}],[{},{}],[{},{}],[{},{}],[{},{}]]'.format(row[1].extent.XMin,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMin,row[1].extent.XMax,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMax,row[1].extent.XMin,row[1].extent.YMin)
					
					query = "{} into LZ_ZONE_BBOX (LZ_ZONE_BBOX_ID, LZ_UPDATE_LOG_ID, LZ_ZONE_OID, LZ_ZONE_INFO, SPATIAL_REF, BBOX) values ({},{},{},'{}','{}','{}')".format(query,nextBiD,lz_update_log_id,row[0],zoneInfo,sRef,bbox)
					
					zcount += 1
					nextBiD += 1
					
					if zcount % 1000 == 0 or zcount == totalRecords:
						#Insert records every 1000
						query = "{} select 1 from dual".format(query)
						
						try:
							c.execute(query)
						except cx_Oracle.Error as error:
							logger.info("[ERROR] {}".format(error))
							print(error)
						
						query = "insert all "
			c.execute("commit")
			logger.debug("Inserted {} Zoning records".format(zcount))
			
		#print("Last inserted ID:", lz_update_log_id)
		
		loadingBar(2,"20% - Extracting Lots for each zone...            ")
		#TO-DO CHANGE TO ITERATE THROUGH BBOX RECORDS TO EXTRACT LOTS
		if totalRecords > 0:
			#GET LOTS FOR EACH ZONE SHAPE
			
			logger.info("[PROCESS] Processing Zones for {} -> {}".format(last_update, end_period))
			#print("[PROCESS] Processing Zones for {} -> {}".format(last_update, end_period))
			logger.debug("Total records are {}".format(totalRecords))
			
			#Go through each record in LandZoning_to_update and find intersected lots
			logger.debug("Going through Zone layers...")
			extractLots(lz_update_log_id, totalRecords, "- Extracting Lots for each zone...      ")
		
		loadingBar(3,"30% - Extracting Lots updated within time period...")
		#Get updated lots within time frame
		get_updated_lots(lz_update_log_id)
		
		#Check total lots required for update
		df_lots_to_create = pd.read_sql("select lot_run, count(*) total_count from (select min(lot_run) lot_run, lotref from lz_lot_spatial where lz_update_log_id = {} and processed is null group by lotref) group by lot_run order by lot_run".format(lz_update_log_id),connection)
		
		lot_runs = list() #Initialise list of Runs
		total_lr = 0 #Track total lots so far
		
		loadingBar(4,"40% - Extracting spatial data for lots...")
		#if total_lots > 0:
		for i, row in df_lots_to_create.iterrows():
			
			lot_runs.append(row["LOT_RUN"]) #Add Lot run to current list
			total_lr += row["TOTAL_COUNT"]
			
			#Progress tracking
			pc = (i + 1)/len(df_lots_to_create)*50
			pc_rd = math.floor(pc/10) + 4
			loading_msg = "{}% - Extracting spatial data and performing intersects...[{}/{}]                              ".format(int(40 + pc),(i + 1),len(df_lots_to_create))
			
			loadingBar(pc_rd,loading_msg)
			
			#When limit is reached, continue to intersect process
			if total_lr > int_limit or (i + 1) == len(df_lots_to_create):
				
				if (i + 1) == len(df_lots_to_create):
					loading_msg = "{}% - Final intersection...".format(int(40 + pc))
				
				#Get Lots to create
				df_lots = get_lot_runs(lz_update_log_id,lot_runs)
				
				#Create Lot Spatial Layer
				logger.info("[PROCESS] Processing lots for lz_update_log_id: {} - Lot runs {} - {}".format(lz_update_log_id,lot_runs[0],lot_runs[-1]))
				createLotLayer(lz_update_log_id,LotUrl,lot_runs,df_lots)
				
				#Tabulate Intersect Lot layer with current Zone layer
				intersectLotZone(lz_update_log_id,"Lot_Zone_to_update_{}_{}_{}".format(lz_update_log_id,lot_runs[0],lot_runs[-1]))

				# Acquire connection from the pool (long intersects cause timeouts)
				connection = pool.acquire()
				c = connection.cursor()
				
				#Store Intersected Results to LZ_TO_UPDATE
				insertToUpdate(lz_update_log_id,"Lot_Zone_to_update_{}_{}_{}".format(lz_update_log_id,lot_runs[0],lot_runs[-1]),df_lots)
				#sys.exit() #Developing chunk processing for Lot JSON Creation
				#Update Lot_Zone table
				loading_msg = "{}% - Extracting spatial data and performing intersects...".format(int(40 + pc)) #give space for Lot Zone count
				updateLotZone(lz_update_log_id,pc_rd,loading_msg)
				
				#Reset
				lot_runs = list()
				total_lr = 0
		
		print("[■■■■■■■■■■] 100% [{} -> {}]                                                           ".format(last_update.strftime('%Y-%m-%d'),end_period.strftime('%Y-%m-%d')))
		logger.info("[PROCESS] Finished updating Lot Zones for {} -> {}".format(last_update,end_period))
		print("[PROCESS] Finished updating Lot Zones for {} -> {}".format(last_update,end_period))
		
		#Update Log record as complete
		c.execute("update LZ_UPDATE_LOG set finish_date = CURRENT_TIMESTAMP where lz_update_log_id = {}".format(lz_update_log_id)) #Update Lot Zone Log to indicate zone is complete
		c.execute("commit")
		
		c.close()
		pool.release(connection)
		
		#Delete layer to release lock
		if arcpy.Exists(LZ_to_update):
			logger.info("[PROCESS] Deleting {}".format(LZ_to_update))
			arcpy.Delete_management(LZ_to_update)
		
		#Finished Zoning chunk, set up for next x days
		last_update = end_period
		end_period = end_period + timedelta(days=day_chunk) #set up next chucnk
	
	logger.info("[FINISH] Lot_Zone Update process finished")
	print("Done!")