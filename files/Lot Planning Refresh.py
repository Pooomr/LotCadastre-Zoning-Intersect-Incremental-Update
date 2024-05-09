''' Update Lot_Zone table Proof of Concept
	v1 - First version - Select records based on last_update_date filter working
'''

import logging
import sys
logging.basicConfig(level=logging.DEBUG)
username = sys.argv[1]
logging.basicConfig(filename="log.txt",
					level=logging.INFO,
					format="%(asctime)s - {} - %(message)s".format(username),
					datefmt='%d/%m/%Y %H:%M:%S')
logging.debug("Importing Python Packages...")
logging.info("[START] Lot_Zone Update process started")

try:
	import arcpy
except:
	print("Error Importing arcpy module, make sure OpenVPN is connected!")
	logging.info("[STOPPED] Unable to import arcpy module, GPR electorate update Stopped")
	sys.exit()

import os
from arcpy import env
import pandas as pd
from datetime import datetime, timedelta
import config
import cx_Oracle

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
	#LotLayer = "{}\\arcGIS\\DCDB_SDE.sde\\DCDB_DELIVERY.sde.LotAll".format(os.getcwd())
	LotUrl = "https://mapprod3.environment.nsw.gov.au/arcgis/rest/services/ePlanning/Planning_Portal_Administration/MapServer/3"
	LotLayer = arcpy.mp.MakeImageServerLayer(LotUrl) #STUCK HERE TRYING TO SORT OUT THE LOT LAYER, MAY NEED TO ADD LAYER TO MAP SIMILAR TO HOW IT IS DONE IN ARCGIS PRO DESKTOP
	
	#ArcPy Settings
	logging.debug("[DEBUG] Setting up ArcGIS connection to Planning SDE")
	env.overwriteOutput = True
	#env.workspace = "{}\\arcGIS\\PlanningSDE.sde\\PlanningDB.SDE.EPI".format(os.getcwd())
	#env.workspace = "{}\\arcGIS\\DCDB_SDE.sde".format(os.getcwd())
	arcFolder = "{}\\arcGIS\\lot_zone_update.gdb".format(os.getcwd())
	#fcList = arcpy.ListFeatureClasses()
	logging.debug("[DEBUG] Connected to Planning SDE")

	# if fcList == None:
		# logging.info("[ERROR] Unable to retrieve results. Please check SDE Connection")
		# print("Error with SDE Connection. Run 'Configure SDE Connection' to refresh connection file")
		# sys.exit()

	# #List layers
	# for fc in fcList:
		# print(fc)
	
	print("Test Selection...")
	logging.info("[INFO] Selecting records")
	
	#Loop through updated Zone records one month at a time
	start_period = last_update #start of loop
	end_period = last_update + timedelta(days=30) #end of first chunk
	current_update = datetime(2023, 3, 5, 12, 58, 57) #Testing
	end_period = datetime(2023, 3, 5, 12, 58, 57) #Testing
	
	#TEST LOT TABLE
	table = arcpy.management.SelectLayerByAttribute(ZoningLayer, "NEW_SELECTION", "OBJECTID = 111", None)
	print("Attributes selected")
	columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] 
	df = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)

	print(df)
	
	while start_period < current_update:
		print("{} -> {}".format(start_period,end_period))
		
		date_range_expression = "LAST_EDITED_DATE >= '{}' AND LAST_EDITED_DATE < '{}'".format(start_period.strftime('%Y-%m-%d %H:%M:%S'),end_period.strftime('%Y-%m-%d %H:%M:%S'))
		
		#Copy updated records to new layer 'LandZoning_to_update'
		arcpy.Select_analysis(ZoningLayer, "{}\\LandZoning_to_update".format(arcFolder), where_clause=date_range_expression)
		
		#Tabulate intersect with Lot Layer to get list of lots to update
		arcpy.analysis.TabulateIntersection("{}\\LandZoning_to_update".format(arcFolder), "OBJECTID", LotLayer, "{}\\Lots_to_update".format(arcFolder), "OBJECTID;CADID;LOTNUMBER;SECTIONNUMBER;PLANLABEL", None, None, "UNKNOWN")
		#arcpy.management.SelectLayerByAttribute("{}\\arcGIS\\PlanningSDE.sde\\PlanningDB.SDE.EPI\\PlanningDB.SDE.EPI_Land_Zoning".format(os.getcwd()), "NEW_SELECTION", date_range_expression, None)
		
		#arcpy.management.CopyFeatures("{}\\arcGIS\\PlanningSDE.sde\\PlanningDB.SDE.EPI\\PlanningDB.SDE.EPI_Land_Zoning".format(os.getcwd()), "{}\\LandZoning_to_update".format(arcFolder), None, None, None, None)
		
		#Set up next chunk
		start_period = end_period
		end_period = end_period + timedelta(days=30)
	
	print("Last_update: {}".format(last_update))
	update_period = last_update + timedelta(days=30)
	print("Last_update + 30: {}".format(update_period))
	
	#date_range_expression = "LAST_EDITED_DATE >= '{}' AND LAST_EDITED_DATE < '{}'".format(last_update.strftime('%Y-%m-%d %H:%M:%S'),update_period.strftime('%Y-%m-%d %H:%M:%S'))
	
	#Testing
	date_range_expression = "LAST_EDITED_DATE >= '2022-10-06 12:58:57' AND LAST_EDITED_DATE < '2023-03-05 12:58:57'"
	
	print("date_range_expression: {}".format(date_range_expression))
	
	table = arcpy.management.SelectLayerByAttribute("{}\\PlanningSDE.sde\\PlanningDB.SDE.EPI\\PlanningDB.SDE.EPI_Land_Zoning".format(os.getcwd()), "NEW_SELECTION", date_range_expression, None)
	print("Attributes selected")
	columns = [f.name for f in arcpy.ListFields(table) if f.type!="Geometry"] 
	df = pd.DataFrame(data=arcpy.da.SearchCursor(table, columns), columns=columns)

	print(df)
	logging.info("[FINISH] Lot_Zone Update process finished")
	print("Done!")