#Generate a new SDE connection file, result will be stored in the same directory as this file
import arcpy
import os
from arcpy import env

env.overwriteOutput = True

#Replace or create SDE connection files

#Planning DB
arcpy.management.CreateDatabaseConnection("{}//arcGIS".format(os.getcwd()), "PlanningSDE", "SQL_SERVER", "**SERVER NAME**", "OPERATING_SYSTEM_AUTH", None, "*****", "SAVE_USERNAME", "PlanningDB", None, "TRANSACTIONAL", "sde.DEFAULT", None)

#DCDB Delivery
arcpy.management.CreateDatabaseConnection("{}//arcGIS".format(os.getcwd()), "DCDB_SDE", "SQL_SERVER", "**SERVER NAME**", "OPERATING_SYSTEM_AUTH", None, "*****", "SAVE_USERNAME", "DCDB_DELIVERY", None, "TRANSACTIONAL", "sde.DEFAULT", None)

print("Successfully updated PlanningDB.SDE and DCDB.SDE file with credentials for user {}".format(sys.argv[1]))