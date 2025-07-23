#config file for GPR Electorate Update
#v2 - Updated for SQLAlchemy compatibility

env_mode = "UAT" #PROD or UAT or DEV

if env_mode == "PROD":
	# Settings for GPR Oracle Database connection PROD
	username = 'mtran'
	password = 'mtran1'
	dsnDCS = '10.5.193.201' #DCS
	dsnDPE = '10.91.102.159' #DPE
	serviceName = 'GPR.SRV-BX-GPR-P'
	port = 1521
	encoding = 'UTF-8'
elif env_mode == "UAT":
	#Settings for GPR Oracle Database connection UAT
	username = 'mtran'
	password = 'mtran1'
	dsnDPE = '10.91.102.160'
	serviceName = 'GPRUAT.SRV-BX-GPR-T'
	port = 1521
	encoding = 'UTF-8'
elif env_mode == "DEV":
	# Settings for GPR Oracle Database connection DEV
	username = 'gpradmin'
	password = 'gpradmin'
	dsnDPE = '10.91.102.161'
	serviceName = 'GPRDEV.SRV-BX-GPR-D'
	port = 1521
	encoding = 'UTF-8'