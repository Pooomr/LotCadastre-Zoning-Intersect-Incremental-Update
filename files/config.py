#config file for GPR Electorate Update

env_mode = "UAT" #PROD or UAT or DEV

if env_mode == "PROD":
	# Settings for GPR Oracle Database connection PROD
	username = ''
	password = ''
	dsnDCS = '' #DCS
	dsnDPE = '' #DPE
	port = 1521
	encoding = 'UTF-8'
elif env_mode == "UAT":
	#Settings for GPR Oracle Database connection UAT
	username = ''
	password = ''
	dsnDPE = ''
	port = 1521
	encoding = 'UTF-8'
elif env_mode == "DEV":
	# Settings for GPR Oracle Database connection DEV
	username = ''
	password = ''
	dsnDPE = ''
	port = 1521
	encoding = 'UTF-8'