cli = require('cli').enable('help', 'status', "version")

config = require( "../../lib/config" )

cli.setApp( "Media-API Migration" )

exports.run = ->
	cli.parse(
		domain: [ "d", "Domain to migrate", "string" ]
		sdbaccesskey: [ "sa", "AWS Access Key", "string" ]
		sdbsecret:  [ "ss", "AWS Access Key", "string" ]
		sdbregion:  [ "sr", "AWSRegion", "string", "" ]
		dynaccesskey: [ "da", "AWS Access Key", "string" ]
		dynsecret:  [ "ds", "AWS Access Key", "string" ]
		dynregion:  [ "dr", "AWSRegion", "string", "" ]
	)
	cli.main ( args, options )->
		
		_cnf =
			migration:
				domain: options.domain
		
		_cnf.simpledb.accessKeyId = options.sdbaccesskey if options.sdbaccesskey?.length
		_cnf.simpledb.secretAccessKey = options.sdbsecret if options.sdbsecret?.length
		_cnf.simpledb.region = options.sdbregion if options.sdbregion?.length

		_cnf.dynamo.accessKeyId = options.dynaccesskey if options.dynaccesskey?.length
		_cnf.dynamo.secretAccessKey = options.dynsecret if options.dynsecret?.length
		_cnf.dynamo.region = options.dynregion if options.dynregion?.length
		
		config.init( _cnf )

		Migration = require( "../../" )

		try
			_mig = new Migration()

			_process_count = 0
			_process_loaded = 0
			_mig.on "loadAllDataStart", ( count )=>
				_process_count = count
				cli.ok "Load all #{count} items ..."
				cli.progress( 0 )
				return 

			_mig.on "loadAllDataStop", =>
				_process_count = 0
				return 

			_mig.on "loadAllDataReceived", ( loaded )=>
				_process_loaded = _process_loaded + loaded
				cli.progress( _process_loaded / _process_count )
				return 

			_mig.start ( err, resp )=>
				if err
					console.log _err.stack
					cli.error( err )
				else
					cli.ok( resp )
				process.exit()
				return
		catch _err
			console.log _err.stack
			cli.error( _err )
		return
	return

process.on "uncaughtException", ( _err )=>
	cli.error( _err )
	console.log _err.stack
	process.exit()
	return