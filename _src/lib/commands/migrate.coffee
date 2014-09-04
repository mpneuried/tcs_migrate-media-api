cli = require('cli').enable('help', 'status', "version")

config = require( "../../lib/config" )

cli.setApp( "Media-API Migration" )

multimeter = require( "multimeter" )
multi = multimeter(process);
charm = multi.charm;
charm.on('^C', process.exit);
charm.reset();

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
		process.stdout.write '\u001B[2J\u001B[0;0f'
		loadbar = multi(20,7, { width: 70 })
		writebar = multi(20,8, { width: 70 })
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
				cli.ok "Load all #{count} items ...\nLoading SimpleDB:\nDynamo write cache:"
				#cli.progress( 0 )
				return 

			_mig.on "loadAllDataStop", =>
				_process_count = 0
				return 

			_mig.on "loadAllDataReceived", ( loaded )=>
				_process_loaded = _process_loaded + loaded
				#cli.progress( _process_loaded / _process_count )
				_prec = ( _process_loaded / _process_count )*100
				loadbar.percent( _prec, "#{Math.round(_prec)} % " )
				return 

			_mig.on "writeData", ( maxSize, open, dynamoState )=>
				if maxSize > 0
					_prec = ( open / maxSize )*100
					#_mig.error( "prec", _prec, open , maxSize )
					writebar.percent( _prec, "#{Math.round(_prec)} % - TODO:#{open}/#{maxSize} CUNITS:#{dynamoState.capacityUnits}  " )
				else
					writebar.percent( 0, "0 % - TODO:#{open}/#{maxSize}   " )
				return

			_mig.start ( err, resp )=>
				if err
					console.log _err.stack
					cli.error( err )
				else
					cli.ok( JSON.stringify( resp, 1, 2 ) )
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