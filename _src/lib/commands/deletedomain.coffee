cli = require('cli').enable('help', 'status', "version")

config = require( "../../lib/config" )

cli.setApp( "Media-API Domain delete" )

multimeter = require( "multimeter" )
multi = multimeter(process);
charm = multi.charm;
charm.on('^C', process.exit);
charm.reset();

exports.run = ->
	cli.parse(
		domain: [ "d", "Domain to delete", "string" ]
		endpoint: [ "e", "The Media-API endpoint", "string" ]
		accesskey: [ "da", "AWS DynamoDB Access Key", "string" ]
		secret:  [ "ds", "AWS DynamoDB Access Key", "string" ]
		region:  [ "dr", "AWS DynamoDB Region", "string", "" ]
	)
	cli.main ( args, options )->

		process.stdout.write '\u001B[2J\u001B[0;0f'

		_cnf =
			deletedomain:
				domain: options.domain
				mediaapiendpoint: options.endpoint or "http://192.168.1.8:8005/mediaapi/"

		_cnf.dynamo.accessKeyId = options.dynaccesskey if options.dynaccesskey?.length
		_cnf.dynamo.secretAccessKey = options.dynsecret if options.dynsecret?.length
		_cnf.dynamo.region = options.dynregion if options.dynregion?.length
		
		config.init( _cnf )

		DeleteDomain = require( "../../deletedomain" )

		try
			_deld = new DeleteDomain( _cnf )

			_deld.start ( err, resp )=>
				if err
					console.log _err?.stack
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
	return