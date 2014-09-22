path = require('path')
crypto = require( "crypto" )
urlParser = require( "url" )

low = require('lowdb')
_ = require('lodash')
request = require( "request" )
TqwClient = require( "task-queue-worker" ).Client

config = require( "./config" )
dynamo = require( "./dynamo" )


module.exports = class DeleteDomain extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			domain: null
			blocksize: 100
			mediaapiendpoint: "http://192.168.1.8:8005/mediaapi/"
			queuename: "ma-deletedomain"

	constructor: ->
		super
		@checked = false

		@start = @_waitUntil( @_start, "checked" )
		
		@_configure()
		return

	_signUrl: ( url, json, asObj = false )=>
		_sign = encodeURIComponent( @_createSign( url, json ) )
		url += ( if "?" in url then "&" else "?" ) + "signature=#{ _sign }"
		if asObj
			_url = ( @config.mediaapiendpoint ).replace( "http://", "" ).replace( "https://", "" )
			[ _h, _p ] = _url.split( ":" )
			
			host: _h
			port: _p or 80
			path: url.replace( _Cnf.host, "" )
			method: 'POST'
		else
			url

	_createSign: ( url, json )=>
		_str = url + if json then JSON.stringify( json ) else ""
		shasum = crypto.createHmac('sha1', @shared.targetdomain?.secretAccessKey )
		shasum.update( _str )
		#console.log "2sign", _str
		return shasum.digest( "base64" )

	_configure: =>
		@debug "check"

		if not @config.domain
			@_handleError( false, "EMISSINGDOMAIN" )
			return
		
		@data = low( @config.domain )
		#@data.autoSave = false
		_path = path.resolve(__dirname + "/../" ) + "/deletedomain.json"
		low.path = _path
		try
			low.load()
		@debug "save data to received `#{_path}`"

		@twc = new TqwClient( queue: @config.queuename )
		@checked = true
		@emit "checked"
		return

	middleware: =>
		_error = false
		[ fns..., cb ] = arguments
		
		if not _.isFunction( fns[ 0 ] )
			data = fns.splice( 0 , 1 )[ 0 ]
		else
			data = {}
		
		_errorFn = ( error )->
			fns = []
			_error = true
			cb( error, data )
			return
	
		run = ->
			if not fns.length
				cb( null, data )
			else
				fn = fns.splice( 0 , 1 )[ 0 ]
				
				fn( data, ->
					run() if not _error
					return
				, _errorFn, fns )
		run()
	
		return

	onEnd: ( cb )=>
		return ( err )=>
			if err
				cb( err )
				return
			@info "all data deleted"
			console.timeEnd( "duration" )
			
			cb( null, true )
			return

	_start: ( cb )=>
		console.time( "duration" )
		@shared = {}
		fnEnd = @onEnd( cb )
		@middleware @shared, @checkDomain, @getAllElementsOfDomain, ( err, shared )=>
			if err
				fnEnd( err )
				return
			
			fnEnd()
			return
		return

	checkDomain: ( shared, next, error, fns )=>
		if not @checked
			error( @_handleError( true, "ECHECKINVALID" ) )
			return 

		@debug "checkTargetDomain"

		# check if the given Domain exists
		dynamo.readDomain @config.domain, ( err, meta )=>
			if err
				error( err )
				return
			shared.targetdomain = meta
			@debug "\n\nDomain Target Infos:", meta
			next()
			return
		return

	getAllElementsOfDomain: ( shared, next, error, fns )=>
		_url = ( @config.mediaapiendpoint + @config.domain + "/search/t/u?limit=1000" )
		_opt = 
			url: @_signUrl( _url )
			json: true

		shared.keys2del = []
		request _opt, ( err, resp, body )=>
			if err
				error( err )
				return
			if body.rows?.length
				for ele in  body.rows
					shared.keys2del.push [ ele.key, ele.revision ]
				@info "currently #{shared.keys2del.length} to delete"
				fns.push @queueDeleteItems 
				next()
			else
				@info "No more elements received ..."
				next()
			return

		return

	queueDeleteItems: ( shared, next, error, fns )=>
		for ele in shared.keys2del
			fns.push @queueDeleteItem( ele[ 0 ], ele[ 1 ] )
		next() 
		return

	queueDeleteItem: ( key, revision )=>
		return ( shared, next, error, fns )=>
			_url = ( @config.mediaapiendpoint + @config.domain + "/#{key}?revision=#{revision}" )
			@debug "delete: #{_url}" 
			@twc.send url: @_signUrl( _url ), method: "DELETE", timeout: 100, ( err, resp )=>
				@debug "delete resp: #{resp}" 
				if err
					error( err )
					return
				next()
				return
			return

	ERRORS: =>
		return @extend {}, super, 
			"EMISSINGDOMAIN": [ 401, "Missing Domain. Please define the option `--domain` or it's shortcut -d`" ]
			"EDOMAINNOTFOUND": [ 404, "Domain not found. The given domain has not been found in source SimpleDB" ]