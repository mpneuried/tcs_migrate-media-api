config = require( "./config" )
simpledb = require( "./simpledb" )
dynamo = require( "./dynamo" )
low = require('lowdb')
_ = require('lodash')
path = require('path')

module.exports = class Migration extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			domain: null
			batchsize: 5

	constructor: ->
		super
		@checked = false

		@on "migrate", dynamo.migrate
		dynamo.on "data.pumped", =>
			@info "dynamo state - done: #{dynamo.stateinfo.iSuccess} todo: #{@shared.sourcedata.length}"
			return

		@_configure()
		return

	_configure: =>
		@debug "check"

		if not @config.domain
			@_handleError( false, "EMISSINGDOMAIN" )
			return
		
		@data = low( @config.domain )
		@data.autoSave = false
		_path = path.resolve(__dirname + "/../" ) + "/db.json"
		low.path = _path
		low.load()
		@debug "save data to received `#{_path}`"

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


	start: ( cb )=>

		@shared = {
			sourcedata: []
		}

		@middleware @shared, @checkSourceDomain, @checkTargetDomain, @loadSourceDomainMeta, @loadData(), ( err, shared )=>
			if err
				cb( err )
				return
			if dynamo.pumping
				dynamo.once "stop.pump", =>
					@info "all data migrated"
					cb( null, true )
			else
				@info "all data migrated"
				cb( null, true )
			return
		return

	loadData: ( _nextToken )=>
		return ( shared, next, error, fns )=>

			if not low( "_domain_state_").get( @config.domain )? 
				low( "_domain_state_").insert( { id: @config.domain, _modified: 0 } )

			@emit "loadAllDataStart", shared.metaSource.ItemCount

			simpledb.loadData @config.domain, _nextToken, ( err, data, nextToken )=>
				if err
					error( err )
					return
				
				@_collectLoadData( data, shared, fns )
				
				# append the loading of next data
				if nextToken?
					fns.push( @loadData( nextToken ) )

				next()
				return

			return

	pullSetting: ( batchsize )=>
		@debug "pull data", @shared.sourcedata.length
		return @shared.sourcedata.splice( 0, batchsize )

	pushSetting: ( datas )=>
		@shared.sourcedata = @shared.sourcedata.concat( datas )
		return

	_collectLoadData: ( data, shared, fns )=>
		@emit "loadAllDataReceived", data.length
		for tuple in data
			shared.sourcedata.push @_convertSimpleDB2DynamoData( tuple )
			#@data.insert( tuple )
			_modified = tuple.modified

		if not dynamo.pumping
			dynamo.pump( @config.domain, @pushSetting, @pullSetting )

		low( "_domain_state_").update( @config.domain, { id: @config.domain, _modified: _modified } )
		low.save()
		return

	_write2Dynamo: ( data )=>
		return ( shared, next, error, fns )=>
			dynamo.migrate @config.domain, data, ( err, data )=>
				if err
					low( "#{ @config.domain}_errors" ).insert( error: err, data: data )
					return
				next()
				return
			return

	_convertSimpleDB2DynamoData: ( data )=>
		attr = {}
		#console.log data
		[ _k, _rev ] = data.id.split( "_" ) 
		_modified = parseInt( data.modified, 10 ) * 1000
		_created = parseInt( data.created, 10 )

		attr.key = data.key
		attr.revision = @_createRevision( _modified )

		attr.filehash = data.filehash
		attr.url = data.asseturl

		attr.ttl = parseInt( data.ttl, 10 )
		attr.content_type = data.content_type or "application/octet-stream"
		attr[ "content-disposition" ] = data.content_disposition
		attr.acl = data.acl

		attr.width = parseInt( data.width, 10 )
		attr.height = parseInt( data.height, 10 )
		
		# TODO fill properties
		attr.properties = {}

		attr.tags = []

		attr.isRevision = if _rev? then 1 else 0
		
		attr.created = _created
		attr.version = parseInt( _.last( data.revision.split( "-" ) ), 10 )
		return attr

	_createRevision: ( date )=>
		if date?
			return date.toString(36)
		else
			return Date.now().toString(36)

	_now: ( date )=>
		if date?
			return Math.round( date/1000 )
		else
			return Math.round( Date.now()/1000 )

	checkSourceDomain: ( shared, next, error, fns )=>
		if not @checked
			error( @_handleError( true, "ECHECKINVALID" ) )
			return 

		@debug "checkSourceDomain"

		# check if the given Domain exists
		simpledb.readDomains ( err, domains )=>
			if err
				error( err )
				return
			if @config.domain not in domains
				error( @_handleError( true, "EDOMAINNOTFOUND" ) )
				return

			next()
			return
		return

	loadSourceDomainMeta: ( shared, next, error, fns )=>
		if not @checked
			error( @_handleError( true, "ECHECKINVALID" ) )
			return 

		simpledb.domainMetadata @config.domain, ( err, meta )=>
			if err
				error( err )
				return
			shared.metaSource = meta
			@info "\n\nDomain Target Infos:\n- Items: #{meta.ItemCount}\n- Attributes: #{meta.AttributeNameCount}"
			next()
			return
		return

	checkTargetDomain: ( shared, next, error, fns )=>
		if not @checked
			error( @_handleError( true, "ECHECKINVALID" ) )
			return 

		@debug "checkTargetDomain"

		# check if the given Domain exists
		dynamo.readDomain @config.domain, ( err, meta )=>
			if err
				error( err )
				return
			@debug "\n\nDomain Target Infos:", meta
			next()
			return
		return


	ERRORS: =>
		return @extend {}, super, 
			"EMISSINGDOMAIN": [ 401, "Missing Domain. Please define the option `--domain` or it's shortcut -d`" ]
			"EDOMAINNOTFOUND": [ 404, "Domain not found. The given domain has not been found in source SimpleDB" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]