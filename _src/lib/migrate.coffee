path = require('path')

low = require('lowdb')
_ = require('lodash')
entities = require("entities")

config = require( "./config" )
simpledb = require( "./simpledb" )
dynamo = require( "./dynamo" )

module.exports = class Migration extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			domain: null
			batchsize: 5

	constructor: ->
		super
		@checked = false
		@importedRevisions = []
		@duplicateRevisions = []

		@on "migrate", dynamo.migrate
		@_maxSourceLength = 0
		dynamo.on "data.pumped", @_updateWriteCacheBar
		@on "data.loaded", @_updateWriteCacheBar

		dynamo.on "data.error", ( err, data )=>
			low( "errors_#{@config.domain}").insert( { err: err, data: data } )
			#low.save()
			return

		@_configure()
		return

	_configure: =>
		@debug "check"

		if not @config.domain
			@_handleError( false, "EMISSINGDOMAIN" )
			return
		
		@data = low( @config.domain )
		#@data.autoSave = false
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
		console.time( "duration" )
		@shared = {
			sourcedata: []
		}
		fnEnd = @onEnd( cb )
		@middleware @shared, @checkSourceDomain, @checkTargetDomain, @loadSourceDomainMeta, @loadData(), ( err, shared )=>
			if err
				fnEnd( err )
				return
			if dynamo.pumping
				dynamo.once "stop.pump", fnEnd
			else
				fnEnd()
			return
		return

	onEnd: ( cb )=>
		return ( err )=>
			if err
				cb( err )
				return
			@info "all data migrated in:"
			console.timeEnd( "duration" )
			@info "duplicateRevisions", @duplicateRevisions
			cb( null, dynamo: dynamo.stateinfo, migration: duplicates: @duplicateRevisions.length )
			return

	loadData: ( _nextToken )=>
		return ( shared, next, error, fns )=>
			@emit "data.loaded"
			if not low( "_domain_state_").get( @config.domain )? 
				low( "_domain_state_").insert( { id: @config.domain, _modified: 0 } )

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
		for _tuple in data
			tuple = @_convertSimpleDB2DynamoData( _tuple )
			_revID = tuple.key + "_" + tuple.revision
			#console.log _revID
			if _revID in @importedRevisions
				@duplicateRevisions.push( _revID )
			else
				@importedRevisions.push( _revID )
				shared.sourcedata.push tuple
				#@data.insert( tuple )
				_modified = tuple.modified

		if not dynamo.pumping
			dynamo.pump( shared.targetdomain, @pushSetting, @pullSetting )

		low( "_domain_state_").update( @config.domain, { id: @config.domain, _modified: _modified } )
		return

	_write2Dynamo: ( data )=>
		return ( shared, next, error, fns )=>
			dynamo.migrate shared.targetdomain, data, ( err, data )=>
				if err
					low( "#{ @config.domain}_errors" ).insert( error: err, data: data )
					return
				next()
				return
			return

	_convertSimpleDB2DynamoData: ( data )=>
		attr = {}
		#console.log data
		[ _k, _krev ] = data.id.split( "_" )
		[ _rev, _version ] = data.revision.split( "-" )
		_modified = parseInt( _rev, 10 ) * 1000
		_created = parseInt( data.created, 10 )

		attr.key = data.key
		attr.revision = @_createRevision( _modified )

		attr.filehash = data.filehash
		attr.url = data.asseturl

		attr.ttl = parseInt( data.ttl, 10 )
		attr.content_type = data.content_type or "application/octet-stream"
		attr[ "content_disposition" ] = data.content_disposition
		attr[ "content_length" ] = parseInt( data.content_length, 10 )
		attr.acl = data.acl

		attr.width = parseInt( data.width, 10 )
		attr.height = parseInt( data.height, 10 )
		
		attr.properties = {}
		# special Case use the field `tcs_TAGS` as author
		if data.tcs_TAGS?
			attr.properties.author = entities.decodeHTML data.tcs_TAGS

		attr.tags = []

		attr.isRevision = if _krev? then 1 else 0
		
		attr.created = _created
		attr.version = parseInt( _version, 10 )
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

	_updateWriteCacheBar: =>
		@_maxSourceLength = @shared.sourcedata.length if @shared.sourcedata.length > @_maxSourceLength
		@emit "writeData", @_maxSourceLength, @shared.sourcedata.length, dynamo.stateinfo
		#@info "dynamo state - done: #{} todo: #{}"
		return

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
			@emit "loadAllDataStart", shared.metaSource.ItemCount
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
			shared.targetdomain = meta
			@debug "\n\nDomain Target Infos:", meta
			next()
			return
		return


	ERRORS: =>
		return @extend {}, super, 
			"EMISSINGDOMAIN": [ 401, "Missing Domain. Please define the option `--domain` or it's shortcut -d`" ]
			"EDOMAINNOTFOUND": [ 404, "Domain not found. The given domain has not been found in source SimpleDB" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]