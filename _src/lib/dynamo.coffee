AWS = require('aws-sdk')
_ = require 'lodash'
async = require "async"

config = require( "./config" )

class Dynamo extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			accessKeyId: null
			secretAccessKey:  null
			region: "eu-west-1"
			apiVersion: "2012-08-10"
			tablenameDomains: null
			tablenameFiles: null
			ReturnConsumedCapacity: "TOTAL"

			pumps: 3
			batchsize: 10

	constructor: ->
		super
		@checked = false
		@c_domains = []

		@wait = 0
		@stateinfo = 
			iSuccess: 0
			#iTodo: 0
		@pumping = false

		@_configure()
		return

	_configure: =>
		@debug "check", @config

		if not @config.accessKeyId
			@_handleError( false, "EMISSINGAWSACCESSKEY" )
			return

		if not @config.secretAccessKey
			@_handleError( false, "EMISSINGAWSSECRET" )
			return

		@client = new AWS.DynamoDB( @config )

		@checked = true
		@emit "checked"
		return

	readDomain: ( domain, cb )=>
		if not @checked
			cb( @_handleError( true, "ECHECKINVALID" ) )
			return 

		params = 
			TableName: @config.tablenameDomains
			Key:
				domain: 
					S: domain

		@_getItem params, ( err, domainData )=>
			if err
				cb( err )
				return

			@debug "got domain", domainData

			if domainData?.domain?
				cb( null, domainData )
			else
				@_handleError( cb, "EDOMAINNOTFOUND" )
			return
		return

	migrate: ( domain, tuple, cb )=>
		_attr = @_attr2Dynamo( domain, tuple )
		@debug "migrate attrs `#{tuple.key}`", _attr, tuple
		@update _attr, ( err, result )=>
			if err
				cb( err )
				return
			cb( null, result )
			return
		return

	pump: ( domain, fnAddData, fnGetData )=>

		if @pumping
			return

		@pumping = true
		@emit "start.pump"
		@info "start pump"
		aPumps = for i in [ 1..@config.pumps ]
			@_pump( domain, fnAddData, fnGetData )

		async.parallel aPumps, ( err, res )=>
			@debug "async.parallel return"
			@pumping = false
			@info "stop pump"
			@emit "stop.pump"
			return
		return

	throttle: =>
		@wait += 50
		@debug( "throttle #{ @wait }" )
		return

	accelerate: =>
		if @wait - 50 <= 0
			@wait = 0
		else
			@wait -= 50
		@debug( "accelerate #{ @wait }" )
		return

	_pump: ( domain, fnAddData, fnGetData )=>
		return ( cba )=>
			@debug "_pump"
			_sett = fnGetData( @config.batchsize )
			
			if _sett.length > 0
				@writeBatch domain, _sett, fnAddData, ( err )=>
					@emit "data.pumped"
					@debug "writeBatch return wait #{@wait}"	
					_.delay( @_pump( domain, fnAddData, fnGetData ), @wait, cba )
					return
			else
				@debug "writeBatch empty"
				cba()
			return

	writeBatch: ( domain, datas, fnAddData, cb )=>
		_datas = []
		for tuple in datas
			if tuple.PutRequest?
				_datas.push( tuple )
			else
				_datas.push( @_attr2Dynamo( domain, tuple ) )

		@client.batchWriteItem @_createDynamoRequest( _datas ), ( err, res )=>
			@info "dynamo written", @wait, res?.ConsumedCapacity[ 0 ]?.CapacityUnits, res?.UnprocessedItems[ @config.tablenameFiles ]?.length
			if _.isNumber( res?.ConsumedCapacity?[ 0 ]?.CapacityUnits )
				@stateinfo.iSuccess += res?.ConsumedCapacity[ 0 ].CapacityUnits

			if not _.isEmpty( res?.UnprocessedItems )
				@throttle()
				_putR = res?.UnprocessedItems[ @config.tablenameFiles ]
				#@stateinfo.iTodo += _putR.length
				@debug "retry", _putR
				fnAddData( _putR )
				cb()
				return
			else
				@accelerate()

			if err? and err.statusCode isnt 200
				@error( err )
				cb()
				return
			cb()
			return
		return

	_createDynamoRequest: ( _datas )=>
		ret = 
			RequestItems: {}
			ReturnConsumedCapacity: "TOTAL"
			#ReturnItemCollectionMetrics: "SIZE"

		_dyn = []

		for data in _datas
			try
				if data.PutRequest?
					_dyn.push data
				else
					item = @_createItem( data )
					if item?
						_dyn.push 
							PutRequest:
								Item: item
			catch _err
				@error "error creating dynamo item", _err, JSON.stringify( data, 1,2 )

		ret.RequestItems[ @config.tablenameFiles ] = _dyn

		ret

	_createItem: ( data )=> 
		_item = 
			key: 
				S: data.key
			rev:
				S: data.rev
			fha: 
				S: data.fha
			url:
				S: data.url
			ttl:
				N: ( data.ttl or -1 ).toString()
			cty:
				S: data.cty
			acl:
				S: data.acl
			old:
				N: data.old.toString()
			crd:
				N: data.crd.toString()
			_u:
				N: data._u.toString()

		if data.cdi?.length
			_item.cdi =  
				S: data.cdi

		if data.hgt > 0
			_item.hgt =  
				N: data.hgt.toString()
		
		if data.wdt > 0
			_item.wdt =  
				N: data.wdt.toString()

		if _.isString(data.prp)
			_item.prp =  
				S: data.prp
		else if data.prp? and Object.keys( data.prp ).length
			_item.prp =  
				S: JSON.stringify( data.prp )

		if data.tgs?.length
			_item.tgs =  
				SS: data.tgs
		return _item

	update: ( data, cb )=>
		params = 
			TableName: @config.tablenameFiles
			Item: @_createItem( data )
			ReturnConsumedCapacity: @config.ReturnConsumedCapacity
			ReturnValues: "NONE"

		@_putItem( params, cb )
		return

	# Attribute Mapping
	attrMapping: 
		key: "key"
		rev: "revision"
		fha: "filehash"
		url: "url"
		ttl: "ttl"
		cty: "content_type"
		cdi: "content-disposition"
		acl: "acl"
		hgt: "height"
		wdt: "width"
		prp: "properties"
		tgs: "tags"
		old: "isRevision"
		crd: "created"
		_u:  "version"

	_attr2Dynamo: ( domain, inp )=>
		attrs = {}
		for _dyn, _api of @attrMapping
			attrs[ _dyn ] = if inp[ _api ]? then inp[ _api ] else null

		attrs.key = domain + ":" + attrs.key
		return attrs

	# Dynamo Helper Methods
	_getItem: ( params, cb )=>
		@debug "getItem", params
		@client.getItem params, @_processDynamoItemReturn( cb )
		return

	_putItem: ( params, cb )=>
		@debug "putItem", params
		@client.putItem params, @_processDynamoPutReturn( cb )
		return

	_processDynamoItemReturn: ( cb )=>
		return ( err, rawData )=>
			if err
				@_processDynamoError( cb, err )
				return

			@debug "_processDynamoItemReturn raw", rawData

			attrs = @_convertItem( rawData.Item )
			
			@debug "_processDynamoItemReturn", attrs
			cb( null, attrs )
			return

	_processDynamoPutReturn: ( cb )=>
		return ( err, rawData )=>
			if err
				@_processDynamoError( cb, err )
				return

			#@debug "_processDynamoPutReturn raw", rawData
			
			attrs = {}

			@debug "_processDynamoPutReturn", attrs
			cb( null, attrs )
			return

	_convertItem: ( raw )=>
		attrs = {}
		for _k, _v of raw
			_type = Object.keys( _v )[ 0 ]
			switch _type
				when "S" 
					attrs[ _k ] = _v[ _type ]
				when "SS" 
					attrs[ _k ] = _v[ _type ]
				when "N" 
					attrs[ _k ] = parseFloat( _v[ _type ] )

		return attrs

	_processDynamoError: ( cb, err )=>
		if err.code is "ResourceNotFoundException"
			@_handleError( cb, "EDYNAMOMISSINGTABLE", err )
			return

		cb( err )
		return

	ERRORS: =>
		return @extend {}, super, 
			"EDYNAMOMISSINGTABLE": [ 400, "The dynamo table does not exist. Please generate it!" ]
			"EDOMAINNOTFOUND": [ 404, "Domain not found. The given domain has not been found in target DynamoDB" ]
			"EMISSINGAWSACCESSKEY": [ 401, "Missing AWS Access Key. Please define the option `--awsaccesskey` or it's shortcut -a`" ]
			"EMISSINGAWSSECRET": [ 401, "Missing AWS Secret. Please define the option `-awssecret` or it's shortcut `-s`" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]

module.exports = new Dynamo()