AWS = require('aws-sdk')
_ = require 'lodash'

config = require( "./config" )

class SimpleDB extends require( "mpbasic" )( config )
	defaults: =>
		return @extend {}, super, 
			accessKeyId: null
			secretAccessKey:  null
			region: "eu-west-1"
			apiVersion: "2009-04-15"

	constructor: ->
		super
		@checked = false
		@c_domains = []

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

		@client = new AWS.SimpleDB( @config )

		@checked = true
		@emit "checked"
		return

	readDomains: ( cb )=>
		if not @checked
			cb( @_handleError( true, "ECHECKINVALID" ) )
			return 

		if not @c_domains?.length
			@_readDomains( cb )
			return

		if @_running_readDomains
			@once "readDomains", =>
				cb( null, @c_domains )
				return
			return

		cb( null, @c_domains )
		return 

	_readDomains: ( cb, nextToken )=>
		@debug "_readDomains"
		@debug "_readDomains - nextToken", nextToken if nextToken?

		@_running_readDomains = true
		_params = 
			MaxNumberOfDomains: 100
		_params.NextToken = nextToken if nextToken?

		@client.listDomains _params, ( err, raw )=>
			if err
				cb( err )
				@_running_readDomains = false
				return 
			@c_domains = _.union( @c_domains, raw.DomainNames )
			# got a nextToken so read the next data and append the domains.
			if raw.NextToken?.length
				@_readDomains( cb, raw.NextToken )
			else
				@_running_readDomains = false
				@emit "readDomains"
				@debug "found #{ @c_domains.length } domains"
				cb( null, @c_domains )
			return
		return 

	domainMetadata: ( domain, cb )=>
		if not @checked
			cb( @_handleError( true, "ECHECKINVALID" ) )
			return 

		_params = 
			DomainName: domain

		@client.domainMetadata _params, ( err, raw )=>
			if err
				cb( err )
				return
			@debug "meta", raw
			cb( null, raw )
			return
		return

	_processData: ( data )=>
		_ret = []
		
		for tuple in data
			attrs = 
				id: tuple.Name
			for attr in tuple.Attributes
				attrs[ attr.Name ] = attr.Value
			_ret.push attrs
		
		#@debug "_processData", data.length
		return _ret

	loadData: ( domain, nextToken, cb )=>
		if not @checked
			cb( @_handleError( true, "ECHECKINVALID" ) )
			return 

		_params = 
			SelectExpression: "select * from `#{domain}` where modified > '0' order by modified desc"
			ConsistentRead: true
		_params.NextToken = nextToken if nextToken?

		@client.select _params, ( err, raw )=>
			@debug "select: items received: #{raw.Items?.length or "-"}"
			if err
				cb( err )
				return
			
			if raw.NextToken?.length
				cb( null, @_processData( raw.Items ), raw.NextToken )
			else
				cb( null, @_processData( raw.Items ) )
			return
		return 

	ERRORS: =>
		return @extend {}, super, 
			"EMISSINGAWSACCESSKEY": [ 401, "Missing AWS Access Key. Please define the option `--awsaccesskey` or it's shortcut -a`" ]
			"EMISSINGAWSSECRET": [ 401, "Missing AWS Secret. Please define the option `-awssecret` or it's shortcut `-s`" ]
			"ECHECKINVALID": [ 500, "Its not possible to run start if the check has been failed" ]

module.exports = new SimpleDB()