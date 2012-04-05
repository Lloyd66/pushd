event = require './event'
crypto = require 'crypto'

class Subscriber
    protocols: ['apns', 'c2dm', 'mpns']
    id_format:
        'apns': /^[0-9a-f]{64}$/i
        'c2dm': /^[a-zA-Z0-9_-]+$/
        'mpns': /^[a-z0-9]+$/ # TODO strictier format

    getInstanceFromRegId: (redis, proto, regid, cb) ->
        return until cb

        throw new Error("Invalid value for `proto'") if proto not in Subscriber::protocols
        throw new Error("Invalid value for `regid'") if not Subscriber::id_format[proto].test(regid)

        # Store regid in lowercase if format ignores case
        if Subscriber::id_format[proto].ignoreCase
            regid = regid.toLowerCase()

        redis.hget "regidmap", "#{proto}:#{regid}", (err, id) =>
            if id?
                # looks like this subscriber is already registered
                redis.exists "subscriber:#{id}", (err, exists) =>
                    if exists
                        cb(new Subscriber(redis, id))
                    else
                        # duh!? the global list reference an unexisting object, fix this inconsistency and return no subscriber
                        redis.hdel "regidmap", "#{proto}:#{regid}", =>
                            cb(null)
            else
                cb(null) # No subscriber for this regid

    create: (redis, fields, cb, tentatives=0) ->
        return until cb

        throw new Error("Missing mandatory `proto' field") if not fields?.proto?
        throw new Error("Missing mandatory `regid' field") if not fields?.regid?

        # Store regid in lowercase if format ignores case
        if Subscriber::id_format[fields.proto].ignoreCase
            fields.regid = fields.regid.toLowerCase()

        if tentatives > 10
            # exceeded the retry limit
            throw new Error "Can't find free uniq id"

        # verify if regid is already registered
        Subscriber::getInstanceFromRegId redis, fields.proto, fields.regid, (subscriber) =>
            if subscriber?
                # this subscriber is already registered
                delete fields.regid
                delete fields.proto
                subscriber.set fields, =>
                    cb(subscriber, created=false, tentatives)
            else
                # register the subscriber using a randomly generated id
                crypto.randomBytes 8, (ex, buf) =>
                    # generate a base64url random uniq id
                    id = buf.toString('base64').replace(/\=+$/, '').replace(/\//g, '_').replace(/\+/g, '-')
                    redis.watch "subscriber:#{id}", =>
                        redis.exists "subscriber:#{id}", (err, exists) =>
                            if exists
                                # already exists, rollback and retry with another id
                                redis.discard =>
                                    return Subscriber::create(redis, fields, cb, tentatives + 1)
                            else
                                fields.created = fields.updated = Math.round(new Date().getTime() / 1000)
                                redis.multi()
                                    # register subscriber regid to db id
                                    .hsetnx("regidmap", "#{fields.proto}:#{fields.regid}", id)
                                    # register subscriber to global list with protocol type stored as score
                                    .zadd("subscribers", @protocols.indexOf(fields.proto), id)
                                    # save fields
                                    .hmset("subscriber:#{id}", fields)
                                    .exec (err, results) =>
                                        if results is null
                                            # Transction discarded due to a parallel creation of the watched subscriber key
                                            # Try again in order to get the peer created subscriber
                                            return Subscriber::create(redis, fields, cb, tentatives + 1)
                                        if not results[0]
                                            # Unlikly race condition: another client registered the same regid at the same time
                                            # Rollback and retry the registration so we can return the peer subscriber id
                                            redis.del "subscriber:#{id}", =>
                                                return Subscriber::create(redis, fields, cb, tentatives + 1)
                                        else
                                            # done
                                            cb(new Subscriber(redis, id), created=true, tentatives)

    constructor: (@redis, @id) ->
        @info = null
        @key = "subscriber:#{@id}"

    delete: (cb) ->
        @redis.multi()
            # get subscriber's regid
            .hmget(@key, 'proto', 'regid')
            # gather subscriptions
            .zrange("subscriber:#{@id}:subs", 0, -1)
            .exec (err, results) =>
                [proto, regid] = results[0]
                events = results[1]
                multi = @redis.multi()
                    # remove from subscriber regid to id map
                    .hdel("regidmap", "#{proto}:#{regid}")
                    # remove from global subscriber list
                    .zrem("subscribers", @id)
                    # remove subscriber info hash
                    .del(@key)
                    # remove subscription list
                    .del("#{@key}:subs")

                # unsubscribe subscriber from all subscribed events
                multi.zrem "event:#{eventName}:devs", @id for eventName in events

                multi.exec (err, results) ->
                    @info = null # flush cache
                    cb(results[1] is 1) if cb # true if deleted, false if did exist

    get: (cb) ->
        return until cb
        # returned cached value or perform query
        if @info?
            cb(@info)
        else
            @redis.hgetall @key, (err, @info) =>
                if info?.updated? # subscriber exists
                    # transform numeric value to number type
                    for own key, value of info
                        num = parseInt(value)
                        @info[key] = if num + '' is value then num else value
                    cb(@info)
                else
                    cb(@info = null) # null if subscriber doesn't exist + flush cache

    set: (fieldsAndValues, cb) ->
        # TODO handle regid update needed for Android
        throw new Error("Can't modify `regid` field") if fieldsAndValues.regid?
        throw new Error("Can't modify `proto` field") if fieldsAndValues.proto?
        fieldsAndValues.updated = Math.round(new Date().getTime() / 1000)
        @redis.multi()
            # check subscriber existance
            .zscore("subscribers", @id)
            # edit fields
            .hmset(@key, fieldsAndValues)
            .exec (err, results) =>
                @info = null # flush cache
                if results[0]? # subscriber exists?
                    cb(true) if cb
                else
                    # remove edited fields
                    @redis.del @key, =>
                        cb(null) if cb # null if subscriber doesn't exist

    incr: (field, cb) ->
        @redis.multi()
            # check subscriber existance
            .zscore("subscribers", @id)
            # increment field
            .hincrby(@key, field, 1)
            .exec (err, results) =>
                if results[0]? # subscriber exists?
                    @info[field] = results[1] if @info? # update cache field
                    cb(results[1]) if cb
                else
                    @info = null # flush cache
                    cb(null) if cb # null if subscriber doesn't exist

    getSubscriptions: (cb) ->
        return unless cb
        @redis.multi()
            # check subscriber existance
            .zscore("subscribers", @id)
            # gather all subscriptions
            .zrange("#{@key}:subs", 0, -1, 'WITHSCORES')
            .exec (err, results) ->
                if results[0]? # subscriber exists?
                    subscriptions = []
                    eventsWithOptions = results[1]
                    for eventName, i in eventsWithOptions by 2
                        subscriptions.push
                            event: event.getEvent(@redis, null, eventName)
                            options: eventsWithOptions[i + 1]
                    cb(subscriptions)
                else
                    cb(null) # null if subscriber doesn't exist

    getSubscription: (event, cb) ->
        return unless cb
        @redis.multi()
            # check subscriber existance
            .zscore("subscribers", @id)
            # gather all subscriptions
            .zscore("#{@key}:subs", event.name)
            .exec (err, results) ->
                if results[0]? and results[1]? # subscriber and subscription exists?
                    cb
                        event: event
                        options: results[1]
                else
                    cb(null) # null if subscriber doesn't exist        

    addSubscription: (event, options, cb) ->
        @redis.multi()
            # check subscriber existance
            .zscore("subscribers", @id)
            # add event to subscriber's subscriptions list
            .zadd("#{@key}:subs", options, event.name)
            # add subscriber to event's subscribers list
            .zadd("#{event.key}:devs", options, @id)
            # set the event created field if not already there (event is lazily created on first subscription)
            .hsetnx(event.key, "created", Math.round(new Date().getTime() / 1000))
            # lazily add event to the global event list
            .sadd("events", event.name)
            .exec (err, results) =>
                if results[0]? # subscriber exists?
                    cb(results[1] is 1) if cb
                else
                    # Tried to add a sub on an unexisting subscriber, remove just added sub
                    # This is an exception so we don't first check subscriber existance before to add sub,
                    # but we manually rollback the subscription in case of error
                    @redis.multi()
                        .del("#{@key}:subs", event.name)
                        .srem(event.key, @id)
                        .exec()
                    cb(null) if cb # null if subscriber doesn't exist

    removeSubscription: (event, cb) ->
        @redis.multi()
            # check subscriber existance
            .zscore("subscribers", @id)
            # remove event from subscriber's subscriptions list
            .zrem("#{@key}:subs", event.name)
            # remove the subscriber from the event's subscribers list
            .zrem("#{event.key}:devs", @id)
            # check if the subscriber list still exist after previous srem
            .exists(event.key)
            .exec (err, results) =>
                if results[3] is 0
                    # The event subscriber list is now empty, clean it
                    event.delete() # TOFIX possible race condition

                if results[0]? # subscriber exists?
                    cb(results[1] is 1) if cb # true if removed, false if wasn't subscribed
                else
                    cb(null) if cb # null if subscriber doesn't exist

exports.createSubscriber = Subscriber::create
exports.protocols = Subscriber::protocols

exports.getSubscriber = (redis, id) ->
    return new Subscriber(redis, id)

exports.getSubscriberFromRegId = (redis, proto, regid, cb) ->
    return Subscriber::getInstanceFromRegId(redis, proto, regid, cb)