apns = require 'apn'

settings = require '../../settings'

class PushServiceAPNS
    tokenFormat: /^[0-9a-f]{64}$/i
    validateToken: (token) ->
        if PushServiceAPNS::tokenFormat.test(token)
            return token.toLowerCase()

    constructor: (conf, @logger, tokenResolver) ->
        conf.errorCallback = (errCode, note) =>
            @logger?.error("APNS Error #{errCode} for subscriber #{note?.device?.subscriberId}")
        @driver = new apns.Connection(conf)

        @payloadFilter = conf.payloadFilter

        @feedback = new apns.Feedback(conf)
        # Handle Apple Feedbacks
        @feedback.on 'feedback', (feedbackData) =>
            feedbackData.forEach (item) =>
                tokenResolver 'apns', item.device.toString(), (subscriber) =>
                    subscriber?.get (info) ->
                        if info.updated < item.time
                            @logger?.warn("APNS Automatic unregistration for subscriber #{subscriber.id}")
                            subscriber.delete()


    push: (subscriber, subOptions, payload) ->
        subscriber.get (info) =>
            note = new apns.Notification()
            note.device = new apns.Device(info.token)
            note.device.subscriberId = subscriber.id # used for error logging
            if subOptions?.ignore_message isnt true and alert = payload.localizedMessage(info.lang)
                note.alert = alert
            note.badge = badge if not isNaN(badge = parseInt(info.badge) + 1)
            note.sound = payload.sound
            note.payload = payload.data
            note.expiry = Math.floor(Date.now() / 1000) + 24*3600;
            @driver.sendNotification note
            # On iOS we have to maintain the badge counter on the server
            # We check if settings of auto_increment_badge is set. If it is undefined, default is auto increment
            if settings?.options?.apns?.auto_increment_badge ? yes
                subscriber.incr 'badge'
            else
                subscriber.set 'badge', 0


exports.PushServiceAPNS = PushServiceAPNS