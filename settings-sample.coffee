exports.server =
    tcp_port: 80
    udp_port: 80
    access_log: yes
    acl:
        # restrict publish access to private networks
        publish: ['127.0.0.1', '10.0.0.0/8', '172.16.0.0/12', '192.168.0.0/16']

<<<<<<< HEAD
exports.redis =
  database: 0

exports.apns =
=======
exports['event-source'] =
    enabled: yes

exports['apns'] =
>>>>>>> upstream/master
    enabled: yes
    class: require('./lib/pushservices/apns').PushServiceAPNS
    # Convert cert.cer and key.p12 using:
    # $ openssl x509 -in cert.cer -inform DER -outform PEM -out apns-cert.pem
    # $ openssl pkcs12 -in key.p12 -out apns-key.pem -nodes
    cert: 'apns-cert.pem'
    key: 'apns-key.pem'
    cacheLength: 100
    # uncommant for dev env
    #gateway: 'gateway.sandbox.push.apple.com'
    #address: 'feedback.sandbox.push.apple.com'

# # Uncomment to use same host for prod and dev
# exports['apns-dev'] =
#     enabled: yes
#     class: require('./lib/pushservices/apns').PushServiceAPNS
#     # Your dev certificats
#     cert: 'apns-cert.pem'
#     key: 'apns-key.pem'
#     cacheLength: 100
#     gateway: 'gateway.sandbox.push.apple.com'

exports['gcm'] =
    enabled: yes
    class: require('./lib/pushservices/gcm').PushServiceGCM
    key: 'GCM API KEY HERE'

exports['mpns'] =
    enabled: no
    class: require('./lib/pushservices/mpns').PushServiceMPNS
    endpoint: 'http://sn1.notify.live.net/throttledthirdparty/01.00/YOUR_ENDPOINT_HERE'

# # Legacy Android Push Service
# exports['c2dm'] =
#     enabled: yes
#     class: require('./lib/pushservices/c2dm').PushServiceC2DM
#     # App credentials
#     user: 'app-owner@gmail.com'
#     password: 'something complicated and secret'
#     source: 'com.yourcompany.app-name'
#     # How many concurrent requests to perform
#     concurrency: 10

exports['http'] =
    enabled: yes
    class: require('./lib/pushservices/http').PushServiceHTTP

exports['mpns-toast'] =
    enabled: yes
    class: require('./lib/pushservices/mpns').PushServiceMPNS
    type: 'toast'
    # Used for WP7.5+ to handle deep linking
    paramTemplate: '/Page.xaml?object=${data.object_id}'

exports['mpns-tile'] =
    enabled: yes
    class: require('./lib/pushservices/mpns').PushServiceMPNS
    type: 'tile'
    # Mapping defines where - in the payload - to get the value of each required properties
    tileMapping:
        id: "${event.name}"
        title: "${data.title}"
        backgroundImage: "${data.background_image_url}"
        backBackgroundImage: "#005e8a"
        backTitle: "${data.back_title}"
        backContent: "${data.message}"
        # param for WP8 flip tile (sent when subscriber declare a minimum OS version of 8.0)
        smallBackgroundImage: "${data.small_background_image_url}"
        wideBackgroundImage: "${data.wide_background_image_url}"
        wideBackContent: "${data.message}"
        wideBackBackgroundImage: "#005e8a"
