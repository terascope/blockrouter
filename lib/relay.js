'use strict';

var net = require('net');
var fs = require('fs');

module.exports = function(context, stats_name) {
    var statsd = context.foundation.getConnection({type: 'statsd', cached: true}).client;
    var config = context.sysconfig;
    var logger = context.logger;

    function start(targets, targetQueues, target) {
        if (targetQueues[target].length > 0) {
            if (targets[target].length === 0) {
                //logger.error("No working relays for target: " + target);
                // We need to wait until there is a valid relay to use
                setTimeout(function() { start(targets, targetQueues, target); }, 50);
                return;
            }

            // Simple round robin balancing
            var relay = targets[target].shift();

            if (relay && relay.readyForData) {

                var filename = targetQueues[target].shift();
                if (fs.existsSync(filename)) {
                    relay.readyForData = false;
                    // There is a potential race condition here between the exists
                    // check and the read when multiple workers exist. Would result
                    // in data duplication
                    try {
                        relay.write(fs.readFileSync(filename));
                        fs.unlinkSync(filename);
                    }
                    catch(err) {
                        // What is relavent to catch here?
                    }
                }

                targets[target].push(relay);
                process.nextTick(function() {
                    start(targets, targetQueues, target);
                });
            }
            else {
                // Bad relay. Try again to select another.
                start(targets, targetQueues, target);
            }
        }
        else {
            setTimeout(function() {
//logger.info("Startni queue " + target)
                start(targets, targetQueues, target);
            }, 500);
        }
    }

    function create(targets, target, config) {
        var relay = new net.Socket();

        relay.connect(config.port, config.host, function() {
            logger.info('Relay - connection to: ' + config.host + ':' + config.port);
        });

        relay.on('connect', function() {
            relay.readyForData = true;
            targets[target].push(relay);
        });

        relay.on('drain', function() {
            relay.readyForData = true;
            targets[target].push(relay);
        });

        // Add a 'data' event handler for the client socket
        // data is what the server sent to this socket
        relay.on('data', function(data) {
            logger.info('Relay - service response: ' + data);
        });

        relay.on('error', function(err) {
            //logger.error("Error from relay " + config.host + ":" + config.port + " - " + err);
        });

        relay.on('close', function(err) {
            if (! relay.timeoutSet) {
                relay.readyForData = false;
                relay.timeoutSet = true;
                //logger.error("Relay - error sending notification to " + config.collector.relay.host + ":" + config.collector.relay.port)
                setTimeout(function() {
                    logger.error("Relay lost connection: " + config.host + ":" + config.port + ". Preparing for reconnect.")
                    if (relay) relay.destroy();
                    relay = null;
                    create(targets, target, config);
                }, 5000);
            }
        });
    }

    return {
        create: create,
        start: start
    };
}

