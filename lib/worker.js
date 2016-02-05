'use strict';

var net = require('net');
var tls = require('tls');
var fs = require('fs');
var mkdirp = require('mkdirp');
var getSize = require('get-folder-size');

var _ = require('lodash');

var port = process.env.PORT || 60190;

var service_name = "VerServer";
var service_version = "1.0";

module.exports = function(context) {
    var logger = context.logger;
    var config = context.sysconfig;
    var cluster = context.cluster;

    // For memory debugging
    if (config.terafoundation.environment === 'production') process.chdir('/app/logs');

    var stats = require('./stats')(context, 'blockrouter');

    process.on('message', function(msg) {
        // TODO: this is here so that we can eventually do a graceful exit.
        // For now it just ends the process.
        if (msg.cmd && msg.cmd == 'stop') {
            logger.info("Worker " + cluster.worker.id + " stopping.");

            process.exit();
        }
    });

    var connections = [];

    var waitingSockets = [];

    var activeTasks = 0;

    // Service can listen for either TLS or plain sockets
    if (config.blockrouter.ssl && config.blockrouter.ssl.enabled) {
        var options = {
            key: fs.readFileSync(config.blockrouter.ssl.key),
            cert: fs.readFileSync(config.blockrouter.ssl.cert),
        };

        // Start a TLS / TCP Server
        tls.createServer(options, socketHandler).listen(port, function() {
            logger.info("Service ready. Listening for TLS connections.")
        });
    }
    else {
        // Start a TCP Server
        net.createServer(socketHandler).listen(port, function() {
            logger.info("Service ready.")
        });
    }

    var targets = {};
    var targetQueues = {};

    setupTargets(config.blockrouter.targets);

    // If the queue is full we can't accept new data.
    var queueFull = true; // Blocked to start
    checkQueueSize();

    // Also need check periodically.
    var queueWatcher = setInterval(checkQueueSize, 5000);

    var spoolBuffer = "";

    var select = 0;
    function socketHandler(socket) {
        socket.setEncoding('utf8');

        socket.remainder = "";

        // Identify this client
        socket.name = socket.remoteAddress + ":" + socket.remotePort;
        connections.push(socket);

        welcome(socket);

        // If there aren't any available slots. pause.
        if (config.blockrouter.backpressure && activeTasks > config.blockrouter.max_tasks) {
            logger.info("Pausing new connection. Currently active connections " + connections.length);
            socket.pause();
            waitingSockets.push(socket);
            stats.count('paused', 1);
        }

        socket.on('error', function(error) {
            stats.count('error', 1);
            processor.handleError(error);
        });

        // Handle incoming data from clients.
        socket.on('data', function (data) {
            activeTasks++;
            if (activeTasks > config.blockrouter.max_tasks || queueFull) {
                if (config.blockrouter.backpressure) {
                    socket.pause();
                    waitingSockets.push(socket);
                    stats.count('paused', 1);
                }
                else {
                    // If we're defined to not provide backpressure then we
                    // have no choice but to drop the data.
                    stats.count('dropping', 1);
                    activeTasks--;
                    return;
                }
            }
            else {
                stats.count('chunk', 1);
            }

            spoolData(socket, data, function() {
                activeTasks--;

                if (activeTasks <= config.blockrouter.max_tasks && ! queueFull) {

                    if (waitingSockets.length > 0) {
                        var task = waitingSockets.shift();
                        task.resume();

                        stats.count('resumed', 1);
                    }
                }
            });
        });

        // Remove the client from the list when it leaves
        socket.on('end', function () {
            connections.splice(connections.indexOf(socket), 1);
        });
    }

    function setupTargets(targetConfig) {
        _.forEach(targetConfig, function(configs, target) {
            targets[target] = []; // This is a list of the socket objects
            targetQueues[target] = []; // This is a query of data blocks that need to be relayed

            // Setup on disk queues
            mkdirp.sync(config.blockrouter.disk_queue.path + "/" + target);

            configs.forEach(function(config) {
                createRelay(target, config);
            });

            initializeQueues();

            startRelayToTarget(target);
        });
    }


    function spoolData(socket, data, cb) {
        data = socket.remainder + data;

        var lines = data.split('\n');
        // We assume the list line is partial so save it for combination
        // with the next chunk of data.
        socket.remainder = lines.pop();

        spoolBuffer += lines.join('\n');

        if (spoolBuffer.length > config.blockrouter.blocksize) {
            flushSpoolBuffer(cb);
        }
        else {
            cb();
        }
    }

    setInterval(function() {
        logger.info("Spool buffer " + spoolBuffer.length);
        if (spoolBuffer.length > 0) flushSpoolBuffer(function(){});
    }, 5000);

    function flushSpoolBuffer(cb) {
        var dataToSend = spoolBuffer;
        spoolBuffer = ""
        queueData(dataToSend, cb);
    }

    function queueData(data, cb) {
        _.forOwn(targets, function(relays, target) {
            var filename = config.blockrouter.disk_queue.path + "/" + target + "/" + Date.now();
            targetQueues[target].push(filename);
            fs.writeFileSync(filename, data);
        });

        cb();
    }

    function startRelayToTarget(target) {
        if (targetQueues[target].length > 0) {
//logger.info("Sending data to Target " + target + " Queue size " + targetQueues[target].length);
            if (targets[target].length === 0) {
                //logger.error("No working relays for target: " + target);
                // We need to wait until there is a valid relay to use
                setTimeout(function() { startRelayToTarget(target); }, 50);
                return;
            }

            // Simple round robin balancing
            var relay = targets[target].shift();

            if (relay && relay.readyForData) {

                var filename = targetQueues[target].shift();
                if (fs.existsSync(filename)) {
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
                    startRelayToTarget(target);
                });
            }
            else {
                // Bad relay. Try again to select another.
                startRelayToTarget(target);
            }
        }
        else {
            setTimeout(function() {
                startRelayToTarget(target);
            }, 500);
        }
    }

    function createRelay(target, config) {
        var relay = new net.Socket();
//        relay.setNoDelay(true);

        relay.connect(config.port, config.host, function() {
            logger.info('Relay - connection to: ' + config.host + ':' + config.port);
        });

        relay.on('connect', function() {
            relay.readyForData = true;
            relay.pendingWrites = 0;
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
                    logger.error("Relay closed connection: " + config.host + ":" + config.port + ". Preparing for reconnect.")
                    if (relay) relay.destroy();
                    relay = null;
                    createRelay(target, config);
                }, 5000);
            }
        });

        targets[target].push(relay);
    }

    function initializeQueues() {
        // On startup we need to pickup and reprocess any data still in the file system.
        _.forOwn(targets, function(relays, target) {
            var path = config.blockrouter.disk_queue.path + "/" + target;
            var files = fs.readdirSync(path);
            files.forEach(function(file) {
                var filename = config.blockrouter.disk_queue.path + "/" + target + "/" + file;
                targetQueues[target].push(filename);
            });
        });
    }

    function checkQueueSize() {
        // There's going to be a latency in this check so if data is coming in really
        // fast the queue size needs to be set carefully.
        getSize(config.blockrouter.disk_queue.path, function(err, size) {
            if (size > config.blockrouter.disk_queue.size) {
                logger.error("Data receiver blocked. Cache disk usage too high: " + size + " bytes");
                queueFull = true;
            }
            else {
                queueFull = false;
                // TODO: does this belong here?
                // It could result in more than one socket being resumed in some
                // scenarios.
                if (waitingSockets.length > 0) {
                    var task = waitingSockets.shift();
                    task.resume();
                }
            }
        });
    }

    function welcome(socket) {
        socket.write(service_name + " " + service_version + " ready, fire away " + socket.remoteAddress + ".\n");
        logger.info("Connection from " + socket.name);
    }
}
