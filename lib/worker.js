'use strict';

var net = require('net');
var tls = require('tls');
var fs = require('fs');
var mkdirp = require('mkdirp');

var _ = require('lodash');

var port = process.env.PORT || 60180;

var service_name = "BlockRouter";
var service_version = "1.0";

module.exports = function(context) {
    var logger = context.logger;
    var config = context.sysconfig;
    var cluster = context.cluster;

    // For memory debugging
    if (config.terafoundation.environment === 'production') process.chdir('/app/logs');

    var stats = require('./stats')(context, 'blockrouter');
    var relay = require('./relay')(context);
    var queue = require('./disk_queue')(context);

    process.on('message', function(msg) {
        // TODO: this is here so that we can eventually do a graceful exit.
        // For now it just ends the process.
        if (msg.cmd && msg.cmd == 'stop') {
            logger.info("Worker " + cluster.worker.id + " stopping.");

            process.exit();
        }
    });

    // Guard against the scenario where sockets don't get properly restarted.
    var taskStarter = setInterval(resumeTask, 1000);

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
        });

        // Handle incoming data from clients.
        socket.on('data', function (data) {
            activeTasks++;
            if (activeTasks > config.blockrouter.max_tasks || ! queue.spaceAvailable()) {
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

            queue.spool(socket, data, function() {
                activeTasks--;

                if (activeTasks <= config.blockrouter.max_tasks && queue.spaceAvailable()) {

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
            socket.end();
            resumeTask();
        });
    }

    function setupTargets(targetConfig) {
        _.forEach(targetConfig, function(configs, target) {
            targets[target] = []; // This is a list of the socket objects
            targetQueues[target] = []; // This is a query of data blocks that need to be relayed

            // Setup on disk queues
            mkdirp.sync(config.blockrouter.disk_queue.path + "/" + target);

            configs.forEach(function(config) {
                relay.create(targets, target, config);
            });

            relay.start(targets, targetQueues, target);
        });

        queue.initialize(targets, targetQueues);
    }

    function resumeTask() {
        if ((waitingSockets.length > 0) && (activeTasks <= config.blockrouter.max_tasks)) {
            var task = waitingSockets.shift();
            task.resume();

            stats.count('resumed', 1);
        }
    }

    function welcome(socket) {
        socket.write(service_name + " " + service_version + " ready, fire away " + socket.remoteAddress + ".\n");
        logger.info("Connection from " + socket.name);
    }
}
