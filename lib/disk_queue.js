'use strict';

var fs = require('fs');
var getSize = require('get-folder-size');

var _ = require('lodash');

module.exports = function(context, stats_name) {
    var statsd = context.foundation.getConnection({type: 'statsd', cached: true}).client;
    var config = context.sysconfig;
    var logger = context.logger;

    // If the queue is full we can't accept new data.
    var queueFull = true; // Blocked to start

    var targets;
    var targetQueues;

    var spoolBuffer = "";

    // Protect against workers that sit around without receiving data
    // for a while but hold unsaved data in the buffer.
    setInterval(function() {
        if (spoolBuffer.length > 0) flushSpoolBuffer(function(){});
    }, 5000);

    function spool(socket, data, cb) {
        data = socket.remainder + data;

        var lines = data.toString().split('\n');

        // We assume the last line is partial so save it for combination
        // with the next chunk of data.
        socket.remainder = lines.pop();

        spoolBuffer += lines.join('\n');
        spoolBuffer += '\n';
        //statsd.count('spooled', lines.length());

        if (spoolBuffer.length > config.blockrouter.blocksize) {
            flushSpoolBuffer(cb);
        }
        else {
            cb();
        }
    }

    function enqueue(data, cb) {
        var datenow = Date.now();
        _.forOwn(targets, function(relays, target) {
            var filename = config.blockrouter.disk_queue.path + "/" + target + "/" + datenow;
            targetQueues[target].push(filename);
            fs.writeFileSync(filename, data);
        });

        cb();
    }

    function initialize(newtargets, newtargetQueues) {
        targets = newtargets;
        targetQueues = newtargetQueues;

        // Verify we have space available for processing.
        checkSize();

        // Also need to check periodically.
        var queueWatcher = setInterval(checkSize, 5000);

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

    function checkSize() {
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
         /*       if (waitingSockets.length > 0) {
                    var task = waitingSockets.shift();
                    task.resume();
                }*/
            }
        });
    }

    function spaceAvailable() {
        return ! queueFull;
    }

    function flushSpoolBuffer(cb) {
        var dataToSend = spoolBuffer;
        spoolBuffer = ""
        enqueue(dataToSend, cb);
    }

    return {
        spool: spool,
        initialize: initialize,
        spaceAvailable: spaceAvailable
    };
}