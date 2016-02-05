'use strict';

module.exports = function(context, stats_name) {
    var statsd = context.foundation.getConnection({type: 'statsd', cached: true}).client;
    var config = context.sysconfig;

    return {
        count: function(counter, increment, reduce) {
            var cluster = 'production';
            if (config.blockrouter.cluster) cluster = config.blockrouter.cluster;

            statsd.increment('blockrouter.' + cluster + '.' + config._nodeName + '.' + stats_name + '.' + counter, increment, reduce);
            statsd.increment('blockrouter.' + cluster + '.' + stats_name + '.' + counter, increment, reduce);
        }
    }
}