
var worker = require('./lib/worker');

function config_schema() {
    return {};
}

var foundation = require('terafoundation')({
    name: 'blockrouter',
    worker: worker,
    config_schema: config_schema
});
