#!/usr/bin/node --harmony
'use strict';
const Model = require('../lib/ffcmodel');
const ffcopr = require('../lib/ffcopr');

const argv = require('yargs') 
    .scriptName('fmacqr')
    .usage('$0 [options] devid time')
    .option('m', {
        alias: 'metrics',
        describe: 'comma separated list of metrics identities\n'
            + 'Examples: -m 1-20 ; -m 5,70-79'
            ,
        nargs: 1,
        demandOption: true,
    })
    .option('l', {
        alias: 'delay',
        describe: 'simulate delay of this seconds',
        nargs: 1,
        default: 0,
    })
    .option('j', {
        alias: 'json',
        describe: 'save device state json file',
        type: 'boolean'
    })
    .argv;

const devid = argv._[0];
const time = argv._[1];
const metricIdList = ffcopr.parseMetricsSpec(argv.metrics);

if (devid == null) {
    console.error('missed devid');
    process.exit(1);
}
if (time == null) {
    console.error('missed time');
    process.exit(1);
}

const ticktime = new Date(time * 1000);
const acqtime = new Date(ticktime.valueOf() + argv.delay * 1000);
const model = new Model();

console.log(`acquiring device ${devid}`);
ffcopr.acquire(model, devid, ticktime, acqtime, metricIdList, argv.json, err => {
    model.stop();
    if (err) console.error(err);
});
