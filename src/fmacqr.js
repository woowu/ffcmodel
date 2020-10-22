#!/usr/bin/node --harmony
'use strict';

const fs = require('fs');
const path = require('path');
const shell = require('shelljs');
const dateformat = require('dateformat');
const mkdirp = require('mkdirp');
const Model = require('../lib/ffcmodel');

const acquire = (devid, ticktime, acqtime, metrics, saveJson, cb) => {
    const devState = {
        devid,
        timestamp: parseInt(acqtime.valueOf() / 1000),
        metrics: [],
    };

    for (const id of metrics) {
        const m = {
            id: id,
            status: 0,
            /* random +- integer with 1 to 4 digits */
            value: Math.trunc(Math.pow(10, 4) * Math.random())
                - Math.pow(10, 4) / 2 + 1,
            /* -5 to 5 */
            scale: Math.trunc(11 * Math.random()) - 5,
        };
        if (id >= 60 && id <= 79)
            m.timestamp = devState.timestamp - Math.trunc(3600 * Math.random());
        devState.metrics.push(m);
    }

    const model = new Model();
    model.putDeviceState(devid, ticktime, devState, err => {
        model.stop();
        if (err || ! saveJson) return cb(err);

        const json = JSON.stringify(devState, null, 2);
        const jsonName = path.join(process.env['HOME'], '.local/share/ffc/json',
            devid.toString()
            + '-'
            + dateformat(ticktime, 'UTC:yyyymmddHHMMss')
            + '.json');
        mkdirp(path.dirname(jsonName))
            .then(() => {
                fs.writeFile(jsonName, json, cb);
            })
            .catch(cb);
    });
};

const parseMetricsSpec = spec => {
    const metrics = new Set();

    spec.split(',').forEach(def => {
        if (isNaN(+def) && def.search('-') >= 0) {
            const start = +def.split('-')[0];
            const end = +def.split('-')[1];
            for (var i = start; i <= end; ++i) {
                if (isNaN(i) || i < 0) {
                    console.error('invalid metric id');
                    process.exit(1);
                }
                metrics.add(i);
            }
        } else if (isNaN(+def) || +def < 0) {
            console.error('invalid metric id ' + def);
            process.exit(1);
        } else
            metrics.add(+def);
    });
    return Array.from(metrics);
};

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
const metrics = parseMetricsSpec(argv.metrics);

if (devid == null) {
    console.error('missed devid');
    process.exit(1);
}
if (time == null) {
    console.error('missed time');
    process.exit(1);
}

const ticktime = new Date(time * 1000);
var acqtime = new Date(ticktime.valueOf() + argv.delay * 1000);

console.log(`acquiring device ${devid}`);
acquire(devid, ticktime, acqtime, metrics, argv.json, err => {
    if (err) console.error(err);
});
