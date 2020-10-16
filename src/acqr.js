#!/usr/bin/node --harmony
'use strict';

const fs = require('fs');
const path = require('path');
const shell = require('shelljs');
const dateformat = require('dateformat');
const Model = require('../lib/ffcmodel');

const acquire = (devid, scheduleTime, realTime, json, cb) => {
    const metrics = {
        devid,
        timestamp: parseInt(realTime.valueOf() / 1000),
        metrics: [],
    };

    for (var i = 0; i < argv.smallMetrics + argv.bigMetrics; ++i) {
        metrics.metrics[i] = {
            id: i + 1,
            status: 0,
            /* random +- integer with 1 to 4 digits */
            value: Math.trunc(Math.pow(10, 4) * Math.random())
                - Math.pow(10, 4) / 2 + 1,
            /* -5 to 5 */
            scale: Math.trunc(11 * Math.random()) - 5,
        };
        if (i >= argv.smallMetrics)
            metrics.metrics[i].timestamp =
                realTime.valueOf() / 1000 -
                Math.trunc(3600 * Math.random());
    }

    const model = new Model();
    model.putDevMetrics(devid, scheduleTime, realTime, metrics, err => {
        model.stop();
        if (err || ! json) return cb(err);
        const jsonName = path.join(process.env['HOME'], '.local/share/ffc/json',
            devid.toString()
            + '-'
            + dateformat(scheduleTime, 'UTC:yyyymmddhhMMss')
            + '.json');
        shell.mkdir('-p', path.dirname(jsonName));
        fs.writeFile(jsonName, JSON.stringify(metrics, null, 2), cb);
    });
};

const argv = require('yargs') 
    .option('d', {
        alias: 'devid',
        describe: 'identity of device to acquire',
        nargs: 1,
        demandOption: true,
    })
    .option('t', {
        alias: 'time',
        describe: 'scheduled acquisition time, in Epoch',
        nargs: 1,
        demandOption: true,
    })
    .option('m', {
        alias: 'smallMetrics',
        describe: 'number of small metrics of each dev',
        nargs: 1,
        default: 20,
    })
    .option('M', {
        alias: 'bigMetrics',
        describe: 'number of big metrics of each dev',
        nargs: 1,
        default: 6,
    })
    .option('l', {
        alias: 'delay',
        describe: 'simulate delay of this seconds',
        nargs: 1,
        default: 0,
    })
    .option('j', {
        alias: 'json',
        describe: 'save metrics in json file',
    })
    .argv;

const scheduleTime = new Date(argv.time * 1000);
var realTime = new Date(scheduleTime.valueOf() + argv.delay * 1000);

console.log(`acquiring device ${argv.d}`);
acquire(argv.d, scheduleTime, realTime, argv.json, err => {
    if (err) console.error(err.message);
});
