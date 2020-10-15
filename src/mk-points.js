#!/usr/bin/node --harmony
'use strict';

const fs = require('fs');
const Model = require('../lib/ffcmodel');

/*---------------------------------------------------------------------------*/
var model;
var jsonOut = {};

const argv = require('yargs') 
    .option('t', {
        alias: 'ticks',
        describe: 'number of time ticks to run',
        demandOption: true,
        nargs: 1,
    })
    .option('e', {
        alias: 'meters',
        describe: 'number of meters',
        demandOption: true,
        nargs: 1,
    })
    .option('i', {
        alias: 'intvl',
        describe: 'measuring interval in minutes',
        nargs: 1,
        default: 15,
    })
    .option('m', {
        alias: 'smallMetrics',
        describe: 'number of small metrics of each meter',
        nargs: 1,
        default: 16,
    })
    .option('M', {
        alias: 'bigMetrics',
        describe: 'number of big metrics of each meter',
        nargs: 1,
        default: 4,
    })
    .option('j', {
        alias: 'json',
        describe: 'save metrics json in a file for reference/debug',
        nargs: 1,
    })
    .option('s', {
        alias: 'startTime',
        describe: 'time to start',
        nargs: 1,
        default: '2020-01-22T00:00Z',
    })
    .argv;

const acquireFromMeter = (meter, scheduleTime, realTime, cb) => {
    const measure = {
        meter,
        timestamp: parseInt(realTime.valueOf() / 1000),
        metrics: [],
    };

    for (var i = 0; i < argv.smallMetrics + argv.bigMetrics; ++i) {
        measure.metrics[i] = {
            id: i + 1,
            status: 0,
            /* random +- integer with 1 to 4 digits */
            value: Math.trunc(Math.pow(10, 4) * Math.random())
                - Math.pow(10, 4) / 2 + 1,
            /* -5 to 5 */
            scale: Math.trunc(11 * Math.random()) - 5,
        };
        if (i >= argv.smallMetrics)
            measure.metrics[i].timestamp =
                realTime.valueOf() / 1000 -
                Math.trunc(3600 * Math.random());
    }

    if (jsonOut) {
        if (! jsonOut.cnt) jsonOut.ws.write('[');
        jsonOut.ws.write((jsonOut.cnt ? ',' : '')
            + '\n' + JSON.stringify(measure, null, 2));
        ++jsonOut.cnt;
    }

    model.putMeasurement(meter, scheduleTime, realTime, measure, err => {
        cb(err);
    });
};

const scheduleAcquire = (meter, time, cb) => {
    if  (meter == argv.meters) return cb(null);

    const delay = Math.trunc((argv.intvl * 60) * Math.random());
    var realTime = new Date(time.valueOf() + delay * 1000);

    console.log('time ' + time.toISOString());
    acquireFromMeter(meter, time, realTime, err => {
        if (err) return cb(err);
        scheduleAcquire(meter + 1, time, cb);
    });
};

const walkTime = (time, tickCnt, cb) => {
    if (tickCnt == argv.ticks) return cb(null, tickCnt);

    scheduleAcquire(0, time, err => {
        if (err) return cb(err, tickCnt);
        time.setMinutes(time.getMinutes() + argv.intvl)

        /* to avoid stack overflow */
        setTimeout(() => {
            walkTime(time, tickCnt + 1, cb);
        }, 1);
    });
};

const startTime = new Date(argv.startTime);
if (isNaN(startTime.valueOf())) {
    console.error('invalid time');
    process.exit(1);
}

if (argv.j)
    jsonOut = {
        ws: fs.createWriteStream(argv.json),
        cnt: 0,
    };

model = new Model();

walkTime(startTime, 0, (err, tickCnt) => {
    model.stop();
    if (jsonOut) jsonOut.ws.end('\n]');

    if (err)
        console.error(err.message);
    else
        console.log(`ttl ${tickCnt} ticks`);
});
