#!/usr/bin/node --harmony
'use strict';

const { spawn } = require('child_process');

const argv = require('yargs') 
    .option('t', {
        alias: 'ticks',
        describe: 'number of time ticks to run',
        demandOption: true,
        nargs: 1,
    })
    .option('d', {
        alias: 'devices',
        describe: 'number of devices',
        demandOption: true,
        nargs: 1,
    })
    .option('i', {
        alias: 'intvl',
        describe: 'acquisition interval in minutes',
        nargs: 1,
        default: 15,
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
    .option('j', {
        alias: 'json',
        describe: 'save metrics in json files',
    })
    .option('s', {
        alias: 'startTime',
        describe: 'time to start',
        nargs: 1,
        default: '2020-01-22T00:00Z',
    })
    .option('p', {
        alias: 'parallelNumber',
        describe: 'max number of devices can be acquired parallelly',
        nargs: 1,
        default: 24,
    })
    .argv;

const acquire = (devid, scheduleTime, cb) => {
    const delay = Math.trunc(60 * Math.random());
    const args = [
        '-d', devid.toString(),
        '-t', (scheduleTime.valueOf() / 1000).toString(),
        '-l', delay.toString(),
    ];

    if (argv.m != null) args.push('-m', argv.m);
    if (argv.M != null) args.push('-M', argv.m);
    if (argv.json) args.push('-j');

    spawn('./bin/acqr', args)
        .on('exit', code => {
            cb(devid, code);
        });
}

const scheduleAcquire = (time, cb) => {
    console.log('time ' + time.toISOString());

    (function serialAndParallel(all, cb) {
        const parallel = all.slice(0, argv.parallelNumber);
        if (! parallel.length) return cb(null);

        var nwait = parallel.length;
        parallel.forEach(devid => {
            acquire(devid, time, (devid, code) => {
                if (code) console.log('acquire device ' + devid
                    + ' exited with code ' + code);
                if (! --nwait)
                    serialAndParallel(all.slice(argv.parallelNumber), cb);
            });
        });
    }([...Array(argv.devices).keys()], cb));
};

const startTime = new Date(argv.startTime);
if (isNaN(startTime.valueOf())) {
    console.error('invalid time');
    process.exit(1);
}

(function walkTime(time, tickCnt, cb) {
    if (tickCnt == argv.ticks) return cb(null, tickCnt);

    scheduleAcquire(time, err => {
        if (err) return cb(err, tickCnt);
        time.setMinutes(time.getMinutes() + argv.intvl)

        /* to avoid stack overflow */
        setTimeout(() => {
            walkTime(time, tickCnt + 1, cb);
        }, 1);
    });
}(startTime, 0, (err, tickCnt) => {
    if (err)
        console.error(err.message);
    else
        console.log(`ttl ${tickCnt} ticks`);
}));
