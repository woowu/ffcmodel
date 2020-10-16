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

const acquire = (devid, nominalTime, cb) => {
    const delay = Math.trunc(60 * Math.random());
    const args = [
        '-d', devid.toString(),
        '-t', (nominalTime.valueOf() / 1000).toString(),
        '-l', delay.toString(),
    ];

    if (argv.m != null) args.push('-m', argv.m);
    if (argv.M != null) args.push('-M', argv.m);
    if (argv.json) args.push('-j');

    const cmd = './bin/acqr';
    spawn(cmd, args)
        .on('exit', code => {
            const cmdline = [cmd, ...args].join(' ');
            cb(devid, code, cmdline);
        });
}

const scheduleAcquire = (nominalTime, cb) => {
    console.log('time ' + nominalTime.toISOString());

    const begin = new Date();
    (function serialAndParallel(all, cb) {
        const parallel = all.slice(0, argv.parallelNumber);
        if (! parallel.length) return cb(null);

        var nwait = parallel.length;
        parallel.forEach(devid => {
            acquire(devid, nominalTime, (devid, code, cmdline) => {
                if (code) {
                    console.error('acquire device ' + devid
                        + ' exited with code ' + code + '. cmdline:');
                    console.error(cmdline);
                }
                if (! --nwait)
                    serialAndParallel(all.slice(argv.parallelNumber), cb);
            });
        });
    }([...Array(argv.devices).keys()], err => {
        const elapsed = new Date() - begin;
        console.log(`acquired ${argv.devices} devices in ${elapsed} msecs. avg ${Math.trunc(elapsed/argv.devices)} msecs/dev`);
        cb(err);
    }));
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
