#!/usr/bin/node --harmony
'use strict';

const { spawn } = require('child_process');
const readline = require('readline');
const dateformat = require('dateformat');
const Model = require('../lib/ffcmodel');
const ffcopr = require('../lib/ffcopr');

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
    return metrics;
};

const parseIntvlSpec = spec => {
    const intvl = spec.split(':')[0];
    const intvlSpec = { intvl, metrics: new Set() };

    for (const m of parseMetricsSpec(spec.split(':')[1]))
        intvlSpec.metrics.add(m);
    return intvlSpec;
};

const schdAcqr = argv => {
    const intvlDefs = [];

    const acquire = (devid, ticktime, metrics, fork, cb) => {
        const ticktimeEpoch = (ticktime.valueOf() / 1000).toString();
        const delay = Math.trunc(60 * Math.random());

        if (fork) {
            const args = [
                '-l', delay.toString(),
                devid.toString(),
                ticktimeEpoch,
            ];

            args.push('-m', metrics.join(','));
            if (argv.json) args.push('-j');

            const cmd = './bin/fmacqr';
            const cmdline = [cmd, ...args].join(' ');
            const child = spawn(cmd, args)
                .on('exit', code => {
                    cb(code, cmdline);
                });

            const rl = readline.createInterface({ input: child.stderr });
            rl.on('line', line => console.error(`dev ${devid} error: ${line}`));
        } else
            (function () {
                const model = new Model();
                ffcopr.acquire(model,
                    devid,
                    ticktime,
                    new Date(ticktime.valueOf() + delay * 1000),
                    metrics,
                    argv.json,
                    err => {
                        model.stop();
                        cb(err);
                    }
                );
            }());
    }

    const schedule = (ticktime, metrics, cb) => {
        const begin = new Date();
        (function serialAndParallel(all, cb) {
            const parallel = all.slice(0, argv.parallelNumber);
            if (! parallel.length) return cb(null);

            var nwait = parallel.length;
            parallel.forEach(devid => {
                acquire(devid, ticktime, metrics, argv.fork, (code, cmdline) => {
                    if  (code == null || typeof code == 'object') {
                        /* an Error object in no-fork mode */
                        var err = code;
                        if (err) return cb(err);
                    }
                    if (typeof code == 'number' && code)
                        return cb(Error('acquire device ' + devid
                            + ' exited with code ' + code + '. cmdline: '
                            + cmdline));
                    if (! --nwait)
                        serialAndParallel(all.slice(argv.parallelNumber), cb);
                });
            });
        }([...Array(argv.devices).keys()], err => {
            const elapsed = new Date() - begin;
            console.log(`acquired ${argv.devices} devices in ${elapsed} msecs.`
                + ` avg ${Math.round(elapsed/argv.devices)} msecs/dev`);
            cb(err);
        }));
    };

    const startTime = new Date(argv.startTime);
    if (isNaN(startTime.valueOf())) {
        console.error('invalid time');
        process.exit(1);
    }
    startTime.setMinutes(0);
    startTime.setSeconds(0);
    startTime.setMilliseconds(0);

    argv.intvl.forEach(intvl => {
       intvlDefs.push(parseIntvlSpec(intvl));
    });

    (function walkTime(time, tickCnt, cb) {
        if (tickCnt == argv.ticks) return cb(null, tickCnt);

        const metrics = new Set();
        for (const def of intvlDefs) {
            if (! (time.getMinutes() % def.intvl))
                for (const m of def.metrics) metrics.add(m);
        }

        const nextMin = new Date(time);
        nextMin.setMinutes(nextMin.getMinutes() + 1);

        /* to avoid stack overflow */
        setTimeout(() => {
            if (metrics.size) {
                console.log('time ' + time.toISOString(),
                    'remained ' + (argv.ticks - tickCnt + 1));
                schedule(time, Array.from(metrics), err => {
                    if (err) return cb(err, tickCnt);
                    walkTime(nextMin, tickCnt + 1, cb);
                });
            } else
                walkTime(nextMin, tickCnt, cb);
        }, 1);
    }(startTime, 0, (err, tickCnt) => {
        if (err)
            console.error(err);
        else
            console.log(`ttl ${tickCnt} ticks`);
    }));
};

const housekeeping = argv => {
    if (isNaN(+argv.level1)) {
        console.error('bad level1 number');
        return;
    }

    const model = new Model();
    model.housekeeping({ level1Blocks: argv.level1 }, err => {
        model.stop();
        if (err) console.error(err);
    });
};

const projectMetrics = argv => {
    const devid = +argv._[1];
    const time = new Date(argv._[2]);

    if (isNaN(time.valueOf())) {
        console.error('invalid time');
        return;
    }

    var metricList = [];
    if (argv.metrics) metricList = Array.from(parseMetricsSpec(argv.metrics));

    const timeStart = new Date();
    const model = new Model();
    model.projectMetrics(devid, time, metricList, (err, metrics) => {
        model.stop();
        const timeEnd = new Date();
        if (err) console.err(err);
        console.log('result ', metrics);
        console.log('used ' + (timeEnd - timeStart) / 1000 + 's');
    });
};

const timeSpan = argv => {
    var devid = argv._[1];

    if (devid == null) {
        console.error('no devid provided');
        process.exit(1);
    };
    devid = +devid;

    const timeStart = new Date();
    const model = new Model();
    model.getDeviceTimeSpan(devid, (err, minTime, maxTime) => {
        model.stop();
        const timeEnd = new Date();
        if (err) console.error(err);
        if (minTime && maxTime) { /* the device may not have */
            console.log('from: ' + dateformat(minTime, 'UTC:yyyy-mm-dd HH:MM Z'));
            console.log('to:   ' + dateformat(maxTime, 'UTC:yyyy-mm-dd HH:MM Z'));
            console.log('used ' + (timeEnd - timeStart) / 1000 + 's');
        }
    });
}

require('yargs') 
    .scriptName('fmcli')
    .usage('$0 <cmd> [options] [args]')
    .command('acqr', 'schedule acquisition', yargs => {
        yargs.option('t', {
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
            describe: 'acquisition interval in minutes, which followed by a\n'
                + 'metric id list. Multiple -i options is allowed.\n'
                + 'Examples: -i 15:1-20 ; -i 5:21,70-79'
                ,
            nargs: 1,
            demandOption: true,
            type: 'array',
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
        .option('f', {
            alias: 'fork',
            describe: 'time to start',
            type: 'boolean',
            default: true,
        })
        .option('p', {
            alias: 'parallelNumber',
            describe: 'max number of devices can be acquired parallelly',
            nargs: 1,
            default: 24,
        });
    }, schdAcqr)
    .command('clean', 'housekeeping the model', yargs => {
        yargs.option('a', {
            alias: 'level1',
            describe: 'number of blocks to keep in level1',
            nargs: 1,
            default: 7,
        })
    }, housekeeping)
    .command('project', 'project metrics', yargs => {
        yargs.option('m', {
            alias: 'metrics',
            describe: 'list of comma separated list of metric IDs.\n'
                + 'E.g., -m 1,3,5 ; -m 1-20,70',
            nargs: 1,
        })
        .positional('device', {
            describe: 'the device identity for which to do the projecting',
        })
        .positional('time', {
            describe: 'Time string in "YYYY-MM-DD HH:MM".'
                + ' Do the projecting from this time',
        })
    }, projectMetrics)
    .command('span', 'get time span', yargs => {
        yargs.positional('device', {
            describe: 'device identity',
        })
    }, timeSpan)
    .argv;
