'use strict';

const path = require('path');
const fs = require('fs');
const redis = require('redis');
const protobuf = require('protobufjs');
const { crc32 } = require('crc');
const shell = require('shelljs');
const winston = require('winston');
const mkdirp = require('mkdirp');

/**
 * Terms
 * =====
 *
 * device:      A device holds metrics that we can acquire at any time.
 *
 * ticktime:    the time with whhich a set of metrics acquired from a device at
 *              whatever time should be associated.
 *
 * Used Redis Keys Used
 * ====================
 *
 * fm:tm                sorted set of days in which we have data
 * fm:m                 sorted set of devid's
 * fm:m:tm:<devid>      sorted set of days in which the device have data
 * fm:m:lgv:<devid>     copy of last good value of the device
 *
 * fm:a:tm              sorted set of days in which we have archived data
 * fm:a:m:tm:<devid>    sorted set of days in which the dev have archived data
 *
 */

const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/dev-state');
const logRoot = path.join(process.env['HOME'], '.local/share/ffc/log');

function FfcModel()
{
    var client;
    var logger;

    /**
     * A daymark is an easy to remeber day name, which is a 8-digi integer,
     * such as 20200101. Daymark was created from ticktime.
     */
    const timeToDaymark = time => {
        const y = time.getUTCFullYear();
        const m = time.getUTCMonth() + 1;
        const d = time.getUTCDate();
        return y * 10000 + m * 100 + d;
    };

    const daydir = daymark => path.join(dataRoot, daymark.toString());
    const devDaydir
        = (devid, daymark) => path.join(daydir(daymark), devid.toString());
    const archiveDir = () => path.join(dataRoot, 'archive');

    const devStateFilename = (devid, time) => {
        const dir = devDaydir(devid, timeToDaymark(time));
        const pathname = path.join(dir,
            (time.valueOf() / 1000).toString()) + '.dat';
        const tmpname = pathname + '.tmp';

        return {pathname, tmpname};
    };

    const persistDevState = (devid, ticktime, devState, cb) => {
        const serializeDevState = cb => {
            protobuf.load('data/dev-state.proto', (err, root) => {
                if (err) return cb(err);

                const spec = root.lookupType('DevState');
                const protoErr = spec.verify(devState);
                if (protoErr) return cb(Error(protoErr));

                const encoded = spec.encode(spec.fromObject(devState)).finish();
                const chksum = crc32(encoded);
                const chksumArray = new Uint8Array(32/8);
                for (var i = 0; i < chksumArray.length; ++i)
                    chksumArray[i] = chksum >> 8 * (chksumArray.length - 1 - i);

                cb(null, Buffer.concat([chksumArray, encoded]));
            });
        };

        const wrFile = buf => {
            const {pathname, tmpname} = devStateFilename(devid, ticktime);

            fs.access(pathname, err => {
                const newFile = err != null;

                mkdirp(path.dirname(tmpname))
                    .then(() => {
                        fs.writeFile(tmpname, buf, err => {
                            if (err) return cb(err); 
                            fs.rename(tmpname, pathname, err => {
                                cb(null, newFile);
                            });
                        });
                    })
                    .catch(cb);
            });
        };

        serializeDevState((err, buf) => {
            if (err) return cb(err);
            wrFile(buf);
        });
    };

    /**
     * Last Good Value of a device is a list of most recent status of its metrics, for
     * each of which we remember the ticktime associated the metric as well as all the
     * other properties found in the metric.  We also remember a most recent ticktime
     * ever recorded for any last metrics of the device, with which we can associate
     * the whole set of last metrics of this device.
     */
    const updateLastGoodValue = (devid, devState, ticktime, cb) => {
        const t = Math.trunc(ticktime.valueOf() / 1000);
        const key = 'fm:m:lgv:' + devid;
        var modified = false;

        const updateMetric = (metric, cb) => {
            const id = metric.id.toString();
            const args = [
                key,
                id + '_ticktime',
                t,
                id + '_status',
                metric.status,
                id + '_value',
                metric.value,
                id + '_scale',
                metric.scale,
            ];
            if (metric.timestamp != null)
                args.push(id + '_timestamp', metric.timestamp);
            client.hmset(args, cb);
        };

        (function updateMetrics(metrics, cb) {
            const m = metrics[0];
            const remaining = metrics.slice(1);
            if (! m) return cb(null);

            const id = m.id.toString();

            client.hget(key, id + '_ticktime', (err, thatTime) => {
                if (err) return cb(err);
                if (thatTime && +thatTime > t) return cb(null);
                updateMetric(m, err => {
                    if (err) return cb(err);
                    modified = true;
                    updateMetrics(remaining, cb);
                });
            });
        }(devState.metrics, err => {
            if (! modified) return cb(null);
            client.hget(key, 'ticktime', (err, lasttime) => {
                if (lasttime && lasttime >= t) return cb(null);
                client.hset(key, 'ticktime', t, cb);
            });
        }));
    };

    const markTime = (devid, ticktime, cb) => {
        const daymark = timeToDaymark(ticktime);

        client.zadd(['fm:m:tm:' + devid, daymark, daymark], err => {
            if (err) return cb(err);
            client.zadd(['fm:tm', daymark, daymark], cb);
        });
    };

    const markArchivedTime = (devid, daymark, cb) => {
        client.zadd(['fm:a:m:tm:' + devid, daymark, daymark], err => {
            if (err) return cb(err);
            client.zadd(['fm:a:tm', daymark, daymark], cb);
        });
    };

    const removeDayIndex = (daymark, cb) => {
        const walkDevices = (list, removedDevices, cb) => {
            const devid = list.pop();
            if (! devid) return cb(null);
            client.zrem(['fm:m:tm:' + devid, daymark], (err, reply) => {
                if (err) return cb(err);
                if (+reply > 0) removedDevices.push(devid);
                walkDevices(list, removedDevices, cb);
            });
        };

        client.zrem(['fm:tm', daymark], err => {
            client.zrange(['fm:m', 0, -1], (err, devList) => {
                if (err || ! devList || ! devList.length) return cb(err);

                const removedDevices = [];
                walkDevices(devList, removedDevices, err => {
                    if (err) return cb(err);
                    cb(null, removedDevices);
                });
            });
        });
    };

    const removeDay = (daymark, cb) => {
        logger.info('remove day ' + daymark);
        removeDayIndex(daymark, (err, removedDevices) => {
            if (err) return cb(err);

            try {
                shell.rm('-rf', daydir(daymark));
            } catch (err) {
                return cb(err);
            }
            cb(null, removedDevices);
        });
    };

    /**
     * Remove any day greater than the day indicated by the given (daymark,
     * score).
     */
    const removeDaysAfter = (daymark, cb) => {
        const backWalkDayList = (list, cb) => {
            const daymark = list.pop();
            if (! daymark) return cb(null);

            removeDay(daymark, err => {
                if (err) return cb(null);

                /* to avoid stack overflow */
                setTimeout(() => {
                    backWalkDayList(list, cb);
                }, 1);
            });
        };

        client.zrangebyscore(['fm:tm', '(' + daymark, '+inf'], (err, reply) => {
            if (err || ! reply || ! reply.length) return cb(err);
            backWalkDayList(reply, cb);
        });
    };

    const archiveDay = (daymark, cb) => {
        logger.info('archive day ' + daymark);

        const sdir = path.basename(daydir(daymark));
        const pdir = path.dirname(daydir(daymark));
        const archiveName = path.join(archiveDir(), daymark + '.tgz');

        try {
            shell.mkdir('-p', archiveDir());
            const cmdline = `tar czf ${archiveName} -C ${pdir} ${sdir}`;
            shell.exec(cmdline, { silent: true }, (code, stdout, stderr) => {
                if (stderr) logger.error(stderr);
                removeDay(daymark, (err, removedDevices) => {
                    if (err) return cb(err);
                    (function markArchivedTimeFor(devices, cb) {
                        const m = devices.pop();
                        if (! m) return cb(null);
                        markArchivedTime(m, daymark, err => {
                            if (err) return cb(err);
                            markArchivedTimeFor(devices, cb);
                        });
                    }(removedDevices, cb));
                });
            });
        } catch (err) {
            cb(err);
        }
    };

    const archiveAgedDays = (level1Days, cb) => {
        const cutDayList = (list, cb) => {
            if (! list.length) return cb(null);

            const d = list.shift();
            archiveDay(d, cb);
        };

        client.zrange(['fm:tm', 0, -1], (err, dayList) => {
            if (err || ! dayList || dayList.length <= level1Days)
                return cb(err);
            cutDayList(dayList.slice(0, dayList.length - level1Days), cb);
        });
    };

    const saveDevStateAndUpdateIndex =
        (devid, ticktime, metrics, cb) => {
            persistDevState(devid, ticktime, metrics,
                (err, newFile) => {
                    if (err) return cb(err);

                    updateLastGoodValue(devid, metrics, ticktime, err => {
                        if (err) return cb(err);
                        markTime(devid, ticktime, err => {
                            if (err || ! newFile) return cb(err);
                            client.zadd(['fm:m', devid, devid], cb)
                        });
                    });
                });
        };

    this.putDevState = (devid, ticktime, devState, cb) => {
        saveDevStateAndUpdateIndex(devid, ticktime, devState, cb);
    };

    this.housekeeping = (options, cb) => {
        removeDaysAfter(timeToDaymark(new Date()), err => {
            if (err) return cb(err);
            if (! options.level1Days > 0) return cb(null);
            archiveAgedDays(options.level1Days, cb);
        });
    };

    this.stop = () => {
        client.quit();
    };

    return (function (ffcModel) {
        logger = winston.createLogger({
            level: 'info',
            format: winston.format.simple(),
            transports: [
                new winston.transports.File({
                    filename: path.join(logRoot, 'ffcmodel-err.log'), level: 'error',
                }),
                new winston.transports.File({
                    filename: path.join(logRoot, 'ffcmodel.log'),
                }),
            ],
        });

        client = redis.createClient();
        client.on('error', err => {
            logger.error(err.message);
        });

        return ffcModel;
    }(this));
}

module.exports = FfcModel;
