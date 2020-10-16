'use strict';

const path = require('path');
const fs = require('fs');
const redis = require('redis');
const protobuf = require('protobufjs');
const { crc32 } = require('crc');
const shell = require('shelljs');

/**
 * Used Redis Keys Used
 * ====================
 *
 * fm:tm                sorted set of days in which we have data
 * fm:m                 sorted set of devid's
 * fm:m:tm:<devid>      sorted set of days in which the dev have data
 * fm:m:lgv:<devid>     last good value of the dev
 *
 * fm:a:tm              sorted set of days in which we have archived data
 * fm:a:m:tm:<devid>    sorted set of days in which the dev have archived data
 *
 * Files Orgnization
 * =================
 *
 *  Dirs
 *  ----
 *  daydir/devDaydir/level1-files
 *  level2/daydir/devDaydir/level2-files
 *  archive/archive-files
 */

const dftLevel1Days = 5;
const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/metrics');

function FfcModel()
{
    var client;

    /**
     * A daymark is an easy to remeber day name, which is a 8-digi integer,
     * such as 20200101. Daymark was created from scheduled acquisition time.
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

    const metricsFilename = (devid, time) => {
        const dir = devDaydir(devid, timeToDaymark(time));
        const pathname = path.join(dir,
            (time.valueOf() / 1000).toString()) + '.dat';
        const tmpname = pathname + '.tmp';

        return {pathname, tmpname};
    };

    const persistMetrics = (devid, time, metrics, cb) => {
        const serializeMetrics = cb => {
            protobuf.load('data/devmetrics.proto', (err, root) => {
                if (err) return cb(err);

                const spec = root.lookupType('DevMetrics');
                const protoErr = spec.verify(metrics);
                if (protoErr) return cb(Error(protoErr));

                const encoded = spec.encode(spec.fromObject(metrics)).finish();
                const chksum = crc32(encoded);
                const chksumArray = new Uint8Array(32/8);
                for (var i = 0; i < chksumArray.length; ++i)
                    chksumArray[i] = chksum >> 8 * (chksumArray.length - 1 - i);

                cb(null, Buffer.concat([chksumArray, encoded]));
            });
        };

        const wrFile = buf => {
            const {pathname, tmpname} = metricsFilename(devid, time);

            fs.access(pathname, err => {
                const newFile = err != null;
                shell.mkdir('-p', path.dirname(pathname));
                fs.writeFile(tmpname, buf, err => {
                    if (err) return cb(err); 
                    shell.mv('-f', tmpname, pathname);
                    cb(null, newFile);
                });
            });
        };

        serializeMetrics((err, buf) => {
            if (err) return cb(err);
            wrFile(buf);
        });
    };

    const updateLastGoodValue = (devid, metrics, time, cb) => {
        const args = [
            'fm:m:lgv:' + devid,
            'nominalTime',
            time.valueOf() / 1000,
            'timestamp',
            metrics.timestamp,
        ];

        metrics.metrics.forEach(m => {
            args.push('status' + m.id, m.status);
            args.push('value' + m.id, m.value);
            args.push('scale' + m.id, m.scale);
            if (m.hasOwnProperty('timestamp'))
                args.push('timestamp' + m.id, m.timestamp);
        });
        client.hmset(args, cb);
    };

    const markTime = (devid, time, cb) => {
        const daymark = timeToDaymark(time);

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
        console.log('remove day ' + daymark);
        removeDayIndex(daymark, (err, removedDevices) => {
            if (err) console.error(err.message);
            shell.rm('-rf', daydir(daymark));
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
        console.log('archive day ' + daymark);

        const sdir = path.basename(daydir(daymark));
        const pdir = path.dirname(daydir(daymark));
        const archiveName = path.join(archiveDir(), daymark + '.tgz');

        shell.mkdir('-p', archiveDir());
        const cmdline = `tar czf ${archiveName} -C ${pdir} ${sdir}`;
        shell.exec(cmdline);

        removeDay(daymark, (err, removedDevices) => {
            (function markArchivedTimeFor(devices, cb) {
                const m = devices.pop();
                if (! m) return cb(null);
                markArchivedTime(m, daymark, err => {
                    if (err) return cb(err);
                    markArchivedTimeFor(devices, cb);
                });
            }(removedDevices, cb));
        });
    };

    const archiveAgedDays = cb => {
        var level1Days = dftLevel1Days;
        const n = parseInt(process.env['FM_LEVEL1_DAYS']);
        if  (! isNaN(n) && n > 0) level1Days = n;

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

    const saveMetricsAndUpdateIndex =
        (devid, time, metrics, cb) => {
            persistMetrics(devid, time, metrics,
                (err, newFile) => {
                    if (err) return cb(err);

                    updateLastGoodValue(devid, metrics, time, err => {
                        if (err) return cb(err);
                        markTime(devid, time, err => {
                            if (err || ! newFile) return cb(err);
                            client.zadd(['fm:m', devid, devid], cb)
                        });
                    });
                });
        };

    this.putDevMetrics = (devid, time, metrics, cb) => {
        saveMetricsAndUpdateIndex(devid, time, metrics,
            err => {
                if (err) return cb(err);

                /* TODO: do it periodically from another process, not every
                 * time when metrics was put.
                 */
                this.houseKeeping(cb);
            });
    };

    this.houseKeeping = cb => {
        removeDaysAfter(timeToDaymark(new Date()), err => {
            if (err) return cb(err);
            archiveAgedDays(cb);
        });
    };

    this.stop = () => {
        client.quit();
    };

    return (function (ffcModel) {
        client = redis.createClient();
        client.on('error', err => {
            console.error(err.message);
        });

        return ffcModel;
    }(this));
}

module.exports = FfcModel;
