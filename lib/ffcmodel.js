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
 * fm:tm                sorted set of days in which we have measurement data
 * fm:m                 sorted set of meters
 * fm:m:tm:<meter>      sorted set of days in which the meter have measurement data
 * fm:m:lgv:<meter>     last good value of measurement of the meter
 *
 * fm:a:tm              sorted set of days in which we have archived measurement
 * fm:a:m:tm:<meter>    sorted set of days in which the meter have archived measurement data
 *
 * Files Orgnization
 * =================
 *
 *  Dirs
 *  ----
 *  daydir/meterDaydir/level1-files
 *  level2/daydir/meterDaydir/level2-files
 *  archive/archive-files
 */

const dftLevel1Days = 5;
const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/measurement');

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
    const meterDaydir
        = (meter, daymark) => path.join(daydir(daymark), meter.toString());
    const archiveDir = () => path.join(dataRoot, 'archive');

    const measureFileName = (meter, measureTime) => {
        const dir = meterDaydir(meter, timeToDaymark(measureTime));
        const pathname = path.join(dir,
            (measureTime.valueOf() / 1000).toString()) + '.dat';
        const tmpname = pathname + '.tmp';

        return {pathname, tmpname};
    };

    const persistMeasure = (meter, scheduleTime, measure, cb) => {
        const serializeMeasure = cb => {
            protobuf.load('data/measurement.proto', (err, root) => {
                if (err) return cb(err);

                const spec = root.lookupType('measurement');
                const protoErr = spec.verify(measure);
                if (protoErr) return cb(Error(protoErr));

                const encoded = spec.encode(spec.fromObject(measure)).finish();
                const chksum = crc32(encoded);
                const chksumArray = new Uint8Array(32/8);
                for (var i = 0; i < chksumArray.length; ++i)
                    chksumArray[i] = chksum >> 8 * (chksumArray.length - 1 - i);

                cb(null, Buffer.concat([chksumArray, encoded]));
            });
        };

        const wrFile = buf => {
            const {pathname, tmpname} = measureFileName(meter, scheduleTime);

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

        serializeMeasure((err, buf) => {
            if (err) return cb(err);
            wrFile(buf);
        });
    };

    const updateLastGoodValue = (meter, measure, scheduleTime, cb) => {
        const args = [
            'fm:m:lgv:' + meter,
            'timestamp',
            measure.timestamp,
            'scheduleTime',
            scheduleTime.valueOf() / 1000,
        ];

        measure.metrics.forEach(m => {
            args.push('status' + m.id, m.status);
            args.push('value' + m.id, m.value);
            args.push('scale' + m.id, m.scale);
            if (m.hasOwnProperty('timestamp'))
                args.push('timestamp' + m.id, m.timestamp);
        });
        client.hmset(args, cb);
    };

    const markTime = (meter, time, cb) => {
        const daymark = timeToDaymark(time);

        client.zadd(['fm:m:tm:' + meter, daymark, daymark], err => {
            if (err) return cb(err);
            client.zadd(['fm:tm', daymark, daymark], cb);
        });
    };

    const markArchivedTime = (meter, daymark, cb) => {
        client.zadd(['fm:a:m:tm:' + meter, daymark, daymark], err => {
            if (err) return cb(err);
            client.zadd(['fm:a:tm', daymark, daymark], cb);
        });
    };

    const removeDayIndex = (daymark, cb) => {
        const walkMeters = (list, removedMeters, cb) => {
            const meter = list.pop();
            if (! meter) return cb(null);
            client.zrem(['fm:m:tm:' + meter, daymark], (err, reply) => {
                if (err) return cb(err);
                if (+reply > 0) removedMeters.push(meter);
                walkMeters(list, removedMeters, cb);
            });
        };

        client.zrem(['fm:tm', daymark], err => {
            client.zrange(['fm:m', 0, -1], (err, meterList) => {
                if (err || ! meterList || ! meterList.length) return cb(err);

                const removedMeters = [];
                walkMeters(meterList, removedMeters, err => {
                    if (err) return cb(err);
                    cb(null, removedMeters);
                });
            });
        });
    };

    const removeDay = (daymark, cb) => {
        console.log('remove day ' + daymark);
        removeDayIndex(daymark, (err, removedMeters) => {
            if (err) console.error(err.message);
            shell.rm('-rf', daydir(daymark));
            cb(null, removedMeters);
        });
    };

    /**
     * Remove any day greater than the day indicated by the given (daymark,
     * score).
     */
    const removeYetToComeDays = (daymark, cb) => {
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

        removeDay(daymark, (err, removedMeters) => {
            (function markArchivedTimeFor(meters, cb) {
                const m = meters.pop();
                if (! m) return cb(null);
                markArchivedTime(m, daymark, err => {
                    if (err) return cb(err);
                    markArchivedTimeFor(meters, cb);
                });
            }(removedMeters, cb));
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

    /* TODO: acquire write lock. */
    const houseKeeping = (lastAcquireTime, cb) => {
        const daymark = timeToDaymark(lastAcquireTime);

        removeYetToComeDays(daymark, err => {
            if (err) return cb(err);
            archiveAgedDays(cb);
        });
    };

    /* TODO: acquire read lock. */
    const saveMeasureAndUpdateIndex
        = (meter, scheduleTime, realTime, measure, cb) => {

        persistMeasure(meter, scheduleTime, measure,
            (err, newFile) => {
                if (err) return cb(err);

                updateLastGoodValue(meter, measure, scheduleTime, err => {
                    if (err) return cb(err);
                    markTime(meter, scheduleTime, err => {
                        if (err || ! newFile) return cb(err);
                        client.zadd(['fm:m', meter, meter], cb)
                    });
                });
            });
    };

    this.putMeasurement = (meter, scheduleTime, realTime, measure, cb) => {
        saveMeasureAndUpdateIndex(meter, scheduleTime, realTime, measure,
            err => {
                if (err) return cb(err)
                houseKeeping(scheduleTime, cb);
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
