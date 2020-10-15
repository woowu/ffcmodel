'use strict';

const path = require('path');
const fs = require('fs');
const redis = require('redis');
const protobuf = require('protobufjs');
const { crc32 } = require('crc');
const shell = require('shelljs');

/**
 * Used Redis Keys Used
 * =====================
 *
 * fm:tm                sorted set of days in which we have data
 * fm:m                 sorted set of meters
 * fm:m:tm:<meter>      sorted set of days in which the meter have data
 * fm:m:lgv:<meter>     last good value of metrics of the meter
 */

/**
 * Measurement files are orgnized in below directory structure:
 *
 * daydir/meterDaydir/level1-files
 * archive/archive-files
 */

const dftLevel1Days = 5;
const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/measurement');

function FfcModel()
{
    var client;

    /**
     * A daymark is an easy to remeber day name, which is a 8-digi integer, such as
     * 20200101. Daymark was created from scheduled acquisition time.
     */
    const calcDaymark = time => {
        const y = time.getUTCFullYear();
        const m = time.getUTCMonth() + 1;
        const d = time.getUTCDate();
        const mark = y * 10000 + m * 100 + d;

        /* make the score smaller */
        const score = Math.trunc((new Date(Date.UTC(y, m - 1, d))
            - new Date('2020-01-01T00:00:00Z')) / 1000 / 3600 / 24);

        return {mark, score};
    };

    const daydir = daymark => path.join(dataRoot, daymark.toString());
    const meterDaydir
        = (meter, daymark) => path.join(daydir(daymark), meter.toString());
    const archiveDir = () => path.join(dataRoot, 'archive');

    const measureFileName = (meter, measureTime) => {
        const dir = meterDaydir(meter, calcDaymark(measureTime).mark);
        const pathname = path.join(dir, (measureTime.valueOf() / 1000).toString())
            + '.dat';
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
        const {mark, score} = calcDaymark(time);

        client.zadd(['fm:m:tm:' + meter, score, mark], err => {
            if (err) return cb(err);
            client.zadd(['fm:tm', score, mark], cb);
        });
    };

    const addMeter = (meter, cb) => client.zadd(['fm:m', meter, meter], cb);

    const removeDayIndex = (daymark, cb) => {
        const walkMeters = (list, cb) => {
            const meter = list.pop();
            if (! meter) return cb(null);
            client.zrem(['fm:m:tm:' + meter, daymark], err => {
                if (err) return cb(err);
                walkMeters(list, cb);
            });
        };

        client.zrem(['fm:tm', daymark], err => {
            client.zrange(['fm:m', 0, -1], (err, meterList) => {
                if (err || ! meterList || ! meterList.length) return cb(err);
                walkMeters(meterList, cb);
            });
        });
    };

    const removeDay = (daymark, cb) => {
        console.log('remove day ' + daymark);
        removeDayIndex(daymark, err => {
            if (err) console.error(err.message);
            shell.rm('-rf', daydir(daymark));
            cb(null);
        });
    };

    const archiveDay = (daymark, cb) => {
        console.log('archive day ' + daymark);

        const sdir = path.basename(daydir(daymark));
        const pdir = path.dirname(daydir(daymark));
        const archiveName = path.join(archiveDir(), daymark + '.tgz');

        shell.mkdir('-p', archiveDir());
        const cmdline = `tar czf ${archiveName} -C ${pdir} ${sdir}`;
        console.log(cmdline);
        shell.exec(cmdline);

        removeDay(daymark, cb);
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
            if (err || ! dayList || dayList.length <= level1Days) return cb(err);
            cutDayList(dayList.slice(0, dayList.length - level1Days), cb);
        });
    };

    /* TODO: acquire write lock. */
    const houseKeeping = (lastAcquireTime, cb) => {
        const {daymark, score} = calcDaymark(lastAcquireTime);

        removeYetToComeDays(daymark, score, err => {
            if (err) return cb(err);
            archiveAgedDays(cb);
        });
    };

    /**
     * Remove any day greater than the day indicated by the given (daymark, score).
     */
    const removeYetToComeDays = (daymark, score, cb) => {
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

        client.zrangebyscore(['fm:tm', '(' + score, '+inf'], (err, reply) => {
            if (err || ! reply || ! reply.length) return cb(err);
            backWalkDayList(reply, cb);
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
                        addMeter(meter, cb);
                    });
                });
            });
    };

    this.putMeasurement = (meter, scheduleTime, realTime, measure, cb) => {
        saveMeasureAndUpdateIndex(meter, scheduleTime, realTime, measure, err => {
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
    }(this));
}

module.exports = FfcModel;
