#!/usr/bin/node --harmony
'use strict';

const path = require('path');
const fs = require('fs');
const redis = require('redis');
const protobuf = require('protobufjs');
const { crc32 } = require('crc');
const shell = require('shelljs');

/*---------------------------------------------------------------------------*/

const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/measurement');
var activeDays = 7;

/*---------------------------------------------------------------------------*/

/* Keys
 * ====
 *
 * fm:tm                sorted set of days in which we have data
 * fm:m                 sorted set of meters
 * fm:m:tm:<meter>      sorted set of days in which the meter have data
 * fm:m:lgv:<meter>     last good value of metrics of the meter
 */

/*---------------------------------------------------------------------------*/

/**
 * A daymark is easy to remeber day name, which is a 8-digi integer, such as
 * 20200101.
 */
const calcDayMark = time => {
    const y = time.getUTCFullYear();
    const m = time.getUTCMonth() + 1;
    const d = time.getUTCDate();
    const mark = y * 10000 + m * 100 + d;

    /* make the score smaller */
    const score = Math.trunc((new Date(Date.UTC(y, m - 1, d)) - new Date('2020-01-01T00:00:00Z'))
        / 1000 / 3600 / 24);

    return {mark, score};
}

/**
 * Measurement files are orgnized in below directory structure:
 *
 * dayDir/meterDayDir/active-files
 * archive/archive-files
 */
const dayDir = dayMark => path.join(dataRoot, dayMark.toString());
const meterDayDir = (meter, dayMark) => path.join(dayDir(dayMark), meter.toString());
const archiveDir = () => path.join(dataRoot, 'archive');

const measureFileName = (meter, measureTime) => {
    const dir = meterDayDir(meter, calcDayMark(measureTime).mark);
    const pathname = path.join(dir, (measureTime.valueOf() / 1000).toString())
        + '.dat';
    const tmpname = pathname + '.tmp';

    return {pathname, tmpname};
};

const saveMeasurement = (meter, time, measure, cb) => {
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
        const {pathname, tmpname} = measureFileName(meter, time);

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

const saveLastGoodValue = (meter, measure, cb) => {
    const args = ['fm:m:lgv:' + meter, 'timestamp', measure.timestamp];

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
    const {mark, score} = calcDayMark(time);

    client.zadd(['fm:m:tm:' + meter, score, mark], err => {
        if (err) return cb(err);
        client.zadd(['fm:tm', score, mark], cb);
    });
};

const addMeter = (meter, cb) => {
    client.zadd(['fm:m', meter, meter], cb);
};

const removeDayIndex = (dayMark, cb) => {
    const walkMeters = (list, cb) => {
        const meter = list.pop();
        if (! meter) return cb(null);
        client.zrem(['fm:m:tm:' + meter, dayMark], err => {
            if (err) return cb(err);
            walkMeters(list, cb);
        });
    };

    client.zrem(['fm:tm', dayMark], err => {
        client.zrange(['fm:m', 0, -1], (err, meterList) => {
            if (err || ! meterList || ! meterList.length) return cb(err);
            walkMeters(meterList, cb);
        });
    });
};

const removeDay = (dayMark, cb) => {
    console.log('remove day ' + dayMark);
    removeDayIndex(dayMark, err => {
        if (err) console.error(err.message);
        shell.rm('-rf', dayDir(dayMark));
        cb(null);
    });
};

const archiveDay = (dayMark, cb) => {
    console.log('archive day ' + dayMark);

    const sdir = path.basename(dayDir(dayMark));
    const pdir = path.dirname(dayDir(dayMark));
    const archiveName = path.join(archiveDir(), dayMark + '.tgz');

    shell.mkdir('-p', archiveDir());
    const cmdline = `tar czf ${archiveName} -C ${pdir} ${sdir}`;
    console.log(cmdline);
    shell.exec(cmdline);

    removeDay(dayMark, cb);
};

/**
 * Remove any day which has is greater than that day indicated
 * by the given (dayMark, score).
 */
const removeYetToComeDays = (dayMark, score, cb) => {
    const backWalkDayList = (list, cb) => {
        const dayMark = list.pop();
        if (! dayMark) return cb(null);

        removeDay(dayMark, err => {
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

const saveMeasureAndUpdateIndex = (meter, time, measure, cb) => {
    /* TODO: acquire read lock. */

    saveMeasurement(meter, time, measure, (err, newFile) => {
        if (err) return cb(err);

        saveLastGoodValue(meter, measure, err => {
            if (err) return cb(err);
            markTime(meter, time, err => {
                if (err || ! newFile) return cb(err);
                addMeter(meter, cb);
            });
        });
    });
};

const archiveAgedDays = cb => {
    const n = parseInt(process.env['FM_ACTIVE_DAYS']);
    if  (! isNaN(n) && n > 0) activeDays = n;

    const cutDayList = (list, cb) => {
        if (! list.length) return cb(null);

        const d = list.shift();
        archiveDay(d, cb);
    };

    client.zrange(['fm:tm', 0, -1], (err, dayList) => {
        if (err || ! dayList || dayList.length <= activeDays) return cb(err);
        cutDayList(dayList.slice(0, dayList.length - activeDays), cb);
    });
};

const houseKeeping = (lastDataTime, cb) => {
    /* TODO: acquire write lock. */

    const {dayMark, score} = calcDayMark(lastDataTime);

    removeYetToComeDays(dayMark, score, err => {
        if (err) return cb(err);
        archiveAgedDays(cb);
    });
};

/*---------------------------------------------------------------------------*/

const putMeasurement = (meter, time, measure, cb) => {
    saveMeasureAndUpdateIndex(meter, time, measure, err => {
        if (err) return err;
        houseKeeping(time, cb);
    });
};

/*---------------------------------------------------------------------------*/
var client;

var jsonOut;
var measuresCnt = 0;

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

const newMeasurement = (meter, time, cb) => {
    const measure = {
        meter,
        timestamp: parseInt(time.valueOf() / 1000),
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
                time.valueOf() / 1000 -
                Math.trunc(3600 * Math.random());
    }
    if (jsonOut)
        jsonOut.write((measuresCnt ? ',\n' : '')
            + JSON.stringify(measure, null, 2));

    putMeasurement(meter, time, measure, err => {
        ++measuresCnt;
        cb(err);
    });
};

const measureAtTime = (meter, time, cb) => {
    if  (meter == argv.meters) return cb(null);

    console.log('time ' + time.toISOString());
    newMeasurement(meter, time, err => {
        if (err) return cb(err);
        measureAtTime(meter + 1, time, cb);
    });
};

const walkTime = (time, tickCnt, cb) => {
    if (tickCnt == argv.ticks) return cb(null, tickCnt);

    measureAtTime(0, time, err => {
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

if (argv.j) {
    jsonOut = fs.createWriteStream(argv.json);
    jsonOut.write('[\n');
}

client = redis.createClient();
client.on('error', err => {
    console.error(err.message);
});

walkTime(startTime, 0, (err, tickCnt) => {
    client.quit();
    if (jsonOut) jsonOut.end('\n]');

    if (err)
        console.error(err.message);
    else
        console.log(`ttl ${tickCnt} ticks`);
});
