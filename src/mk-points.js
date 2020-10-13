#!/usr/bin/node --harmony
'use strict';

const path = require('path');
const fs = require('fs');
const redis = require('redis');
const protobuf = require('protobufjs');
const { crc32 } = require('crc');
const shell = require('shelljs');

const startTime = new Date('2020-01-22T00:00+00:00')
const intvl = 15;   /* minutes */
const metricsNum = 79;
const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/measurement');

/* Keys
 * ====
 *
 * fm:np                number of points
 * fm:np:<day>          number of points in a day
 * fm:tm                sorted array of days
 * fm:m:tm:<meter>      sorted array of days of a meter
 * fm:m:lgv:<meter>     last good value of metrics of a meter
 */

const calcDayMark = time => {
    const y = time.getUTCFullYear();
    const m = time.getUTCMonth() + 1;
    const d = time.getUTCDate();
    const mark = (y - 2000) * 10000 + m * 100 + d;
    const score = y * 10000 + m * 100 + d - 20100101;   /* make the number smaller */

    return {mark, score};
}

const saveLastGoodValue = (meter, time, cb) => {
    const args = ['fm:m:lgv:' + meter, 'time', time / 1000];

    for (var i = 0; i < metricsNum; ++i) {
        args.push((i + 1).toString());
        var value = (Math.random() * 1000).toFixed(3);
        if (Math.trunc(Math.random() * 10) == 1)    /* should it has a flag */
            value = value + '*';
        args.push(value);
    }

    client.hmset(args, cb);
};

const markTime = (meter, time, cb) => {
    const {mark, score} = calcDayMark(time);

    client.zadd(['fm:m:tm:' + meter, mark, score], err => {
        if (err) return cb(err);
        client.zadd(['fm:tm', mark, score], cb);
    });
};

const incCounters = (meter, time, cb) => {
    client.incr('fm:np', err => {
        if (err) return cb(err);

        const mark = calcDayMark(time).mark;
        client.incr('fm:np:' + mark, cb);
    });
};

const saveMeasurement = (time, meter, measure, cb) => {

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
        const dayName = calcDayMark(time).mark;
        const dir = path.join(dataRoot, dayName.toString(), meter.toString());
        const pathname = path.join(dir, (time.valueOf() / 1000).toString()) + '.dat';
        const tmpname = pathname + '.tmp';
        fs.access(pathname, err => {
            const newFile = err != null;
            shell.mkdir('-p', dir);
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

const newMeasurement = (time, meter, cb) => {
    const measure = {
        meter,
        timestamp: parseInt(time.valueOf() / 1000),
    };

    saveMeasurement(time, meter, measure, (err, newFile) => {
        if (err) return cb(err);

        saveLastGoodValue(meter, time, err => {
            if (err) return cb(err);
            markTime(meter, time, err => {
                if (err || ! newFile) return cb(err);
                incCounters(meter, time, cb);
            });
        });
    });
};

const measureAtTime = (time, meter, cb) => {
    if  (meter == argv.m) return cb(null);

    console.log('time ' + time.toISOString());
    newMeasurement(time, meter, err => {
        if (err) return cb(err);
        measureAtTime(time, meter + 1, cb);
    });
};

const walkTime = (time, tickCnt, cb) => {
    if (tickCnt == argv.n) return cb(null, tickCnt);

    /* in order to avoid stack overflow */
    setTimeout(() => {
        measureAtTime(time, 0, err => {
            if (err) return cb(err, tickCnt);
            time.setMinutes(time.getMinutes() + intvl)
            walkTime(time, tickCnt + 1, cb);
        });
    }, 1);
};

const argv = require('yargs') 
    .option('n', {
        describe: 'number of time points',
        demandOption: true,
        nargs: 1,
    })
    .option('m', {
        describe: 'number of meters',
        demandOption: true,
        nargs: 1,
    })
    .argv;

const client = redis.createClient();

client.on('error', err => {
    console.error(err);
});

walkTime(startTime, 0, (err, tickCnt) => {
    client.quit();
    if (err)
        console.error(err.message);
    else
        console.log(`data genereated on ttl ${tickCnt} ticks`);
});
