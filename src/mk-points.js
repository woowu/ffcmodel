#!/usr/bin/node --harmony
'use strict';

const redis = require('redis');

const startTime = new Date('2020-01-01T00:00+00:00')
const intvl = 15;           /* minutes */
const maxTicks = 96 * 36;
//const maxTicks = 96 * 180;
const maxMeter = 99;
const metricsNum = 79;

const shortTime = time => parseInt(time.valueOf() / 1000 / 60);

const saveLastGoodValue = (meter, time, cb) => {
    const args = [];

    args.push('m:lgv:' + meter);
    args.push('time');
    args.push(`${shortTime(time)}`);
    for (var i = 0; i < metricsNum; ++i) {
        args.push((i + 1).toString());
        var value = (Math.random() * 1000).toFixed(3);
        if (Math.trunc(Math.random() * 10) == 1)    /* should it has a flag */
            value = value + '*';
        args.push(value);
    }

    // console.log(args.join(' '));
    client.hmset(args, (err, reply) => {
        if (err) return cb(err);
        cb(null);
    });
};

const meterMarkTime = (meter, time, cb) => {
    const args = [];
    const t = shortTime(time);

    args.push('m:mt:' + meter);
    args.push(t);
    args.push(t);

    client.zadd(args, (err, reply) => {
        if (err) return cb(err);
        client.set(`m:p:${meter}:${t}`, 1, (err, reply) => {
            cb(err);
        });
    });
};

const newMeasurement = (time, meter, cb) => {
    saveLastGoodValue(meter, time, err => {
        if (err) return cb(err);
        meterMarkTime(meter, time, cb);
    });
};

const measureAtTime = (time, meter, cb) => {
    if  (meter > maxMeter) return cb(null);

    console.log('time ' + time.toISOString());
    newMeasurement(time, meter, err => {
        if (err) return cb(err);
        measureAtTime(time, meter + 1, cb);
    });
};

const walkTime = (time, tickCnt, cb) => {
    if (tickCnt == maxTicks) return cb(null, tickCnt);

    /* in order to avoid stack overflow */
    setTimeout(() => {
        measureAtTime(time, 0, err => {
            if (err) return cb(err, tickCnt);
            time.setMinutes(time.getMinutes() + intvl)
            walkTime(time, tickCnt + 1, cb);
        });
    }, 1);
};

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
