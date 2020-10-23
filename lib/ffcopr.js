'use strict';
const mkdirp = require('mkdirp');
const dateformat = require('dateformat');
const fs = require('fs');
const path = require('path');

const parseMetricsSpec = spec => {
    const metrics = new Set();

    spec.split(',').forEach(def => {
        if (isNaN(+def) && def.search('-') >= 0) {
            const start = +def.split('-')[0];
            const end = +def.split('-')[1];
            for (var i = start; i <= end; ++i) {
                if (isNaN(i) || i < 0)
                    return null;
                metrics.add(i);
            }
        } else if (isNaN(+def) || +def < 0)
            return null;
        else
            metrics.add(+def);
    });
    return Array.from(metrics);
};

const acquire = (model, devid, ticktime, acqtime, metricIdList, saveJson, cb) => {
    const devState = {
        devid,
        timestamp: parseInt(acqtime.valueOf() / 1000),
        metrics: [],
    };

    for (const id of metricIdList) {
        const m = {
            id: id,
            status: 0,
            /* random +- integer with 1 to 4 digits */
            value: Math.trunc(Math.pow(10, 4) * Math.random())
                - Math.pow(10, 4) / 2 + 1,
            /* -5 to 5 */
            scale: Math.trunc(11 * Math.random()) - 5,
        };
        if (id >= 60 && id <= 79)
            m.timestamp = devState.timestamp - Math.trunc(3600 * Math.random());
        devState.metrics.push(m);
    }

    model.putDeviceState(devid, ticktime, devState, err => {
        if (err || ! saveJson) return cb(err);

        const json = JSON.stringify(devState, null, 2);
        const jsonName = path.join(process.env['HOME'], '.local/share/ffc/json',
            devid.toString()
            + '-'
            + dateformat(ticktime, 'UTC:yyyymmddHHMMss')
            + '.json');
        mkdirp(path.dirname(jsonName))
            .then(() => {
                fs.writeFile(jsonName, json, cb);
            })
            .catch(cb);
    });
};

module.exports = {
    acquire,
    parseMetricsSpec,
}
