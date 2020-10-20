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
 * block:       N consecutive hours. N defults to 2 and can be set from environment
 *              variable FM_HOuRS_PER_BLOCK
 *
 * Used Redis Keys Used
 * ====================
 *
 * fm:blk               sorted set of blocks in which we have data
 * fm:d                 sorted set of devid's
 * fm:d:blk:<devid>     sorted set of blocks in which the device have data
 * fm:d:lgv:<devid>     copy of last good value of the device
 *
 * fm:_blk              sorted set of blocks in which we have archived data
 * fm:d:_blk:<devid>    sorted set of blocks in which the dev have archived data
 *
 */

const dataRoot = path.join(process.env['HOME'], '.local/share/ffc/dev-state');
const logRoot = path.join(process.env['HOME'], '.local/share/ffc/log');
const dftBlockHours = 2;

function FfcModel()
{
    var client;
    var logger;
    var blockHours;

    /**
     * A blockindex is an unambiguous integer identity of a block. Blockindex was
     * created from time.
     *
     * Sample example blockindexs are: 2020010100, 2020010102
     */
    const timeToBlockindex = time => {
        const y = time.getUTCFullYear();
        const m = time.getUTCMonth() + 1;
        const d = time.getUTCDate();
        const h = time.getUTCHours();
        return y * 1000000 + m * 10000 + d * 100 + Math.trunc(h / blockHours);
    };

    const blockdir = blockindex => path.join(dataRoot, blockindex.toString());
    const devBlockdir
        = (devid, blockindex) => path.join(blockdir(blockindex), devid.toString());
    const archiveDir = () => path.join(dataRoot, 'archive');

    const devStateFilename = (devid, time) => {
        const dir = devBlockdir(devid, timeToBlockindex(time));
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
        const key = 'fm:d:lgv:' + devid;
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
        const blockindex = timeToBlockindex(ticktime);

        client.zadd(['fm:d:blk:' + devid, blockindex, blockindex], err => {
            if (err) return cb(err);
            client.zadd(['fm:blk', blockindex, blockindex], cb);
        });
    };

    const markArchivedTime = (devid, blockindex, cb) => {
        client.zadd(['fm:d:_blk:' + devid, blockindex, blockindex], err => {
            if (err) return cb(err);
            client.zadd(['fm:_blk', blockindex, blockindex], cb);
        });
    };

    const removeBlockIndex = (blockindex, cb) => {
        const walkDevices = (list, removedDevices, cb) => {
            const devid = list.pop();
            if (! devid) return cb(null);
            client.zrem(['fm:d:blk:' + devid, blockindex], (err, reply) => {
                if (err) return cb(err);
                if (+reply > 0) removedDevices.push(devid);
                walkDevices(list, removedDevices, cb);
            });
        };

        client.zrem(['fm:blk', blockindex], err => {
            client.zrange(['fm:d', 0, -1], (err, devList) => {
                if (err || ! devList || ! devList.length) return cb(err);

                const removedDevices = [];
                walkDevices(devList, removedDevices, err => {
                    if (err) return cb(err);
                    cb(null, removedDevices);
                });
            });
        });
    };

    const removeBlock = (blockindex, cb) => {
        logger.info('remove block ' + blockindex);
        removeBlockIndex(blockindex, (err, removedDevices) => {
            if (err) return cb(err);

            try {
                shell.rm('-rf', blockdir(blockindex));
            } catch (err) {
                return cb(err);
            }
            cb(null, removedDevices);
        });
    };

    /**
     * Remove any block greater than the block indicated by the given (blockindex,
     * score).
     */
    const removeBlocksAfter = (blockindex, cb) => {
        const backWalkBlockList = (list, cb) => {
            const blockindex = list.pop();
            if (! blockindex) return cb(null);

            removeBlock(blockindex, err => {
                if (err) return cb(null);

                /* to avoid stack overflow */
                setTimeout(() => {
                    backWalkBlockList(list, cb);
                }, 1);
            });
        };

        client.zrangebyscore(['fm:blk', '(' + blockindex, '+inf'], (err, reply) => {
            if (err || ! reply || ! reply.length) return cb(err);
            backWalkBlockList(reply, cb);
        });
    };

    const archiveBlock = (blockindex, cb) => {
        logger.info('archive block ' + blockindex);

        const sdir = path.basename(blockdir(blockindex));
        const pdir = path.dirname(blockdir(blockindex));
        const archiveName = path.join(archiveDir(), blockindex + '.tgz');

        try {
            shell.mkdir('-p', archiveDir());
            const cmdline = `tar czf ${archiveName} -C ${pdir} ${sdir}`;
            shell.exec(cmdline, { silent: true }, (code, stdout, stderr) => {
                if (stderr) logger.error(stderr);
                removeBlock(blockindex, (err, removedDevices) => {
                    if (err) return cb(err);
                    (function markArchivedTimeFor(devices, cb) {
                        const m = devices.pop();
                        if (! m) return cb(null);
                        markArchivedTime(m, blockindex, err => {
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

    const archiveAgedBlocks = (level1Blocks, cb) => {
        const cutBlockList = (list, cb) => {
            if (! list.length) return cb(null);

            const d = list.shift();
            archiveBlock(d, cb);
        };

        client.zrange(['fm:blk', 0, -1], (err, blockList) => {
            if (err || ! blockList || blockList.length <= level1Blocks)
                return cb(err);
            cutBlockList(blockList.slice(0, blockList.length - level1Blocks), cb);
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
                            client.zadd(['fm:d', devid, devid], cb)
                        });
                    });
                });
        };

    this.putDevState = (devid, ticktime, devState, cb) => {
        logger.debug('put devState', devid, ticktime);
        saveDevStateAndUpdateIndex(devid, ticktime, devState, cb);
    };

    this.housekeeping = (options, cb) => {
        removeBlocksAfter(timeToBlockindex(new Date()), err => {
            if (err) return cb(err);
            if (! options.level1Blocks > 0) return cb(null);
            archiveAgedBlocks(options.level1Blocks, cb);
        });
    };

    this.projectMetrics = (devid, time, cb) => {
        cb(null, []);
    };

    this.getDeviceTimeSpan = (device, cb) => {
        cb(null, {
            start: new Date(),
            end: new Date(),
        });
    };

    this.getDeviceLastGoodValue = (device, cb) => {
        cb(null, {
            lastTicktime: new Date(),
            metrics: [],
        });
    };

    this.stop = () => {
        client.quit();
    };

    return (function (ffcModel) {
        logger = winston.createLogger({
            level: 'debug',
            format: winston.format.combine(
                winston.format.timestamp({ format: 'YY-MM-DD hh:mm:ss' }),
                winston.format.json(),
            ),
            transports: [
                new winston.transports.File({
                    filename: path.join(logRoot, 'ffcmodel-err.log'), level: 'error',
                }),
                new winston.transports.File({
                    filename: path.join(logRoot, 'ffcmodel.log'),
                }),
            ],
        });
        if (process.env['FM_LOG_CONSOLE'])
            logger.add(new winston.transports.Console());

        const n = process.env['MF_HOURS_PER_BLOCK'];
        if (n != null && ! isNaN(+n) && +n > 0 && +n <= 24)
            blockHours = n;
        else
            blockHours = dftBlockHours;
        logger.debug('blockHours ' + blockHours);

        client = redis.createClient();
        client.on('error', err => {
            logger.error(err);
        });

        return ffcModel;
    }(this));
}

module.exports = FfcModel;
