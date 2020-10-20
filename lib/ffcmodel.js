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
 * fm:devices           sorted set of devid's
 * fm:blk:<devid>       sorted set of blocks in which the device have data
 * fm:lgv:<devid>       copy of last good value of the device
 *
 * fm:_blk:<devid>      sorted set of blocks in which the dev have archived data
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

    const markTime = (devid, ticktime, cb) => {
        const blockindex = timeToBlockindex(ticktime);

        client.zadd(['fm:blk:' + devid, blockindex, blockindex], cb);
    };

    const markDeviceBlockArchived = (devid, blockindex, cb) => {
        client.zadd(['fm:_blk:' + devid, blockindex, blockindex], cb);
    };

    const removeDeviceBlockIndex = (devid, blockindex, cb) => {
        client.zrem(['fm:blk:' + devid, blockindex], cb);
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
        const key = 'fm:lgv:' + devid;
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

    const removeDeviceBlock = (devid, blockindex, cb) => {
        logger.info(`remove block ${blockindex} of device ${devid}`);
        removeDeviceBlockIndex(devid, blockindex, err => {
            if (err) return cb(err);

            try {
                shell.rm('-rf', devBlockdir(devid, blockindex));

                /* If this is the last device in the blockdir, then
                 * the parent directory can be removed, otherwise
                 * removing it will reported an error which we don't
                 * need to care.
                 */
                fs.rmdir(blockdir(blockindex), () => {
                    cb(null);
                });
            } catch (err) {
                return cb(err);
            }
        });
    };

    const removeDeviceBlockAfter = (devid, blockindex, cb) => {
        const key = `fm:blk:${devid}`;
        const rangeStart = `(${blockindex}`;
        client.zrangebyscore(key, rangeStart, '+inf', (err, blockList) => {
            if (err || ! blockList || ! blockList.length) return cb(err);

            logger.debug(`device ${devid} got ${blockList.length} younger than now blocks to remove`);
            (function processBlocks(blocks) {
                if (! blocks.length) return cb(null);
                const block = blocks[0];
                removeDeviceBlockIndex(devid, block, err => {
                    if (err) return cb(err);

                    try {
                        shell.rm('-rf', devBlockdir(devid, block));
                    } catch (err) {
                        return cb(err);
                    }
                    processBlocks(blocks.slice(1));
                });
            }(blockList));
        });
    };

    /**
     * Remove blocks younger than the given one from all the devices.
     */
    const removeBlocksAfter = (blockindex, cb) => {
        client.zrange(['fm:devices', 0, -1], (err, devList) => {
            if (err || ! devList) return (cb);

            (function processDevices(devList) {
                if (! devList.length) return cb(null);

                const devid = devList[0];
                removeDeviceBlockAfter(devid, blockindex, err => {
                    if (err) return cb(err);
                    processDevices(devList.slice(1));
                });
            }(devList));
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
                            client.zadd(['fm:devices', devid, devid], cb)
                        });
                    });
                });
        };

    const archiveDeviceBlock = (devid, blockindex, cb) => {
        logger.info(`archive block ${blockindex} of device ${devid}`);

        const sdir = path.basename(devBlockdir(devid, blockindex));
        const pdir = path.dirname(devBlockdir(devid, blockindex));
        const archiveName = path.join(archiveDir(),
            `${devid}-${blockindex}.tgz`);

        mkdirp(archiveDir())
            .then(() => {
                const cmdline = `tar czf ${archiveName} -C ${pdir} ${sdir}`;
                try {
                    shell.exec(cmdline, { silent: true }, (code, stdout, stderr) => {
                        if (stderr) logger.error(stderr);
                        removeDeviceBlock(devid, blockindex, err => {
                            if (err) return cb(err);
                            markDeviceBlockArchived(devid, blockindex, cb);
                        });
                    });
                } catch(err) {
                    cb(err);
                }
            })
            .catch(cb);
    };

    const archiveDeviceAgedBlocks = (devid, level1BlocksNum, cb) => {
        const key = `fm:blk:${devid}`;
        client.zcard(key, (err, length) => {
            if (err || ! length) return cb(err);
            length = +length;
            if (length - level1BlocksNum <= 0) return cb(null);

            const nremove = length - level1BlocksNum;
            logger.debug(`device ${devid} got ${nremove} aged blocks to remove`);

            client.zrange([key, 0, nremove - 1],
                (err, blockList) => {
                    if (err || ! blockList || ! blockList.length) return cb(err);

                    (function processBlocks(blocks) {
                        if (! blocks.length) return cb(null);

                        const b = +blocks[0];
                        archiveDeviceBlock(devid, b, err => {
                            if (err) return cb(err);
                            processBlocks(blocks.slice(1));
                        });
                    }(blockList));
                });
        });
    };

    const archiveAgedBlocks = (level1BlocksNum, cb) => {
        client.zrange(['fm:devices', 0, -1], (err, devList) => {
            logger.debug('200');
            if (err || ! devList || ! devList.length) return cb(err);
            logger.debug('201');

            (function processList(devList) {
                if (! devList.length) return cb(null);
                const devid = devList[0];
                logger.debug('202');
                archiveDeviceAgedBlocks(devid, level1BlocksNum, err => {
                    if (err) return(err);
                    processList(devList.slice(1));
                });
            }(devList));
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
                winston.format.timestamp({ format: 'YYYY-MM-DD hh:mm:ss' }),
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

        client = redis.createClient();
        client.on('error', err => {
            logger.error(err);
        });

        return ffcModel;
    }(this));
}

module.exports = FfcModel;
