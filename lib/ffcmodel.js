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

const devDataRoot = path.join(process.env['HOME'], '.local/share/ffc/dev-state');
const logRoot = path.join(process.env['HOME'], '.local/share/ffc/log');
const dftBlockHours = 2;

function FfcModel()
{
    var client;
    var logger;
    var blockHours;
    var level1BlocksTravelMax;
    var archiveBlocksTravelMax;

    /**
     * A blockindex (or shortly block) is an unambiguous integer identity of a
     * block. Blockindex was created from time.
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

    const blockdir = block => path.join(devDataRoot, block.toString());
    const devBlockdir =
        (devid, block) => path.join(blockdir(block), devid.toString());
    const archiveDir = devid => path.join(devDataRoot, 'archive', devid.toString());
    const archiveName =
        (devid, block) => path.join(archiveDir(devid), `${devid}-${block}.tgz`);

    const devStateFilename = (devid, time) => {
        const dir = devBlockdir(devid, timeToBlockindex(time));
        const pathname = path.join(dir,
            (time.valueOf() / 1000).toString()) + '.dat';
        const tmpname = pathname + '.tmp';

        return {pathname, tmpname};
    };

    const markTime = (devid, ticktime, cb) => {
        const block = timeToBlockindex(ticktime);

        client.zadd(['fm:blk:' + devid, block, block], cb);
    };

    const markDeviceBlockArchived = (devid, block, cb) => {
        client.zadd(['fm:_blk:' + devid, block, block], cb);
    };

    const removeDeviceBlockIndex = (devid, block, cb) => {
        client.zrem(['fm:blk:' + devid, block], cb);
    };

    const loadDevStateSpec = cb => {
        protobuf.load('data/dev-state.proto', (err, root) => {
            if (err) return cb(err);

            const spec = root.lookupType('DevState');
            var protoErr = spec.verify(spec);
            if (protoErr) return cb(Error(protoErr));
            return cb(null, spec);
        });
    };

    const persistDevState = (devid, ticktime, devState, cb) => {
        const serializeDevState = cb => {
            loadDevStateSpec((err, spec) => {
                if (err) return cb(err);

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

    const removeDeviceBlock = (devid, block, cb) => {
        logger.info(`remove block ${block} of device ${devid}`);
        removeDeviceBlockIndex(devid, block, err => {
            if (err) return cb(err);

            try {
                shell.rm('-rf', devBlockdir(devid, block));

                /* If this is the last device in the blockdir, then
                 * the parent directory can be removed, otherwise
                 * removing it will reported an error which we don't
                 * need to care.
                 */
                fs.rmdir(blockdir(block), () => {
                    cb(null);
                });
            } catch (err) {
                return cb(err);
            }
        });
    };

    const removeDeviceBlockAfter = (devid, block, cb) => {
        const key = `fm:blk:${devid}`;
        const rangeStart = `(${block}`;
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
    const removeBlocksAfter = (block, cb) => {
        client.zrange(['fm:devices', 0, -1], (err, devList) => {
            if (err || ! devList) return (cb);

            (function processDevices(devList) {
                if (! devList.length) return cb(null);

                const devid = devList[0];
                removeDeviceBlockAfter(devid, block, err => {
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

    const archiveDeviceBlock = (devid, block, cb) => {
        logger.info(`archive block ${block} of device ${devid}`);

        const sdir = path.join(path.basename(blockdir(block)), devid.toString());
        const pdir = devDataRoot;

        mkdirp(archiveDir(devid))
            .then(() => {
                const cmdline = `tar czf ${archiveName(devid, block)} -C ${pdir} ${sdir}`;
                try {
                    shell.exec(cmdline, { silent: true }, (code, stdout, stderr) => {
                        if (stderr) logger.error(stderr);
                        removeDeviceBlock(devid, block, err => {
                            if (err) return cb(err);
                            markDeviceBlockArchived(devid, block, cb);
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
            if (err || ! devList || ! devList.length) return cb(err);

            (function processList(devList) {
                if (! devList.length) return cb(null);
                const devid = devList[0];
                archiveDeviceAgedBlocks(devid, level1BlocksNum, err => {
                    if (err) return(err);
                    processList(devList.slice(1));
                });
            }(devList));
        });
    };

    const getBlocksBackwards = (devid, time, inArchive, cb) => {
        var key;
        var limit;
        if (inArchive) {
            key = `fm:_blk:${devid}`;
            limit = archiveBlocksTravelMax;
        } else {
            key = `fm:blk:${devid}`;
            limit = level1BlocksTravelMax;
        }

        const greatest = timeToBlockindex(time);
        client.zrevrangebyscore(key, greatest, '-inf',
            'limit', 0, limit,
            (err, reply) => {
                if (err) return cb(err);
                const blockList = reply.map(n => +n);
                cb(null, blockList);
            }
        );
    };

    const loadArchiveBlock = (devid, block, cb) => {
        const cmdline = `tar xzf ${archiveName(devid, block)} -C ${devDataRoot}`;
        try {
            shell.exec(cmdline, { silent: true }, (code, stdout, stderr) => {
                if (stderr) logger.error(stderr);
                if (code) return cb(Error('cmdline' + ' exited with ' + code));
                return cb(null);
            });
        } catch(err) {
            cb(err);
        }
    };

    const isBlockInArchive = (devid, block, cb) => {
        client.zrangebyscore('fm:_blk:' + devid, block, block, (err, reply) => {
            if (err) {
                logger.error(err);
                return cb(false);
            }
            cb(reply.length > 0);
        });
    };

    const openBlock = (devid, block, cb) => {
        isBlockInArchive(devid, block, yes => {
            const open = () => {
                const dir = devBlockdir(devid, block);
                fs.readdir(dir, (err, files) => {
                    if (err) return cb(err);
                    cb(null, dir, files); 
                });
            };

            if (yes)
                loadArchiveBlock(devid, block, err => {
                    if (err) return cb(err);
                    open();
                });
            else
                open();
        });
    };

    const filterAndSortFileList = (fileList, maxTime) => {
        var timeEpoch;
        if (maxTime) timeEpoch = Math.trunc(maxTime.valueOf() / 1000);

        const filtered = fileList.filter(name => {
            if (path.extname(name) != '.dat') return false;
            if (timeEpoch && +name.split('.')[0] > timeEpoch) return false;
            return true;
        });

        return filtered.sort((a, b) => +b.split('.')[0] - +a.split('.')[0]);
    };

    const copyMetricsFromFile = (pathname, metricIdList, cb) => {
        const chkcrc = (crc32Array, data) => {
            var chksum = 0;
            for (var i = 0; i < 32/8; ++i) chksum = chksum * 256 + crc32Array[i];
            return chksum == crc32(data);
        };

        const decodeDevState = (data, cb) => {
            loadDevStateSpec((err, spec) => {
                var devState;
                try {
                    devState = spec.decode(data);
                } catch (err) {
                    cb(err);
                }
                cb(null, devState);
            });
        };

        const ticktime = +path.basename(pathname).split('.')[0];

        fs.readFile(pathname, (err, buf) => {
            if (err) return cb(err)

            const crc32Array = buf.slice(0, 32/8); 
            const data = buf.slice(32/8);

            if (! chkcrc(crc32Array, data))
                return cb(Error('chksum error on file ' + pathname));

            const resultList = [];
            decodeDevState(data, (err, devState) => {
                if (err) return cb(err);

                devState.metrics.forEach(m => {
                    if (! metricIdList.length || metricIdList.includes(m.id))
                        resultList.push({...m, ticktime: ticktime});
                });
                cb(null, resultList);
            });
        });
    };

    const projectMetricsFromSortedFiles =
        (dir, files, metricIdList, alreadyHad, cb) => {
            const resultMetricList = [...alreadyHad];
            const resolvedIdList = resultMetricList.reduce(
                (acc, curr) => [...acc, curr.id], []);

            (function walkFiles(files) {
                if (! files.length) return cb(null, resultMetricList);
                const pathname = path.join(dir, files[0]);
                copyMetricsFromFile(pathname, metricIdList,
                    (err, metricsCopy) => {
                        if (err)
                            logger.error(err);
                        else {
                            metricsCopy.forEach(m => {
                                if (! resolvedIdList.includes(m.id)) {
                                    resultMetricList.push(m);
                                    resolvedIdList.push(m.id);
                                }
                            });
                        }
                        if (resolvedIdList.length < metricIdList.length)
                            walkFiles(files.slice(1));
                        else
                            return cb(null, resultMetricList);
                    });
            }(files));
        };

    const getMinTimeInBlock = (devid, block, cb) =>{
        openBlock(devid, block, (err, dir, files) => {
            if (err) return cb(err);
            if (! files.length) return cb('not found');
            files = filterAndSortFileList(files);
            cb(null, new Date(+files[files.length - 1].split('.')[0] * 1000));
        });
    };

    const getMaxTimeInBlock = (devid, block, cb) =>{
        openBlock(devid, block, (err, dir, files) => {
            if (err) return cb(err);
            if (! files.length) return cb('not found');
            files = filterAndSortFileList(files);
            cb(null, new Date(+files[0].split('.')[0] * 1000));
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

    /**
     * If metricIdList is not provided, it will just get the metrics copy from
     * the most recent device state file no younger than the givine 'time'.
     */
    this.projectMetrics = (devid, time, metricIdList, cb) => {
        if (typeof metricIdList == 'function') {
            cb = metricIdList;
            metricIdList = [];
        }

        var resultMetricList = [];

        const doProjectingInBlocks = (blockList, cb) => {
            (function walkBlockList(list) {
                if (! list.length) return cb();
                const b = list[0];
                openBlock(devid, b, (err, dir, files) => {
                    if (err) return cb();
                    files = filterAndSortFileList(files, time);
                    projectMetricsFromSortedFiles(dir, files,
                        metricIdList,
                        resultMetricList,
                        (err, improvedResult) => {
                            resultMetricList = improvedResult;
                            if (! metricIdList.length
                                || improvedResult.length == metricIdList.length)
                                return cb();
                            else
                                walkBlockList(list.slice(1));
                        });
                });
            }(blockList));
        };

        getBlocksBackwards(devid, time, false, (err, blockList) => {
            if (err) return cb(err);
            doProjectingInBlocks(blockList, () => {
                if (resultMetricList.length
                    && resultMetricList.length == metricIdList.length)
                    return cb(null, resultMetricList);
                getBlocksBackwards(devid, time, true, (err, blockList) => {
                    if (err) return cb(null, resultMetricList);
                    doProjectingInBlocks(blockList, () => {
                        return cb(null, resultMetricList);
                    });
                });
            });
        });
    };

    this.getDeviceTimeSpan = (devid, cb) => {
        var blockMin, blockMax;

        const key = 'fm:blk:' + devid;
        client.zrange(key, 0, 0, (err, reply) => {
            if (err) return cb(err);
            if (! reply || ! reply.length) return cb(Error('not exist'));
            blockMin = +reply[0];
            client.zrevrange('fm:blk:' + devid, 0, 0, (err, reply) => {
                if (err) return cb(err);
                if (! reply || ! reply.length) return cb(Error('not exist'));
                blockMax = +reply[0];

                client.zrange('fm:_blk:' + devid, 0, 0, (err, reply) => {
                    if (err) return cb(err);
                    if (reply && reply.length) blockMin = +reply[0];

                    getMinTimeInBlock(devid, blockMin, (err, min) => {
                        if (err) return cb(err);
                        getMaxTimeInBlock(devid, blockMax, (err, max) => {
                            if (err) return cb(err);
                            cb(null, min, max);
                        });
                    });
                });
            });
        });
    };

    this.getDeviceLastGoodValue = (devid, cb) => {
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

        const n = process.env['FM_HOURS_PER_BLOCK'];
        if (n != null && ! isNaN(+n) && +n > 0 && +n <= 24)
            blockHours = n;
        else
            blockHours = dftBlockHours;

        level1BlocksTravelMax = (2 * 24) / blockHours;
        archiveBlocksTravelMax = 2;  /* archive blocks are slower to open */

        client = redis.createClient();
        client.on('error', err => {
            logger.error(err);
        });

        return ffcModel;
    }(this));
}

module.exports = FfcModel;
