/**
 * 'volttron': device connector for an Eclipse VOLTTRON platform, via the
 * fuxa-bridge agent (https://github.com/eclipse-volttron).
 *
 * Reads stream in real time from the bridge WebSocket (`/ws`): a snapshot on
 * connect then per-scrape updates. Writes go to the bridge REST endpoint
 * (`PUT /api/points`), which calls the platform driver's set_point RPC.
 *
 * Device property:
 *   data.property.address = bridge base URL, e.g. http://volttron:8080
 * Tag address:
 *   tag.address = a VOLTTRON point key "<campus>/<building>/<device>/<point>",
 *   e.g. "campus/building/modbus_sim/temperature". For writes it is split on
 *   the last '/' into (path, point) as set_point expects.
 */

'use strict';
const WebSocket = require('ws');
const axios = require('axios');
const utils = require('../../utils');
const deviceUtils = require('../device-utils');

function VOLTTRONclient(_data, _logger, _events, _runtime) {
    var runtime = _runtime;
    var data = JSON.parse(JSON.stringify(_data));   // Current device data { id, name, tags, property, ... }
    var logger = _logger;
    var events = _events;                           // Events to commit change to runtime
    var self = this;

    var ws = null;                                  // Bridge WebSocket
    var connected = false;
    var connecting = false;
    var working = false;                            // Overloading guard for polling
    var overloading = 0;
    var lastStatus = '';
    var lastTimestampValue = 0;
    var varsValue = {};                             // tagId -> { id, value, type, daq, tagref, timestamp, changed }
    var pointCache = {};                            // VOLTTRON point key -> latest raw value
    var reconnectTimer = null;

    var _baseUrl = function () {
        var addr = (data.property && data.property.address) ? data.property.address : 'http://localhost:8080';
        return addr.replace(/\/+$/, '');
    };
    var _wsUrl = function () {
        return _baseUrl().replace(/^http/, 'ws') + '/ws';
    };

    /**
     * Initialize the device type
     */
    this.init = function (_type) {
        return false;
    };

    /**
     * Connect to the bridge WebSocket and start caching point values.
     * Emits connection status to clients.
     */
    this.connect = function () {
        return new Promise(function (resolve, reject) {
            if (connected || connecting) {
                resolve();
                return;
            }
            if (!data.property || !data.property.address) {
                logger.error(`'${data.name}' missing bridge address`);
                reject('missing-address');
                return;
            }
            connecting = true;
            _clearReconnect();
            try {
                logger.info(`'${data.name}' try to connect ${_baseUrl()}`, true);
                _emitStatus('connect-busy');
                ws = new WebSocket(_wsUrl());
                ws.on('open', function () {
                    connected = true;
                    connecting = false;
                    logger.info(`'${data.name}' connected to bridge`, true);
                    _emitStatus('connect-ok');
                    resolve();
                });
                ws.on('message', function (raw) {
                    try {
                        var msg = JSON.parse(raw.toString());
                        if (msg.type === 'snapshot' && msg.points) {
                            for (var k in msg.points) {
                                pointCache[k] = msg.points[k].value;
                            }
                        } else if (msg.type === 'update' && msg.points) {
                            for (var u in msg.points) {
                                pointCache[u] = msg.points[u];
                            }
                        }
                    } catch (e) {
                        // ignore malformed frame
                    }
                });
                ws.on('error', function (err) {
                    logger.error(`'${data.name}' bridge ws error: ${err}`);
                    if (connecting) {
                        connecting = false;
                        _emitStatus('connect-error');
                        reject(err);
                    }
                });
                ws.on('close', function () {
                    var wasConnected = connected;
                    connected = false;
                    connecting = false;
                    if (wasConnected) {
                        logger.warn(`'${data.name}' bridge connection closed`, true);
                        _emitStatus('connect-off');
                    }
                });
            } catch (err) {
                connecting = false;
                _emitStatus('connect-error');
                reject(err);
            }
        });
    };

    /**
     * Disconnect from the bridge
     */
    this.disconnect = function () {
        return new Promise(function (resolve) {
            _clearReconnect();
            try {
                if (ws) {
                    ws.removeAllListeners();
                    ws.close();
                }
            } catch (e) { /* noop */ }
            ws = null;
            connected = false;
            connecting = false;
            _emitStatus('connect-off');
            resolve(true);
        });
    };

    /**
     * Read values in polling mode: refresh tag values from the point cache,
     * save changed values to DAQ and emit values to clients.
     */
    this.polling = async function () {
        if (_checkWorking(true)) {
            try {
                var changed = await _updateTags();
                lastTimestampValue = new Date().getTime();
                _emitValues(varsValue);
                if (self.addDaq && !utils.isEmptyObject(changed)) {
                    self.addDaq(changed, data.name, data.id);
                }
            } catch (err) {
                logger.error(`'${data.name}' polling error: ${err}`);
            }
            _checkWorking(false);
        }
    };

    var _updateTags = async function () {
        var changed = {};
        var timestamp = new Date().getTime();
        for (var id in data.tags) {
            var tag = data.tags[id];
            var point = tag.address;
            if (point === undefined || point === null || !(point in pointCache)) {
                continue;
            }
            var raw = pointCache[point];
            var old = varsValue[id] ? varsValue[id].value : null;
            var value = await deviceUtils.tagValueCompose(raw, old, tag, runtime);
            var entry = {
                id: id,
                value: value,
                type: tag.type,
                daq: tag.daq,
                tagref: tag,
                timestamp: timestamp,
                changed: false,
            };
            if (old !== value && self.addDaq && deviceUtils.tagDaqToSave(entry, timestamp)) {
                changed[id] = entry;
            }
            varsValue[id] = entry;
        }
        return changed;
    };

    /**
     * Load Tags attribute to read with polling
     */
    this.load = function (_data) {
        data = JSON.parse(JSON.stringify(_data));
        varsValue = {};
        var count = Object.keys(data.tags || {}).length;
        logger.info(`'${data.name}' data loaded (${count})`, true);
    };

    /**
     * Return Tags values map { tagId: { id, value, type, ... } }
     */
    this.getValues = function () {
        return varsValue;
    };

    /**
     * Return Tag value { id, value, ts }
     */
    this.getValue = function (id) {
        if (varsValue[id]) {
            return { id: id, value: varsValue[id].value, ts: lastTimestampValue };
        }
        return null;
    };

    /**
     * Return connection status
     */
    this.getStatus = function () {
        return lastStatus;
    };

    /**
     * Return Tag property to show in frontend
     */
    this.getTagProperty = function (id) {
        if (data.tags[id]) {
            var tag = data.tags[id];
            return { id: id, name: tag.name, type: tag.type, address: tag.address };
        }
        return null;
    };

    /**
     * Set the Tag value to device: write the point through the bridge,
     * which calls platform.driver set_point.
     */
    this.setValue = async function (tagId, value) {
        var tag = data.tags[tagId];
        if (!tag || tag.address === undefined || tag.address === null) {
            return false;
        }
        var point = String(tag.address);
        var idx = point.lastIndexOf('/');
        if (idx <= 0) {
            logger.error(`'${data.name}' setValue: bad point address '${point}'`);
            return false;
        }
        var path = point.substring(0, idx);
        var pointName = point.substring(idx + 1);
        try {
            var rawValue = await deviceUtils.tagRawCalculator(value, tag, runtime);
            await axios.put(_baseUrl() + '/api/points', { path: path, point: pointName, value: rawValue });
            logger.info(`'${data.name}' setValue(${point} = ${rawValue})`, true, true);
            return true;
        } catch (err) {
            logger.error(`'${data.name}' setValue error: ${err}`);
            return false;
        }
    };

    /**
     * Return if device is connected
     */
    this.isConnected = function () {
        return connected;
    };

    /**
     * Bind the DAQ store function
     */
    this.bindAddDaq = function (fnc) {
        self.addDaq = fnc;
    };
    this.addDaq = null;

    /**
     * Return the timestamp of last read tag operation on polling
     */
    this.lastReadTimestamp = function () {
        return lastTimestampValue;
    };

    var _emitStatus = function (status) {
        lastStatus = status;
        events.emit('device-status:changed', { id: data.id, status: status });
    };

    var _emitValues = function (values) {
        events.emit('device-value:changed', { id: data.id, values: values });
    };

    var _clearReconnect = function () {
        if (reconnectTimer) {
            clearTimeout(reconnectTimer);
            reconnectTimer = null;
        }
    };

    /**
     * Guard against overlapping polling cycles (slow bridge / network).
     */
    var _checkWorking = function (check) {
        if (check && working) {
            overloading++;
            if (overloading >= 3) {
                logger.error(`'${data.name}' polling overloading, force reset`);
                working = false;
                overloading = 0;
            }
            return false;
        }
        working = check;
        if (!check) {
            overloading = 0;
        }
        return true;
    };
}

module.exports = {
    init: function (settings) {
    },
    create: function (data, logger, events, manager, runtime) {
        return new VOLTTRONclient(data, logger, events, runtime);
    }
};
