"use strict";
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.createRedisDataLoader = void 0;
const lodash_1 = __importDefault(require("lodash"));
const dataloader_1 = __importDefault(require("dataloader"));
const json_stable_stringify_1 = __importDefault(require("json-stable-stringify"));
function getErrorMessage(error) {
    if (error instanceof Error)
        return error.message;
    return String(error);
}
function createRedisDataLoader(config) {
    const redisRW = config.redisRW;
    const redisRO = config.redisRO;
    function isReplicaLoadingDataError(exception) {
        const errorMessage = getErrorMessage(exception);
        return errorMessage.includes('LOADING');
    }
    function parse(resp) {
        if (resp === '' || resp === null) {
            return null;
        }
        else if (Buffer.isBuffer(resp)) {
            return resp.toString();
        }
        else if (typeof resp === 'string') {
            return JSON.parse(resp);
        }
        else {
            return null;
        }
    }
    function toString(val) {
        if (lodash_1.default.isObject(val)) {
            return JSON.stringify(val);
        }
        else {
            return '';
        }
    }
    function makeKey(keySpace, key, cacheKeyFn = (k) => (lodash_1.default.isObject(k) ? (0, json_stable_stringify_1.default)(k) : k)) {
        return `${keySpace ? keySpace + ':' : ''}${cacheKeyFn(key)}`;
    }
    async function rSetAndGet(keySpace, key, rawVal, opt) {
        const val = toString(rawVal);
        const fullKey = makeKey(keySpace, key, opt.cacheKeyFn);
        const multiRW = redisRW.multi();
        multiRW.set(fullKey, val);
        if (opt.expire) {
            multiRW.expire(fullKey, opt.expire);
        }
        await multiRW.exec();
        // try {
        //   const multiRO = redisRO.multi()
        //   multiRO.get(fullKey)
        //   const replies = await multiRO.exec()
        //   const lastReply: string | number | Buffer | Array<RedisCommandRawReply> | undefined | null = _.last(replies)
        //   return parse(lastReply)
        // } catch (ex) {
        //   if (isReplicaLoadingDataError(ex)) {
        //     // this replica is reloading from disc and not ready for work. retry
        //     // loading these keys from the primary instead.
        //     multiRW.get(fullKey)
        //     const replies = await multiRW.exec()
        //     const lastReply: string | number | Buffer | Array<RedisCommandRawReply> | undefined | null = _.last(replies)
        //     return parse(lastReply)
        //   }
        //   throw ex
        // }
        return parse(val);
    }
    // const rGet = async (keySpace: string, key: string, opt: RedisDataLoaderOptions) => {
    //   const result = await redisRO.get(makeKey(keySpace, key, opt.cacheKeyFn))
    //   return parse(result)
    // }
    async function rMGet(keySpace, keys, opt) {
        const cacheKeys = lodash_1.default.map(keys, (k) => makeKey(keySpace, k, opt.cacheKeyFn));
        try {
            const results = await redisRO.mGet(cacheKeys);
            return results.map((result) => parse(result));
        }
        catch (ex) {
            if (isReplicaLoadingDataError(ex)) {
                // this replica is reloading from disc and not ready for work. retry
                // loading these keys from the primary instead.
                const results = await redisRW.mGet(cacheKeys);
                return results.map((result) => parse(result));
            }
            throw ex;
        }
    }
    async function rDel(keySpace, key, opt) {
        const cacheKey = makeKey(keySpace, key, opt.cacheKeyFn);
        await redisRW.del(cacheKey);
    }
    return class RedisDataLoader {
        constructor(ks, userLoader, opt) {
            this.options = opt || {};
            this.options.cacheKeyFn = this.options.cacheKeyFn || ((k) => (lodash_1.default.isObject(k) ? (0, json_stable_stringify_1.default)(k) : k));
            this.keySpace = ks;
            this.loader = new dataloader_1.default(async (keys) => {
                const results = await rMGet(this.keySpace, keys, this.options);
                return results.map((result, index) => {
                    if (result === '') {
                        return Promise.resolve(null);
                    }
                    else if (result === null) {
                        return userLoader
                            .load(keys[index])
                            .then((resp) => {
                            return rSetAndGet(this.keySpace, keys[index], resp, this.options);
                        })
                            .then((r) => {
                            return Promise.resolve(r === '' || lodash_1.default.isUndefined(r) ? null : r);
                        });
                    }
                    else {
                        return Promise.resolve(result);
                    }
                });
            }, lodash_1.default.omit(this.options, ['expire', 'buffer']));
        }
        load(key) {
            return key ? Promise.resolve(this.loader.load(key)) : Promise.reject(new TypeError('key parameter is required'));
        }
        loadMany(keys) {
            return keys
                ? Promise.resolve(Promise.all(keys.map((k) => this.loader.load(k))))
                : Promise.reject(new TypeError('keys parameter is required'));
        }
        prime(key, val) {
            if (!key) {
                return Promise.reject(new TypeError('key parameter is required'));
            }
            else if (val === undefined) {
                return Promise.reject(new TypeError('value parameter is required'));
            }
            else {
                return rSetAndGet(this.keySpace, key, val, this.options).then((r) => {
                    this.loader.clear(key).prime(key, r === '' ? null : r);
                });
            }
        }
        clear(key) {
            return key
                ? rDel(this.keySpace, key, this.options).then(() => this.loader.clear(key))
                : Promise.reject(new TypeError('key parameter is required'));
        }
        clearAllLocal() {
            return Promise.resolve(this.loader.clearAll());
        }
        clearLocal(key) {
            return Promise.resolve(this.loader.clear(key));
        }
    };
}
exports.createRedisDataLoader = createRedisDataLoader;
