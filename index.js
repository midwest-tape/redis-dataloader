const _ = require('lodash');
const DataLoader = require('dataloader');
const stringify = require('json-stable-stringify');

const mapPromise = (promise, fn) => Promise.all(promise.map(fn));

module.exports = fig => {
  const redis = fig.redis;
  const redis_ro = fig.redis_ro;
  const isIORedis = redis_ro.constructor.name !== 'RedisClient';

  const parse = async (resp, opt) => {
      try {
        if (resp === '' || resp === null) {
          return(resp);
        } else if (opt.deserialize) {
          return opt.deserialize(resp);
        } else {
          if (Buffer.isBuffer(resp)) {
            resp = resp.toString();
          }
          return JSON.parse(resp);
        }
      } catch (err) {
        throw new Error(err);
      }
    };

  const toString = async (val, opt) => {
    if (val === null) {
      return '';
    } else if (opt.serialize) {
      return opt.serialize(val);
    } else if (_.isObject(val)) {
      return JSON.stringify(val);
    } else {
      throw new Error('Must be Object or Null');
    }
  };

  const makeKey = (keySpace, key, cacheKeyFn) =>
    `${keySpace ? keySpace + ':' : ''}${cacheKeyFn(key)}`;

  const rSetAndGet = async (keySpace, key, rawVal, opt) => {
      const val = await toString(rawVal, opt);

      const fullKey = makeKey(keySpace, key, opt.cacheKeyFn);
      redis.set(fullKey, val);
      const multi = redis_ro.multi();
      if (opt.expire) {
        multi.expire(fullKey, opt.expire);
      }
      if (opt.buffer) {
        multi.getBuffer(fullKey);
      } else {
        multi.get(fullKey);
      }

      return await new Promise((resolve, reject) => {
          multi.exec(async (err, replies) => {
          if(err) {
            reject(err);
          }

          const lastReply = isIORedis
            ? _.last(_.last(replies))
            : _.last(replies);

          const parsedValue = await parse(lastReply, opt)
          resolve(parsedValue);
        });
      });
  }

  const rGet = async (keySpace, key, opt) =>
      (opt.buffer ? redis_ro.getBuffer : redis_ro.get)(
        makeKey(keySpace, key, opt.cacheKeyFn),
        async (err, result) => {
          if(err) {
            throw new Error();
          }
          await parse(result, opt);
        }
      );

  const rMGet = (keySpace, keys, opt) => {
    if (opt.buffer) {
      // Have to use multi.getBuffer instead of mgetBuffer
      // because mgetBuffer throws an error.
      return new Promise((resolve, reject) => {
        let multi = redis_ro.pipeline();
        for (const key of keys) {
          multi = multi.getBuffer(makeKey(keySpace, key, opt.cacheKeyFn));
        }
        multi = multi.exec((err, replies) => {
          return err
            ? reject(err)
            // [1] because it's an array where 0 = key, 1 = value.
            : mapPromise(replies, r => parse(r[1], opt)).then(resolve);
        });
      });
    } else {
      return new Promise((resolve, reject) =>
        redis.mget(
          _.map(keys, k => makeKey(keySpace, k, opt.cacheKeyFn)),
          (err, results) => {
            return err
              ? reject(err)
              : mapPromise(results, r => parse(r, opt)).then(resolve);
          }
        )
      );
    }
  }

  const rDel = async (keySpace, key, opt) =>
    redis.del(
      makeKey(keySpace, key, opt.cacheKeyFn),
      async (err, resp) => {
        if(err) {
          throw new Error();
        }
       return resp;
      }
    );

  return class RedisDataLoader {
    constructor(ks, userLoader, opt) {
      const customOptions = [
        'expire',
        'serialize',
        'deserialize',
        'cacheKeyFn',
        'buffer'
      ];
      this.opt = _.pick(opt, customOptions) || {};
      this.opt.cacheKeyFn =
        this.opt.cacheKeyFn || (k => (_.isObject(k) ? stringify(k) : k));
      if (this.opt.buffer && !isIORedis) {
        throw new Error('opt.buffer can only be used with ioredis');
      }
      this.keySpace = ks;
      this.loader = new DataLoader(
        keys =>
          rMGet(this.keySpace, keys, this.opt).then(results =>
            mapPromise(results, (v, i) => {
              if (v === '') {
                return Promise.resolve(null);
              } else if (v === null) {
                return userLoader
                  .load(keys[i])
                  .then(resp =>
                    rSetAndGet(this.keySpace, keys[i], resp, this.opt)
                  )
                  .then(r => (r === '' ? null : r));
              } else {
                return Promise.resolve(v);
              }
            })
          ),
        _.chain(opt)
          .omit(customOptions)
          .extend({ cacheKeyFn: this.opt.cacheKeyFn })
          .value()
      );
    }

    load(key) {
      return key
        ? Promise.resolve(this.loader.load(key))
        : Promise.reject(new TypeError('key parameter is required'));
    }

    loadMany(keys) {
      return keys
        ? Promise.resolve(Promise.all(keys.map((k) => this.loader.load(k))))
        : Promise.reject(new TypeError('keys parameter is required'));
    }

    prime(key, val) {
      if (!key) {
        return Promise.reject(new TypeError('key parameter is required'));
      } else if (val === undefined) {
        return Promise.reject(new TypeError('value parameter is required'));
      } else {
        return rSetAndGet(this.keySpace, key, val, this.opt).then(r => {
          this.loader.clear(key).prime(key, r === '' ? null : r);
        });
      }
    }

    clear(key) {
      return key
        ? rDel(this.keySpace, key, this.opt).then(() => this.loader.clear(key))
        : Promise.reject(new TypeError('key parameter is required'));
    }

    clearAllLocal() {
      return Promise.resolve(this.loader.clearAll());
    }

    clearLocal(key) {
      return Promise.resolve(this.loader.clear(key));
    }
  };
};
