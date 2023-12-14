import { RedisClientType } from 'redis'
import { RedisCommandArgument, RedisCommandRawReply } from '@redis/client/dist/lib/commands'

import _ from 'lodash'
import DataLoader from 'dataloader'

import stringify from 'json-stable-stringify'

export interface RedisDataLoaderConfig {
  redisRW: RedisClientType<any, any, any>
  redisRO: RedisClientType<any, any, any>
}

export interface RedisDataLoaderOptions extends DataLoader.Options<any, any> {
  expire: number
}

export interface RedisDataLoader {
  options: RedisDataLoaderOptions
  keySpace: string
  loader: DataLoader<any, any>

  load<T>(key: string): Promise<T>

  loadMany<T>(keys: string[]): Promise<T[]>

  prime(key: string, val: object | null | undefined): Promise<void>

  clear(key: string): Promise<DataLoader<any, any, any>>
}

export function createRedisDataLoader(config: RedisDataLoaderConfig) {
  const redisRW = config.redisRW
  const redisRO = config.redisRO

  function parse(resp: RedisCommandRawReply): null | string | object {
    if (resp === null) {
      return null
    } else if (Buffer.isBuffer(resp)) {
      return resp.toString()
    } else if (typeof resp === 'string') {
      return JSON.parse(resp)
    } else {
      return null
    }
  }

  function toString(val: null | object): RedisCommandArgument {
    if (_.isObject(val)) {
      return JSON.stringify(val)
    } else {
      return ''
    }
  }

  function makeKey(
    keySpace: string,
    key: string,
    cacheKeyFn = (k: string | object) => (_.isObject(k) ? stringify(k) : k),
  ) {
    return `${keySpace ? keySpace + ':' : ''}${cacheKeyFn(key)}`
  }

  async function rSetAndGet(keySpace: string, key: string, rawVal: null | object, opt: RedisDataLoaderOptions) {
    const val = toString(rawVal)

    const fullKey = makeKey(keySpace, key, opt.cacheKeyFn)

    const multiRW = redisRW.multi()
    const multiRO = redisRO.multi()

    multiRW.set(fullKey, val)

    if (opt.expire) {
      multiRW.expire(fullKey, opt.expire)
    }

    await multiRW.exec()

    multiRO.get(fullKey)

    const replies = await multiRO.exec()

    const lastReply = _.last(replies)
    return parse(lastReply)
  }

  // const rGet = async (keySpace: string, key: string, opt: RedisDataLoaderOptions) => {
  //   const result = await redisRO.get(makeKey(keySpace, key, opt.cacheKeyFn))
  //   return parse(result)
  // }

  async function rMGet(keySpace: string, keys: readonly string[], opt: RedisDataLoaderOptions) {
    const cacheKeys = _.map(keys, (k) => makeKey(keySpace, k, opt.cacheKeyFn)) as any
    const results = await redisRO.mGet(cacheKeys)
    return results.map((result) => parse(result))
  }

  async function rDel(keySpace: string, key: string, opt: RedisDataLoaderOptions) {
    const cacheKey = makeKey(keySpace, key, opt.cacheKeyFn) as any
    await redisRW.del(cacheKey)
  }

  return class RedisDataLoader {
    options: RedisDataLoaderOptions
    keySpace: string
    loader: DataLoader<any, any>

    constructor(ks: string, userLoader: DataLoader<any, any>, opt: RedisDataLoaderOptions) {
      this.options = opt

      this.options.cacheKeyFn = this.options.cacheKeyFn || ((k) => (_.isObject(k) ? stringify(k) : k))

      this.keySpace = ks

      this.loader = new DataLoader(async (keys: readonly string[]) => {
        const results = await rMGet(this.keySpace, keys, this.options)

        return results.map((result, index) => {
          if (result === '') {
            return Promise.resolve(null)
          } else if (result === null) {
            return userLoader
              .load(keys[index])
              .then((resp) => {
                return rSetAndGet(this.keySpace, keys[index], resp, this.options)
              })
              .then((r) => { return r === '' || _.isUndefined(r) ? null : r })
          } else {
            return Promise.resolve(result)
          }
        })
      }, _.omit(this.options, ['expire', 'buffer']))
    }

    load<T>(key: string): Promise<T> {
      return key ? Promise.resolve(this.loader.load(key)) : Promise.reject(new TypeError('key parameter is required'))
    }

    loadMany<T>(keys: string[]): Promise<T[]> {
      return keys
        ? Promise.resolve(Promise.all(keys.map((k) => this.loader.load(k))))
        : Promise.reject(new TypeError('keys parameter is required'))
    }

    prime(key: string, val: object | null | undefined) {
      if (!key) {
        return Promise.reject(new TypeError('key parameter is required'))
      } else if (val === undefined) {
        return Promise.reject(new TypeError('value parameter is required'))
      } else {
        return rSetAndGet(this.keySpace, key, val, this.options).then((r) => {
          this.loader.clear(key).prime(key, r === '' ? null : r)
        })
      }
    }

    clear(key: string) {
      return key
        ? rDel(this.keySpace, key, this.options).then(() => this.loader.clear(key))
        : Promise.reject(new TypeError('key parameter is required'))
    }

    clearAllLocal() {
      return Promise.resolve(this.loader.clearAll())
    }

    clearLocal(key: string) {
      return Promise.resolve(this.loader.clear(key))
    }
  }
}
