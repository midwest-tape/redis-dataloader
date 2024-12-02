import { RedisClientType } from 'redis'
import { RedisCommandArgument, RedisCommandRawReply } from '@redis/client/dist/lib/commands'

import _ from 'lodash'
import DataLoader from 'dataloader'

import stringify from 'json-stable-stringify'

import debug from 'debug'

const d = debug('redis-dataloader')

export interface RedisDataLoaderConfig {
  redisRW: RedisClientType<any, any, any>
  redisRO: RedisClientType<any, any, any>
}

export interface RedisDataLoaderOptions extends DataLoader.Options<any, any> {
  expire: number
}

function getErrorMessage(error: unknown) {
  if (error instanceof Error) return error.message
  return String(error)
}

export interface IRedisDataLoader {
  options?: RedisDataLoaderOptions
  keySpace: string
  loader: DataLoader<any, any>

  load<T>(key: string): Promise<T>

  loadMany<T>(keys: string[]): Promise<T[]>

  prime(key: string, val: object | null | undefined): Promise<void>

  clear(key: string): Promise<DataLoader<any, any, any>>

  clearAllLocal(): Promise<DataLoader<any, any, any>>;

  clearLocal(key: string): Promise<DataLoader<any, any, any>>;
}

export function createRedisDataLoader(config: RedisDataLoaderConfig) {
  const redisRW = config.redisRW
  const redisRO = config.redisRO

  function isReplicaLoadingDataError(exception: unknown) {
    const errorMessage = getErrorMessage(exception)
    return errorMessage.includes('LOADING')
  }

  function parse(resp: RedisCommandRawReply): null | string | object {
    if (resp === '' || resp === null) {
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

    multiRW.set(fullKey, val)

    if (opt.expire) {
      multiRW.expire(fullKey, opt.expire)
    }

    await multiRW.exec()

    try {
      const multiRO = redisRO.multi()
      multiRO.get(fullKey)
      const replies = await multiRO.exec()
      const lastReply: string | number | Buffer | Array<RedisCommandRawReply> | undefined | null = _.last(replies)
      return parse(lastReply)
    } catch (ex) {
      if (isReplicaLoadingDataError(ex)) {
        // this replica is reloading from disc and not ready for work. retry
        // loading these keys from the primary instead.
        multiRW.get(fullKey)
        const replies = await multiRW.exec()
        const lastReply: string | number | Buffer | Array<RedisCommandRawReply> | undefined | null = _.last(replies)
        return parse(lastReply)
      }
      throw ex
    }
  }

  function rPipelineSet(keySpace: string, data: {
    key: string,
    rawVal: null | object
  }[], opt: RedisDataLoaderOptions) {
    const multiRW = redisRW.multi()

    for (let i = 0; i < data.length; i++) {
      const item = data[i]

      const val = toString(item.rawVal)

      const fullKey = makeKey(keySpace, item.key, opt.cacheKeyFn)


      if (opt.expire) {
        d('setting redis data', fullKey, `for ${opt.expire}ms`)
        multiRW.set(fullKey, val, { EX: opt.expire })
      } else {
        d('setting redis data', fullKey)
        multiRW.set(fullKey, val)
      }
    }

    return multiRW.execAsPipeline()
  }

  // const rGet = async (keySpace: string, key: string, opt: RedisDataLoaderOptions) => {
  //   const result = await redisRO.get(makeKey(keySpace, key, opt.cacheKeyFn))
  //   return parse(result)
  // }

  async function rMGet(keySpace: string, keys: readonly string[], opt: RedisDataLoaderOptions) {
    const cacheKeys = _.map(keys, (k) => makeKey(keySpace, k, opt.cacheKeyFn)) as any

    try {
      const results = await redisRO.mGet(cacheKeys)
      return results.map((result) => parse(result))
    } catch (ex) {
      if (isReplicaLoadingDataError(ex)) {
        // this replica is reloading from disc and not ready for work. retry
        // loading these keys from the primary instead.
        const results = await redisRW.mGet(cacheKeys)
        return results.map((result) => parse(result))
      }
      throw ex
    }
  }

  async function rDel(keySpace: string, key: string, opt: RedisDataLoaderOptions) {
    const cacheKey = makeKey(keySpace, key, opt.cacheKeyFn) as any
    await redisRW.del(cacheKey)
  }

  return class RedisDataLoader implements IRedisDataLoader {
    options: RedisDataLoaderOptions
    keySpace: string
    loader: DataLoader<any, any>

    constructor(ks: string, userLoader: DataLoader<any, any>, opt?: RedisDataLoaderOptions) {
      this.options = opt || {} as RedisDataLoaderOptions

      this.options.cacheKeyFn = this.options.cacheKeyFn || ((k) => (_.isObject(k) ? stringify(k) : k))

      this.keySpace = ks

      this.loader = new DataLoader(async (keys: readonly string[]) => {
        const results = await rMGet(this.keySpace, keys, this.options)

        const dataToStore: { key: string; rawVal: null | object }[] = []

        const fetches: Promise<any>[] = []

        for (let index = 0; index < results.length; index++) {
          const result = results[index]

          if (result === '') {
            d('found -NULL- in redis', keys[index])
            fetches.push(Promise.resolve(null))
          } else if (result === null) {
            fetches.push(userLoader
              .load(keys[index])
              .then((resp) => {
                d('found in user loader', keys[index])
                dataToStore.push({ key: keys[index], rawVal: resp })
                return resp
              })
              .then((r) => {
                return Promise.resolve(r === '' || _.isUndefined(r) ? null : r)
              }))
          } else {
            d('found in redis', keys[index])
            fetches.push(Promise.resolve(result))
          }
        }

        const response = await Promise.all(fetches)

        if (dataToStore.length > 0) {
          // set all data in redis at once without waiting for response from redis
          rPipelineSet(this.keySpace, dataToStore, this.options).catch((reason) => {
            // we are catching and not throwing the failure
            // because we don't want downstream services to
            // fail if redis does
            d('redis pipeline setting failed', reason)
          })
        }

        return response
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
