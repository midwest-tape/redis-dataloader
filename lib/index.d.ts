import { RedisClientType } from 'redis';
import DataLoader from 'dataloader';
export interface RedisDataLoaderConfig {
    redisRW: RedisClientType<any, any, any>;
    redisRO: RedisClientType<any, any, any>;
}
export interface RedisDataLoaderOptions extends DataLoader.Options<any, any> {
    expire: number;
}
export interface RedisDataLoader {
    options: RedisDataLoaderOptions;
    keySpace: string;
    loader: DataLoader<any, any>;
    load<T>(key: string): Promise<T>;
    loadMany<T>(keys: string[]): Promise<T[]>;
    prime(key: string, val: object | null | undefined): Promise<void>;
    clear(key: string): Promise<DataLoader<any, any, any>>;
}
export declare function createRedisDataLoader(config: RedisDataLoaderConfig): {
    new (ks: string, userLoader: DataLoader<any, any>, opt: RedisDataLoaderOptions): {
        options: RedisDataLoaderOptions;
        keySpace: string;
        loader: DataLoader<any, any>;
        load<T>(key: string): Promise<T>;
        loadMany<T_1>(keys: string[]): Promise<T_1[]>;
        prime(key: string, val: object | null | undefined): Promise<void>;
        clear(key: string): Promise<DataLoader<any, any, any>>;
        clearAllLocal(): Promise<DataLoader<any, any, any>>;
        clearLocal(key: string): Promise<DataLoader<any, any, any>>;
    };
};
