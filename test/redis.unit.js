require('./test')({
  name: 'with driver "redis"',
  redis: require('redis-mock').createClient(),
  redis_ro: require('redis-mock').createClient(),
});
