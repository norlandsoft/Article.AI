# Redis 客户端
import json
import redis
import threading
import time
from redis.exceptions import ConnectionError, TimeoutError

from writer.utils.Log import Log

class RedisClient:
  _instance = None
  _lock = threading.Lock()

  def __new__(cls):
    if cls._instance is None:
      with cls._lock:
        if cls._instance is None:
          cls._instance = super().__new__(cls)
          cls._instance.config = {
            'max_connections': 10,
            'socket_timeout': 5,
            'socket_connect_timeout': 5,
            'retry_on_timeout': True,
            'socket_keepalive': True,
            'health_check_interval': 30
          }
          cls._instance.connection = None
          cls._instance.pool = None
          cls._instance.max_retries = 3
          cls._instance.retry_delay = 1
          cls._instance.connect()
    return cls._instance

  def __init__(self):
    pass

  def connect(self):
    try:
      # redis_config = GlobalConfig().get('redis_config')
      redis_config = {
        'host': '127.0.0.1',
        'port': 6379,
        'password': '',
        'db': 0
      }
      if redis_config:
        redis_config = json.loads(redis_config.decode('utf-8'))
        valid_config = {
          k: v for k, v in redis_config.items()
          if k in [
            'host', 'port', 'password', 'db',
            'max_connections', 'socket_timeout',
            'socket_connect_timeout', 'retry_on_timeout',
            'socket_keepalive', 'health_check_interval'
          ]
        }
        if 'connection_timeout' in redis_config:
          valid_config['socket_connect_timeout'] = redis_config['connection_timeout']

        self.config.update(valid_config)

      pool_kwargs = {
        'host': self.config.get('host', '127.0.0.1'),
        'port': self.config.get('port', 6379),
        'db': self.config.get('db', 0),
        'max_connections': self.config.get('max_connections', 10),
        'socket_timeout': self.config.get('socket_timeout', 5),
        'socket_connect_timeout': self.config.get('socket_connect_timeout', 5),
        'retry_on_timeout': self.config.get('retry_on_timeout', True),
        'socket_keepalive': self.config.get('socket_keepalive', True),
        'health_check_interval': self.config.get('health_check_interval', 30)
      }

      if 'password' in self.config and self.config['password']:
        pool_kwargs['password'] = self.config['password']

      self.pool = redis.ConnectionPool(**pool_kwargs)
      self.connection = redis.Redis(connection_pool=self.pool)
      self.connection.ping()
      Log.info("Redis 连接成功")
    except Exception as e:
      Log.error(f"Redis连接失败: {e}")
      raise

  def execute_with_retry(self, func, *args, **kwargs):
    for attempt in range(self.max_retries):
      try:
        return func(*args, **kwargs)
      except (ConnectionError, TimeoutError) as e:
        if attempt == self.max_retries - 1:
          Log.error(f"操作失败，已重试{self.max_retries}次: {e}")
          raise
        time.sleep(self.retry_delay)
        self.connect()

  def reconnect_if_needed(self):
    """如果连接断开，尝试重新连接"""
    try:
      # 测试连接是否有效
      self.connection.ping()
    except (ConnectionError, TimeoutError):
      Log.error("获取连接失败, 重新连接...")
      self.connect()

  def get(self, key):
    """
    获取键值，自动将bytes解码为str
    """
    try:
      value = self.execute_with_retry(lambda: self.connection.get(key))
      # 如果值存在且是bytes类型，则解码为str
      if value and isinstance(value, bytes):
        return value.decode('utf-8')
      return value
    except Exception as e:
      Log.error(f"get key:{key} error - {e}")
      raise

  def set(self, key, value):
    """
    设置键值，确保value是str类型
    """
    try:
      self.reconnect_if_needed()
      value = str(value) if value is not None else ""
      return self.connection.set(key, value)
    except Exception as e:
      Log.error(f"set key:{key} value:{value} error - {e}")
      raise

  def set_with_expire(self, key, value, expire):
    """
    设置键值并设置过期时间，确保value是str类型
    """
    try:
      self.reconnect_if_needed()
      value = str(value) if value is not None else ""
      return self.connection.setex(key, expire, value)
    except Exception as e:
      Log.error(f"setex key:{key} value:{value} error - {e}")
      raise

  def delete(self, key):
    self.reconnect_if_needed()
    return self.connection.delete(key)

  def delete_all(self, prefix):
    self.reconnect_if_needed()
    pattern = f'{prefix}*'
    # 收集所有匹配的键
    keys_to_delete = list(self.connection.scan_iter(match=pattern))
    # 一次性删除所有匹配的键
    if keys_to_delete:
      self.connection.delete(*keys_to_delete)

  def exists(self, key):
    self.reconnect_if_needed()
    return self.connection.exists(key)

  # lpush
  def push(self, queue, value):
    """
    推送数据到队列，确保value是str类型
    """
    try:
      self.reconnect_if_needed()
      value = str(value) if value is not None else ""
      return self.connection.lpush(queue, value)
    except Exception as e:
      Log.error(f"push queue:{queue} value:{value} error - {e}")
      raise

  # rpop
  def fetch(self, key):
    """
    从队列获取数据，自动将bytes解码为str
    """
    try:
      value = self.execute_with_retry(lambda: self.connection.rpop(key))
      if value and isinstance(value, bytes):
        return value.decode('utf-8')
      return value
    except Exception as e:
      Log.error(f"fetch queue:{key} error - {e}")
      raise

  def disconnect(self):
    """断开与Redis的连接"""
    try:
      if self.connection:
        self.connection.close()
        self.connection = None
    finally:
      if self.pool:
        self.pool.disconnect()
        self.pool = None

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_val, exc_tb):
    self.disconnect()

  def __del__(self):
    """析构函数，确保关闭连接"""
    self.disconnect()