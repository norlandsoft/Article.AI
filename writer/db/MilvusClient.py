# Milvus数据库客户端
import json
from pymilvus import (
  connections,
  FieldSchema,
  CollectionSchema,
  DataType,
  Collection,
  MilvusException,
  utility,
)
from pymilvus.client.types import LoadState
from writer.utils.Log import Log

import time
import threading


class CollectionInfo:
  def __init__(self, id: str, status="A", loaded=False):
    self.id = id
    self.status = status
    self.loaded = loaded


class MilvusClient:
  def __init__(self):
    self.host = "localhost"
    self.port = "19530"
    self.db_name = "default"
    self.pool_size = 10
    self.connection_pool = []
    self.lock = threading.Lock()
    self.init_connection_pool()

  def init_connection_pool(self):
    for idx in range(self.pool_size):
      try:
        alias_id = f"CONN_{idx}"
        if connections.has_connection(alias_id):
          connections.disconnect(alias_id)
        if alias_id in self.connection_pool:
          self.connection_pool.append(alias_id)
        connections.connect(
          alias=alias_id, db_name=self.db_name, host=self.host, port=self.port
        )
        self.connection_pool.append(alias_id)

      except Exception as e:
        Log.error(f"Failed to connect to Milvus: {e}")
        time.sleep(1)

  def get_connection(self):
    with self.lock:
      if not self.connection_pool:
        self.init_connection_pool()

      conn_alias = self.connection_pool.pop()

      # 检查连接是否可用
      if not self.check_connection(conn_alias):
        self.reconnect(conn_alias)

      return conn_alias

  def check_connection(self, conn_alias):
    try:
      # 尝试执行一个简单的操作来检查连接
      utility.list_collections(using=conn_alias)
      return True
    except Exception:
      return False

  def reconnect(self, conn_alias):
    try:
      connections.disconnect(conn_alias)
      connections.connect(
        alias=conn_alias, host=self.host, port=self.port, db_name=self.db_name
      )
    except Exception as e:
      Log.error(f"重新连接到 Milvus 失败: {e}")
      time.sleep(1)

  def execute_with_retry(self, func, *args, **kwargs):
    conn_alias = self.get_connection()
    try:
      return func(conn_alias, *args, **kwargs)
    except Exception as e:
      Log.error(f"执行操作时发生错误: {e}")
      self.reconnect(conn_alias)
      # 重试一次
      return func(conn_alias, *args, **kwargs)
    finally:
      self.release_connection(conn_alias)

  def create_collection(self, name: str) -> bool:
    """
    创建collection，仅适合本项目使用
    :param name:
    :return:
    """
    conn_alias = self.get_connection()
    try:
      fields_schema = [
        FieldSchema(
          name="id", dtype=DataType.INT64, is_primary=True, auto_id=True
        ),
        FieldSchema(name="file_id", dtype=DataType.VARCHAR, max_length=32),
        FieldSchema(name="file_name", dtype=DataType.VARCHAR, max_length=256),
        FieldSchema(name="group_id", dtype=DataType.VARCHAR, max_length=32),
        FieldSchema(
          name="txt_content", dtype=DataType.VARCHAR, max_length=2048
        ),
        FieldSchema(name="vec_content", dtype=DataType.FLOAT_VECTOR, dim=1024),
      ]
      schema = CollectionSchema(fields=fields_schema)
      collection = Collection(name=name, schema=schema, using=conn_alias)
      # 创建索引
      params = {
        "index_type": "IVF_FLAT",
        "metric_type": "L2",
        "params": {"nlist": 128},
      }
      collection.create_index("vec_content", index_params=params)

      return True
    except MilvusException as me:
      self.reconnect(conn_alias)
      return False
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def __collection_info(self, conn_alias, collection_name):
    """
    获取collection信息, 供 collection_info 和 collection_list 调用
    :return:
    """
    try:
      # collection = Collection(name=collection_name, using=conn_alias)

      # 获取加载状态
      load_state = utility.load_state(
        collection_name=collection_name, using=conn_alias
      )
      if load_state == LoadState.Loaded:
        loaded = True
      else:
        loaded = False

      info = CollectionInfo(id=collection_name, status="A", loaded=loaded)

      return info
    except MilvusException as me:
      raise me
    except Exception as e:
      raise e

  def collection_info(self, collection_name):
    """
    获取Collection信息
    :param collection_name:
    :return:
    """
    conn_alias = self.get_connection()

    # 检查collection是否存在
    has_collection = utility.has_collection(
      collection_name=collection_name, using=conn_alias
    )
    if not has_collection:
      return CollectionInfo(id=collection_name, status="N", loaded=False)

    try:
      return self.__collection_info(conn_alias, collection_name)
    except MilvusException:
      return CollectionInfo(id=collection_name, status="N", loaded=False)
    except Exception as e:
      self.reconnect(conn_alias)
      return CollectionInfo(id=collection_name, status="N", loaded=False)
    finally:
      self.release_connection(conn_alias)

  def collection_list(self) -> list:
    """
    获取Collection列表
    :return:
    """
    conn_alias = self.get_connection()
    try:
      name_list = utility.list_collections(using=conn_alias)
      info_list = []
      # get info
      for name in name_list:
        info = self.__collection_info(conn_alias, name)
        info_list.append(info)

      return info_list
    except Exception as e:
      self.reconnect(conn_alias)
      Log.error(e)
      return []
    finally:
      self.release_connection(conn_alias)

  def is_loaded(self, collection_name):
    conn_alias = self.get_connection()
    try:
      state = utility.load_state(collection_name, using=conn_alias)
      if state == LoadState.Loaded:
        return True
      return False
    except MilvusException:
      self.reconnect(conn_alias)
      return False
    finally:
      self.release_connection(conn_alias)

  def insert(self, collection_name, data, partition_name=None):
    """
    插入数据
    如果指定了 partition_name，数据将被插入到该分区。如果没有指定分区，数据将被插入到默认分区。
    插入数据后，我们调用 load 方法来加载集合，以便可以对其进行搜索。
    确保 data 参数是一个列表，其中包含要插入的向量。每个向量应该是一个 NumPy 数组。
    如果数据已经是这种格式，那么可以按照上面的代码进行操作。如果不是，需要对数据进行预处理以匹配所需的格式。
    """
    conn_alias = self.get_connection()
    try:
      collection = Collection(name=collection_name, using=conn_alias)
      if partition_name:
        # 如果分区不存在，则创建它
        if not collection.has_partition(partition_name):
          collection.create_partition(partition_name)
        # 插入数据到指定分区
        collection.insert(data, partition_name=partition_name)
      else:
        # 插入数据到默认分区
        collection.insert(data)
      collection.flush()
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def insert_all(self, collection_name, data_all, partition_name=None):
    """
    批量插入数据
    """
    conn_alias = self.get_connection()
    try:
      collection = Collection(name=collection_name, using=conn_alias)
      if partition_name:
        # 如果分区不存在,则创建它
        if not collection.has_partition(partition_name):
          collection.create_partition(partition_name)
        # 插入数据到指定分区
        for data in data_all:
          collection.insert(data, partition_name=partition_name)
      else:
        # 插入数据到默认分区
        for data in data_all:
          collection.insert(data)
      collection.flush()
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def load_collection(self, collection_name):
    """
    加载集合，以便可以对其进行搜索
    """
    conn_alias = self.get_connection()
    try:
      collection = Collection(name=collection_name, using=conn_alias)
      collection.load()
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def release_collection(self, collection_name):
    """
    释放集合，以便可以将其删除
    """
    conn_alias = self.get_connection()
    try:
      collection = Collection(name=collection_name, using=conn_alias)
      collection.release()
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def search(
      self, collection_name, query_vectors, limit, param=None, partition_names=None
  ):
    """
    知识检索
    query_vectors 是一个包含查询向量的列表，limit 是一个整数，表示每个查询返回的最相似的向量数量。
    params 是一个字典，可以包含额外的搜索参数，如 nprobe。
    partition_names 是一个列表，可以包含要搜索的分区的名称。
    """
    if param is None:
      param = {"metric_type": "L2", "nprobe": 16}
    conn_alias = self.get_connection()
    try:
      collection = Collection(name=collection_name, using=conn_alias)
      results = collection.search(
        query_vectors,
        anns_field="vec_content",
        limit=limit,
        param=param,
        partition_names=partition_names,
        output_fields=["file_name", "txt_content"],
      )

      # 返回结果中 txt_content 字段列表
      # return results
      return [hit.txt_content for hits in results for hit in hits]
    except MilvusException as me:
      if me.code == 1100:
        return []
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def delete_collection(self, collection_name, expr=None, partition_name=None):
    """
    删除数据
    expr 是一个字符串，表示一个条件表达式，用于指定要删除的行。如果指定了 partition_name，删除操作将只在该分区中进行
    请确保您的 expr 参数是一个有效的条件表达式，例如 "age > 30", 如果 expr 为空字符串，则删除所有数据
    如需要执行精确匹配的删除，可以使用 TermExpr 类来构造表达式，但这通常用于更复杂的查询场景
    如果指定了 partition_name，删除操作将只在该分区中进行。
    """
    conn_alias = self.get_connection()
    try:
      collection = Collection(name=collection_name, using=conn_alias)
      if expr is None:
        expr = "id >= 0"  # 一个始终为真的表达式，会删除所有数据
      if partition_name:
        collection.delete(expr, partition_name=partition_name)
      else:
        collection.delete(expr)
    except Exception as e:
      self.reconnect(conn_alias)
      raise e
    finally:
      self.release_connection(conn_alias)

  def release_connection(self, conn_alias):
    with self.lock:
      if conn_alias not in self.connection_pool:
        self.connection_pool.append(conn_alias)