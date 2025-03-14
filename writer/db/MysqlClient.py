import pymysql

from dbutils.pooled_db import PooledDB  # 新版本的导入路径
from typing import List, Dict, Any, Optional, Union
import threading
import logging

class MysqlClient:
    """MySQL数据库操作类，实现线程安全的单例模式和连接池管理"""
    
    _instance = None
    _lock = threading.Lock()
    _pool = None
    _initialized = False
    
    def __new__(cls, *args, **kwargs):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self, host='localhost', port=3306, user='root', password='', database='', 
                 min_connections=5, max_connections=20, blocking=True, max_usage=0, 
                 set_session=None, ping=1, **kwargs):
        """
        初始化MySQL连接池
        
        参数:
            host: 数据库主机地址
            port: 数据库端口
            user: 数据库用户名
            password: 数据库密码
            database: 数据库名
            min_connections: 最小连接数
            max_connections: 最大连接数
            blocking: 连接池满时是否阻塞等待
            max_usage: 连接最大使用次数，0表示无限制
            set_session: 会话设置
            ping: 是否在使用连接前ping一下服务端
            **kwargs: 其他pymysql参数
        """
        with self._lock:
            if not self._initialized:
                try:
                    self._pool = PooledDB(
                        creator=pymysql,
                        maxconnections=max_connections,
                        mincached=min_connections,
                        maxcached=max_connections,
                        maxshared=max_connections,
                        blocking=blocking,
                        maxusage=max_usage,
                        setsession=set_session,
                        ping=ping,
                        host=host,
                        port=port,
                        user=user,
                        password=password,
                        database=database,
                        charset='utf8mb4',
                        cursorclass=pymysql.cursors.DictCursor,
                        **kwargs
                    )
                    self._initialized = True
                    logging.info(f"MySQL连接池初始化成功: {host}:{port}/{database}")
                except Exception as e:
                    logging.error(f"MySQL连接池初始化失败: {str(e)}")
                    raise
    
    @classmethod
    def get_instance(cls, **kwargs):
        """获取MysqlClient实例的类方法"""
        if cls._instance is None:
            cls(**kwargs)
        return cls._instance
    
    def get_connection(self):
        """获取数据库连接"""
        if not self._pool:
            raise Exception("数据库连接池未初始化")
        return self._pool.connection()
    
    def execute(self, sql: str, params: tuple = None) -> int:
        """
        执行增删改操作
        
        参数:
            sql: SQL语句
            params: SQL参数
            
        返回:
            受影响的行数
        """
        if not self._pool:
            raise Exception("数据库连接池未初始化")
            
        conn = self._pool.connection()
        cursor = conn.cursor()
        try:
            affected_rows = cursor.execute(sql, params)
            conn.commit()
            return affected_rows
        except Exception as e:
            conn.rollback()
            logging.error(f"SQL执行错误: {sql}, 参数: {params}, 错误: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def execute_many(self, sql: str, params_list: List[tuple]) -> int:
        """
        批量执行SQL
        
        参数:
            sql: SQL语句
            params_list: 参数列表
            
        返回:
            受影响的行数
        """
        if not params_list:
            return 0
            
        conn = self._pool.connection()
        cursor = conn.cursor()
        try:
            affected_rows = cursor.executemany(sql, params_list)
            conn.commit()
            return affected_rows
        except Exception as e:
            conn.rollback()
            logging.error(f"批量SQL执行错误: {sql}, 参数数量: {len(params_list)}, 错误: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def query_one(self, sql: str, params: tuple = None) -> Optional[Dict]:
        """
        查询单条记录
        
        参数:
            sql: SQL语句
            params: SQL参数
            
        返回:
            单条记录字典或None
        """
        conn = self._pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetchone()
        except Exception as e:
            logging.error(f"查询错误: {sql}, 参数: {params}, 错误: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def query_all(self, sql: str, params: tuple = None) -> List[Dict]:
        """
        查询多条记录
        
        参数:
            sql: SQL语句
            params: SQL参数
            
        返回:
            记录字典列表
        """
        conn = self._pool.connection()
        cursor = conn.cursor()
        try:
            cursor.execute(sql, params)
            return cursor.fetchall()
        except Exception as e:
            logging.error(f"查询错误: {sql}, 参数: {params}, 错误: {str(e)}")
            raise
        finally:
            cursor.close()
            conn.close()
    
    def query_limit(self, sql: str, params: tuple = None, page: int = 1, page_size: int = 20) -> List[Dict]:
        """
        分页查询
        
        参数:
            sql: SQL语句(不包含LIMIT)
            params: SQL参数
            page: 页码(从1开始)
            page_size: 每页记录数
            
        返回:
            记录字典列表
        """
        if page < 1:
            page = 1
            
        offset = (page - 1) * page_size
        limit_sql = f"{sql} LIMIT {offset}, {page_size}"
        return self.query_all(limit_sql, params)
    
    def count(self, sql: str, params: tuple = None) -> int:
        """
        统计记录数
        
        参数:
            sql: SQL语句
            params: SQL参数
            
        返回:
            记录数
        """
        count_sql = f"SELECT COUNT(*) AS count FROM ({sql}) AS t"
        result = self.query_one(count_sql, params)
        return result.get('count', 0) if result else 0
    
    def insert(self, table: str, data: Dict) -> int:
        """
        插入数据
        
        参数:
            table: 表名
            data: 数据字典
            
        返回:
            受影响的行数
        """
        if not data:
            return 0
            
        fields = ','.join(f"`{k}`" for k in data.keys())
        values = ','.join(['%s'] * len(data))
        sql = f"INSERT INTO `{table}` ({fields}) VALUES ({values})"
        return self.execute(sql, tuple(data.values()))
    
    def insert_many(self, table: str, data_list: List[Dict]) -> int:
        """
        批量插入数据
        
        参数:
            table: 表名
            data_list: 数据字典列表
            
        返回:
            受影响的行数
        """
        if not data_list:
            return 0
            
        # 确保所有字典有相同的键
        keys = data_list[0].keys()
        fields = ','.join(f"`{k}`" for k in keys)
        values = ','.join(['%s'] * len(keys))
        sql = f"INSERT INTO `{table}` ({fields}) VALUES ({values})"
        
        # 准备参数列表
        params_list = [tuple(item.get(k) for k in keys) for item in data_list]
        return self.execute_many(sql, params_list)
    
    def update(self, table: str, data: Dict, condition: str, params: tuple = None) -> int:
        """
        更新数据
        
        参数:
            table: 表名
            data: 更新的数据字典
            condition: WHERE条件
            params: 条件参数
            
        返回:
            受影响的行数
        """
        if not data:
            return 0
            
        set_fields = ','.join([f"`{k}`=%s" for k in data.keys()])
        sql = f"UPDATE `{table}` SET {set_fields} WHERE {condition}"
        all_params = tuple(data.values()) + (params if params else ())
        return self.execute(sql, all_params)
    
    def delete(self, table: str, condition: str, params: tuple = None) -> int:
        """
        删除数据
        
        参数:
            table: 表名
            condition: WHERE条件
            params: 条件参数
            
        返回:
            受影响的行数
        """
        sql = f"DELETE FROM `{table}` WHERE {condition}"
        return self.execute(sql, params)
    
    def transaction(self):
        """
        返回事务上下文管理器
        
        用法:
            with db.transaction() as cursor:
                cursor.execute(sql1, params1)
                cursor.execute(sql2, params2)
        """
        return TransactionContext(self._pool)
    
    def close(self):
        """关闭连接池"""
        if self._pool:
            self._pool.close()
            self._pool = None
            self._initialized = False


class TransactionContext:
    """事务上下文管理器"""
    
    def __init__(self, pool):
        self.pool = pool
        self.conn = None
        self.cursor = None
    
    def __enter__(self):
        self.conn = self.pool.connection()
        self.cursor = self.conn.cursor()
        return self.cursor
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            if exc_type is None:
                self.conn.commit()
            else:
                self.conn.rollback()
                logging.error(f"事务执行错误: {exc_val}")
        finally:
            if self.cursor:
                self.cursor.close()
            if self.conn:
                self.conn.close()
        return False  # 不抑制异常