import logging
import time
import os


class Log:
  @staticmethod
  def initialize_logger():
    logger = logging.getLogger('air_engine_logger')
    logger.setLevel(logging.DEBUG)

    now = time.strftime("%Y%m%d", time.localtime())
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

    # file log handler
    # 获取当前运行目录
    current_dir = os.path.dirname(os.path.abspath(__file__))
    log_file_name = current_dir + f"/logs/engine-{now}.log"
    file_handler = logging.FileHandler(log_file_name)
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    # 控制台日志
    # console log handler
    # console_handler = logging.StreamHandler()
    # console_handler.setLevel(logging.DEBUG)
    # console_handler.setFormatter(formatter)
    # logger.addHandler(console_handler)

    return logger

  @staticmethod
  def debug(message):
    logger = logging.getLogger('air_engine_logger')
    if not logger.handlers:
      logger = Log.initialize_logger()
    logger.debug(message)

  @staticmethod
  def error(message):
    logger = logging.getLogger('air_engine_logger')
    if not logger.handlers:
      logger = Log.initialize_logger()
    logger.error(message)

  @staticmethod
  def warn(message):
    logger = logging.getLogger('air_engine_logger')
    if not logger.handlers:
      logger = Log.initialize_logger()
    logger.warning(message)

  @staticmethod
  def info(message):
    logger = logging.getLogger('air_engine_logger')
    if not logger.handlers:
      logger = Log.initialize_logger()
    logger.info(message)