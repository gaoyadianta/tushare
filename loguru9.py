from loguru import logger

# logger.add("debug.log", filter=lambda record: record["level"].name == "DEBUG")
# logger.add("info.log", filter=lambda record: record["level"].name == "INFO")
# logger.add("error.log", filter=lambda record: record["level"].name == "ERROR")

# logger.info('hello')
# logger.debug('debug')

logger.add('普通日志.log', filter=lambda x: '[普通]' in x['message'])
logger.add('警告日志.log', filter=lambda x: '[需要注意]' in x['message']) # 改
logger.add('致命错误.log', filter=lambda x: '[致命]' in x['message']) # 改

logger.info('[普通]我是一条普通日志')
logger.info('[需要注意]xx 写法在下个版本将会移除，请做好迁移')
logger.error('[致命]系统启动失败！')