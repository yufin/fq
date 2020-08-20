# fq
 Quant framework
 超级轻量级本地量化框架
 
 受精卵状态，发育中
 
 结构:
 data_module中SqlApi对数据库操作进行封装，DataApi对SqlApi进行封装
 backtesting_module.backtesting_frame中的TradesRecorder主要记录交易计算净值
 SimAgent针对不同市场进行交易规则与环境的模拟.
 
 后续
 1.因子生成脚本
 2.深度学习算法应用模块
 3.回测流程脚本
 4.规范代码
 5.还没想好
 
 
 setup
 1.首先打开data_module.data_update,创建数据库村粗数据
 
 2.设置数据库索引 run data_module.data_api.SqlApi.shell运行sql命令设置索引，
 推荐设置两个单独索引 on code, on date.
 
 3.还没想好
