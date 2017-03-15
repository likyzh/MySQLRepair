### MySQLRepair 解决什么问题 ###
    
在MySQL主从复制的架构下经常会遇到主从数据不一致的问题，那么一班解决这类问题的办法一班是三类：
    
*重建从节点 、
修复不一致的数据 、
直接跳过错误*
    
那么如果数据库不大的情况下我们一般是直接重建从节点解决，对于不敏感的数据有时候我们也会跳过，但这总不是很完美的方法，重建会带来性能开销，直接跳过则会带来数据不一致的风险，那么有没有直接修复不一致的数据呢，MySQLRepair应运而生。
 

### MySQLRepair 原理 ###
MySQLRepair要求Slave的Binlog的Format为ROW格式，因为ROW格式会记录数据库变更的前像和后像，这个是恢复的关键。
MySQLRepair会解析出问题的一段Binlog，如果发现是1032错误（# 1032 : key not found）那么就需要根据Binlog里面记录的数据向DB里面插入对应的数据，如果发现是1062错误（1062 : duplicate key），则会删除掉DB里面存在的数据。

### MySQLRepair 如何使用 ###

MySQLRepair的使用非常简单，只需要在出问题的从机上运行Python脚本即可。
    
python dbRepair.py -h 127.0.0.1 -P 3306 -u sys -p sys -t 5
    
    Usage:dbRepair.py [-h localhost -u root -p xxxxxxx -P 3306 ]
    arguments:
        -h               host/ip. must have.
        -u<user>         username. must have
        -p<string>       password
        -P<NUM>          port
        -t<NUM>          every seconds to check slave.
        -h               help
日志输出如下：

    2017-03-15 16:07:58,346 - 1.py:749 - SlaveSQLRepair - Slave All Check Right , Sleep 5 Seconds for Next Loop
    2017-03-15 16:08:18,368 - 1.py:749 - SlaveSQLRepair - Slave All Check Right , Sleep 5 Seconds for Next Loop
    2017-03-15 16:08:23,374 - 1.py:749 - SlaveSQLRepair - Slave All Check Right , Sleep 5 Seconds for Next Loop
    2017-03-15 16:08:28,383 - 1.py:749 - SlaveSQLRepair - Slave All Check Right , Sleep 5 Seconds for Next Loop
    2017-03-15 16:08:33,396 - 1.py:734 - SlaveSQLRepair - ERROR FOUND !!! STRAT TO REPAIR RECORD
    2017-03-15 16:08:33,396 - 1.py:735 - SlaveSQLRepair - ERROR MESSAGE : Could not execute Delete_rows event on table zty.t1; Can't find record in 't1', Error_code: 1032; handler error HA_ERR_END_OF_FILE; the event's master log mysql-bin.000002, end_log_pos 631904743
    2017-03-15 16:08:33,396 - 1.py:736 - SlaveSQLRepair - RELAY LOG FILE : /ebs/mysql_data/mysqld-relay-bin.000027 . START POSITION : 222295 . STOP POSITION : 223468
    2017-03-15 16:08:33,406 - 1.py:640 - SlaveSQLRepair - SQL COMMAND IS : REPLACE INTO zty.t1 SELECT 1
    2017-03-15 16:08:33,409 - 1.py:640 - SlaveSQLRepair - SQL COMMAND IS : REPLACE INTO zty.t1 SELECT 1
