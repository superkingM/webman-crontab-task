<?php
return [
    'enable' => false,
    'task' => [
        'listen' => '0.0.0.0:12345',
        'crontab_table' => 'system_crontab', //任务计划表
        'crontab_table_log' => 'system_crontab_log',//任务计划流水表
        'crontab_redis_key' => 'crontab_worker',//任务redis执行存储
        'crontab_redis_action' => 'crontab_action',//任务操作指令
        'prefix' => '',//数据库前缀
        'debug' => true, //控制台输出日志
        'write_log' => true, // 任务计划日志
    ],
];
