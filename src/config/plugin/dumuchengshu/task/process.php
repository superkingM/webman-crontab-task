<?php

use dumuchengshu\Task\Server;

return [
    'cron_task' => [
        'handler' => Server::class,
        'listen' => 'text://' . config('plugin.dumuchengshu.task.app.task.listen'), // 这里用了text协议，也可以用frame或其它协议
        'count' => 4, // 多进程每个进程只执行一个定时任务,需要提前规划好进程数
    ]
];
