<?php


namespace dumuchengshu\Task;

use support\Container;
use support\Db;
use support\Redis;
use Workerman\Connection\TcpConnection;
use Workerman\Crontab\Crontab;
use Workerman\Timer;
use Workerman\Worker;

class Server
{
    const FORBIDDEN_STATUS = '0';

    const NORMAL_STATUS = '1';

    // 命令任务
    public const COMMAND_CRONTAB = '1';
    // 类任务
    public const CLASS_CRONTAB = '2';
    // URL任务
    public const URL_CRONTAB = '3';
    // EVAL 任务
    public const EVAL_CRONTAB = '4';
    //shell 任务
    public const SHELL_CRONTAB = '5';

    private $worker;
    /**
     * 调试模式
     * @var bool
     */
    private $debug = false;

    /**
     * 记录日志
     * @var bool
     */
    private $writeLog = false;

    /**
     * 任务进程池
     * @var Crontab[] array
     */
    private $crontabPool = [];

    /**
     * 定时任务表
     * @var string
     */
    private $crontabTable;

    /**
     * 定时任务日志表
     * @var string
     */
    private $crontabLogTable;
    /**
     * 数据库前缀
     * @var string
     */
    private $prefix;
    /**
     * redis hash key
     * @var string
     */
    private $redisHashKey = 'crontab_worker';
    /**
     * 执行指令
     * @var string
     */
    private $redisAction = 'crontab_action';

    public function __construct()
    {
    }

    public function getRedisAction()
    {
        return $this->redisAction;
    }

    public function onWorkerStart(Worker $worker)
    {
        $config = config('plugin.dumuchengshu.task.app.task');
        $this->debug = $config['debug'] ?? true;
        $this->writeLog = $config['write_log'] ?? true;
        $this->crontabTable = $config['crontab_table'];
        $this->crontabLogTable = $config['crontab_table_log'];
        $this->redisHashKey = $config['crontab_redis_key'];
        $this->redisAction = $config['crontab_redis_action'];
        $this->prefix = $config['prefix'];
        $this->worker = $worker;
        $this->checkCrontabTables();
        $this->crontabInit();
        $this->monitorAction();
    }


    /**
     * 当客户端与Workman建立连接时(TCP三次握手完成后)触发的回调函数
     * 每个连接只会触发一次onConnect回调
     * 此时客户端还没有发来任何数据
     * 由于udp是无连接的，所以当使用udp时不会触发onConnect回调，也不会触发onClose回调
     *
     * @param TcpConnection $connection
     */
    public function onConnect(TcpConnection $connection)
    {
        $this->checkCrontabTables();
    }

    public function onMessage(TcpConnection $connection, $data)
    {
        Redis::set($this->redisAction, $data);
        Redis::expire($this->redisAction, 5);
        $connection->send("ok\n");
        /*$data = json_decode($data, true);
        $method = $data['method'];
        $args = $data['args'];
        $connection->send(call_user_func([$this, $method], $args));*/
    }

    /**
     *初始化定时任务
     */
    private function crontabInit()
    {
        if ($this->worker->id == 0) {
            $this->removeTaskId();
        }
        $crontab = Db::table($this->crontabTable)->where('status', self::NORMAL_STATUS)->orderByDesc('sort')->offset($this->worker->id)->first();
        if ($crontab) {
            Redis::hSet($this->redisHashKey, $crontab->id, $this->worker->id);
            $this->crontabRun($crontab->id);
        }
    }

    /**
     *监测指令
     */
    private function monitorAction()
    {
        Timer::add(0.1, function () {
            RedisLock::onLock('action', function () {
                $data = Redis::get($this->redisAction);
                if ($data) {
//                    var_dump($data);
                    $data = json_decode($data, true);
                    $method = $data['method'];
                    $args = $data['args'];
                    call_user_func([$this, $method], $args);
                }
            });
        });
    }

    /**
     * 删除指令
     */
    private function delAction()
    {
        Redis::del($this->redisAction);
    }

    /**
     * 创建定时器
     * @param $id
     */
    private function crontabRun($id)
    {
        $data = Db::table($this->crontabTable)->where('id', $id)->where('status', self::NORMAL_STATUS)->first();
        $data = json_decode(json_encode($data), true);
        if (!empty($data)) {
            switch ($data['type']) {
                case self::COMMAND_CRONTAB:
                    $this->crontabPool[$data['id']] = [
                        'id' => $data['id'],
                        'target' => $data['target'],
                        'rule' => $data['rule'],
                        'parameter' => $data['parameter'],
                        'singleton' => $data['singleton'],
                        'create_time' => date('Y-m-d H:i:s'),
                        'crontab' => new Crontab($data['rule'], function () use ($data) {
                            if (!$this->canRun($data['id'])) {
                                return false;
                            }

                            $time = time();
                            $parameter = $data['parameter'] ?: '{}';
                            $startTime = microtime(true);
                            $code = 0;
                            $result = true;
                            try {
                                if (strpos($data['target'], 'php webman') !== false) {
                                    $command = $data['target'];
                                } else {
                                    $command = "php webman " . $data['target'];
                                }
                                $exception = shell_exec($command);
                            } catch (\Throwable $e) {
                                $result = false;
                                $code = 1;
                                $exception = $e->getMessage();
                            }

                            $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'],
                                $result);
                            $endTime = microtime(true);
                            $this->updateRunning($data['id'], $time);
                            $this->writeLog && $this->crontabRunLog([
                                'crontab_id' => $data['id'],
                                'target' => $data['target'],
                                'parameter' => $parameter,
                                'exception' => $exception,
                                'return_code' => $code,
                                'running_time' => round($endTime - $startTime, 6),
                                'create_time' => $time,
                                'update_time' => $time,
                            ]);

                            $this->isSingleton($data);
                        })
                    ];
                    break;
                case self::CLASS_CRONTAB:
                    $this->crontabPool[$data['id']] = [
                        'id' => $data['id'],
                        'target' => $data['target'],
                        'rule' => $data['rule'],
                        'parameter' => $data['parameter'],
                        'singleton' => $data['singleton'],
                        'create_time' => date('Y-m-d H:i:s'),
                        'crontab' => new Crontab($data['rule'], function () use ($data) {
                            if (!$this->canRun($data['id'])) {
                                return false;
                            }
                            $time = time();
                            $class = trim($data['target']);
                            $parameters = !empty($data['parameter']) ? json_decode($data['parameter'], true) : [];
                            $startTime = microtime(true);
                            if ($class) {
                                $class = explode('\\', $class);
                                $method = end($class);
                                array_pop($class);
                                $class = implode('\\', $class);
                                if (class_exists($class) && method_exists($class, $method)) {
                                    try {
                                        $result = true;
                                        $code = 0;
                                        $instance = Container::get($class);
                                        if ($parameters && is_array($parameters)) {
                                            $res = $instance->{$method}(...$parameters);
                                        } else {
                                            $res = $instance->{$method}();
                                        }
                                    } catch (\Throwable $throwable) {
                                        $result = false;
                                        $code = 1;
                                    }
                                    $exception = isset($throwable) ? $throwable->getMessage() : $res;
                                } else {
                                    $result = false;
                                    $code = 1;
                                    $exception = "方法或类不存在或者错误";
                                }
                            } else {
                                $result = false;
                                $code = 1;
                                $exception = "方法或类不存在或者错误";
                            }
                            $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'],
                                $result);
                            $endTime = microtime(true);
                            $this->updateRunning($data['id'], $time);
                            $this->writeLog && $this->crontabRunLog([
                                'crontab_id' => $data['id'],
                                'target' => $data['target'],
                                'parameter' => json_encode($parameters),
                                'exception' => $exception ?? '',
                                'return_code' => $code,
                                'running_time' => round($endTime - $startTime, 6),
                                'create_time' => $time,
                                'update_time' => $time,
                            ]);
                            $this->isSingleton($data);
                        })
                    ];
                    break;
                case self::URL_CRONTAB:
                    $this->crontabPool[$data['id']] = [
                        'id' => $data['id'],
                        'target' => $data['target'],
                        'rule' => $data['rule'],
                        'parameter' => $data['parameter'],
                        'singleton' => $data['singleton'],
                        'create_time' => date('Y-m-d H:i:s'),
                        'crontab' => new Crontab($data['rule'], function () use ($data) {
                            if (!$this->canRun($data['id'])) {
                                return false;
                            }

                            $time = time();
                            $url = trim($data['target']);
                            $startTime = microtime(true);
                            $client = new \GuzzleHttp\Client();
                            try {
                                $response = $client->get($url);
                                $result = $response->getStatusCode() === 200;
                                $code = 0;
                            } catch (\Throwable $throwable) {
                                $result = false;
                                $code = 1;
                                $exception = $throwable->getMessage();
                            }

                            $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'],
                                $result);
                            $endTime = microtime(true);
                            $this->updateRunning($data['id'], $time);
                            $this->writeLog && $this->crontabRunLog([
                                'crontab_id' => $data['id'],
                                'target' => $data['target'],
                                'parameter' => $data['parameter'],
                                'exception' => $exception ?? '',
                                'return_code' => $code,
                                'running_time' => round($endTime - $startTime, 6),
                                'create_time' => $time,
                                'update_time' => $time,
                            ]);
                            $this->isSingleton($data);

                        })
                    ];
                    break;
                case self::EVAL_CRONTAB:
                    $this->crontabPool[$data['id']] = [
                        'id' => $data['id'],
                        'target' => $data['target'],
                        'rule' => $data['rule'],
                        'parameter' => $data['parameter'],
                        'singleton' => $data['singleton'],
                        'create_time' => date('Y-m-d H:i:s'),
                        'crontab' => new Crontab($data['rule'], function () use ($data) {
                            if (!$this->canRun($data['id'])) {
                                return false;
                            }

                            $time = time();
                            $startTime = microtime(true);
                            $result = true;
                            $code = 0;
                            try {
                                eval($data['target']);
                            } catch (\Throwable $throwable) {
                                $result = false;
                                $code = 1;
                                $exception = $throwable->getMessage();
                            }

                            $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'],
                                $result);
                            $endTime = microtime(true);
                            $this->updateRunning($data['id'], $time);
                            $this->writeLog && $this->crontabRunLog([
                                'crontab_id' => $data['id'],
                                'target' => $data['target'],
                                'parameter' => $data['parameter'],
                                'exception' => $exception ?? '',
                                'return_code' => $code,
                                'running_time' => round($endTime - $startTime, 6),
                                'create_time' => $time,
                                'update_time' => $time,
                            ]);
                            $this->isSingleton($data);

                        })
                    ];
                    break;
                case self::SHELL_CRONTAB:
                    $this->crontabPool[$data['id']] = [
                        'id' => $data['id'],
                        'target' => $data['target'],
                        'rule' => $data['rule'],
                        'parameter' => $data['parameter'],
                        'singleton' => $data['singleton'],
                        'create_time' => date('Y-m-d H:i:s'),
                        'crontab' => new Crontab($data['rule'], function () use ($data) {
                            if (!$this->canRun($data['id'])) {
                                return false;
                            }

                            $time = time();
                            $parameter = $data['parameter'] ?: '';
                            $startTime = microtime(true);
                            $code = 0;
                            $result = true;
                            try {
                                $exception = shell_exec($data['target']);
                            } catch (\Throwable $e) {
                                $result = false;
                                $code = 1;
                                $exception = $e->getMessage();
                            }

                            $this->debug && $this->writeln('执行定时器任务#' . $data['id'] . ' ' . $data['rule'] . ' ' . $data['target'],
                                $result);
                            $endTime = microtime(true);
                            $this->updateRunning($data['id'], $time);
                            $this->writeLog && $this->crontabRunLog([
                                'crontab_id' => $data['id'],
                                'target' => $data['target'],
                                'parameter' => $parameter,
                                'exception' => $exception,
                                'return_code' => $code,
                                'running_time' => round($endTime - $startTime, 6),
                                'create_time' => $time,
                                'update_time' => $time,
                            ]);
                            $this->isSingleton($data);

                        })
                    ];
                    break;
            }
        }
    }

    /**
     * 是否单次
     *
     * @param $crontab
     *
     * @return void
     */
    private function isSingleton($crontab)
    {
        if ($crontab['singleton'] == 0 && isset($this->crontabPool[$crontab['id']])) {
            $this->crontabPool[$crontab['id']]['crontab']->destroy();
            Db::table($this->crontabTable)->where('id', $crontab['id'])->update(['status' => self::FORBIDDEN_STATUS]);
            $this->debug && $this->writeln('定时器销毁', true);
        }
    }

    /**
     * 移除redis未启用任务ID
     */
    private function removeTaskId()
    {
        $crontabIds = Db::table($this->crontabTable)->where('status', self::NORMAL_STATUS)->orderByDesc('sort')->get()->pluck('id')->toArray();
        $hKeys = Redis::hKeys($this->redisHashKey);
        $diffIds = array_diff($hKeys, $crontabIds);
        if (!empty($diffIds)) {
            Redis::pipeline(function ($pipe) use ($diffIds) {
                foreach ($diffIds as $id) {
                    $pipe->hDel($this->redisHashKey, $id);
                }
            });
        }
    }

    /**
     * 判断任务是否可以执行
     * 由于禁用定时器或销毁定时器，crontab 会在下一个周期60s才会生效，这里做限制
     * @param $id
     * @return bool
     */
    private function canRun($id)
    {
        $status = Db::table($this->crontabTable)->where('id', $id)->value('status');
        if ($status == self::NORMAL_STATUS) {
            return true;
        }

        return false;
    }

    /**
     * 更新运行次数/时间
     *
     * @param $id
     * @param $time
     *
     * @return void
     */
    private function updateRunning($id, $time)
    {
        $table = $this->prefix . $this->crontabTable;
        Db::select("UPDATE {$table} SET running_times = running_times + 1, last_running_time = {$time} WHERE id = {$id}");
    }

    /**
     * 创建定时任务
     * @param array $param
     *
     * @return string
     */
    private function crontabCreate(array $param)
    {
        RedisLock::onLock('create', function () use ($param) {
            if (!$this->isEmptyProcess()) {
                return false;
            }
            $param['create_time'] = $param['update_time'] = time();
            $id = Db::table($this->crontabTable)->insertGetId($param);
            $id && $this->crontabRun($id);
            Redis::hSet($this->redisHashKey, $id, $this->worker->id);
            $this->delAction();
            return true;
        });
    }


    /**
     * 修改定时器
     *
     * @param array $param
     *
     * @return string
     */
    private function crontabUpdate(array $param)
    {
        RedisLock::onLock('update', function () use ($param) {
            $runTask = Redis::hGet($this->redisHashKey, $param['id']);
            if ($runTask === false) {
                if ($this->isEmptyProcess()) {
                    $this->_update($param);
                }
            } else {
                if (isset($this->crontabPool[$param['id']])) {
                    $this->crontabPool[$param['id']]['crontab']->destroy();
                    unset($this->crontabPool[$param['id']]);
                    $this->_update($param);
                }
            }

        });
    }

    /**
     * 更新封装部分代码
     * @param $param
     */
    private function _update($param)
    {
        $row = Db::table($this->crontabTable)->where('id', $param['id'])->update($param);
        if ($param['status'] == self::NORMAL_STATUS) {
            $this->crontabRun($param['id']);
            Redis::hSet($this->redisHashKey, $param['id'], $this->worker->id);
        } else {
            Redis::hDel($this->redisHashKey, $param['id']);
        }
        $this->delAction();
    }

    /**
     *清除定时任务
     * @param array $param
     */
    private function crontabDelete(array $param)
    {
        RedisLock::onLock('delete', function () use ($param) {
            $runTask = Redis::hGet($this->redisHashKey, $param['id']);
            if ($runTask === false) {
                Db::table($this->crontabTable)->where('id', $param['id'])->delete();
                $this->delAction();
            }
            if (isset($this->crontabPool[$param['id']])) {
                $this->crontabPool[$param['id']]['crontab']->destroy();
                unset($this->crontabPool[$param['id']]);
                Db::table($this->crontabTable)->where('id', $param['id'])->delete();
                Redis::hDel($this->redisHashKey, $param['id']);
                $this->delAction();
            }
        });

    }


    /**
     * 重启定时任务
     *
     * @param array $param
     *
     * @return string
     */
    private function crontabReload(array $param)
    {
        RedisLock::onLock('reload', function () use ($param) {
            $runTask = Redis::hGet($this->redisHashKey, $param['id']);
            if ($runTask === false) {
                if ($this->isEmptyProcess()) {
                    $this->_reload($param);
                }
            } else {
                if (isset($this->crontabPool[$param['id']])) {
                    $this->crontabPool[$param['id']]['crontab']->destroy();
                    unset($this->crontabPool[$param['id']]);
                    $this->_reload($param);
                }
            }
        });
    }

    /**
     * 重启封装
     * @param $param
     */
    private function _reload($param)
    {
        Db::table($this->crontabTable)->where('id', $param['id'])->update(['status' => self::NORMAL_STATUS]);
        $this->crontabRun($param['id']);
        Redis::hSet($this->redisHashKey, $param['id'], $this->worker->id);
        $this->delAction();
    }

    /**
     * 判断是否是空闲进程
     * @return bool
     */
    private function isEmptyProcess()
    {
        $runProcessIds = Redis::hVals($this->redisHashKey);
        $workerId = $this->worker->id;
        if (in_array($workerId, $runProcessIds)) {
            return false;
        }
        return true;
    }

    /**
     * 记录执行日志
     *
     * @param array $param
     *
     * @return void
     */
    private function crontabRunLog(array $param): void
    {
        Db::table($this->crontabLogTable)->insert($param);
    }

    /**
     * 输出日志
     *
     * @param      $msg
     * @param bool $isSuccess
     */
    private function writeln($msg, bool $isSuccess)
    {
        echo '[' . date('Y-m-d H:i:s') . '] ' . $msg . ($isSuccess ? " [Ok] " : " [Fail] ") . PHP_EOL;
    }

    /**
     *检测表是否存在
     */
    private function checkCrontabTables()
    {
        $allTables = $this->getDbTables();
        !in_array($this->crontabTable, $allTables) && $this->createCrontabTable();
        !in_array($this->crontabLogTable, $allTables) && $this->createCrontabLogTable();
    }

    /**
     * 获取数据库表名
     * @return array
     */
    private function getDbTables(): array
    {
        return Db::select('show tables');
    }

    /**
     * 创建定时器任务表
     */
    private function createCrontabTable()
    {
        $table = $this->prefix . $this->crontabTable;
        $sql = <<<SQL
 CREATE TABLE IF NOT EXISTS `{$table}`  (
  `id` int(11) UNSIGNED NOT NULL AUTO_INCREMENT,
  `title` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务标题',
  `type` tinyint(1) NOT NULL DEFAULT 1 COMMENT '任务类型 (1 command, 2 class, 3 url, 4 eval)',
  `rule` varchar(100) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '任务执行表达式',
  `rule_params` varchar(255) NOT NULL DEFAULT '' COMMENT '任务执行表达式的字段',
  `target` varchar(150) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL DEFAULT '' COMMENT '调用任务字符串',
  `parameter` varchar(500)  COMMENT '任务调用参数', 
  `running_times` int(11) NOT NULL DEFAULT '0' COMMENT '已运行次数',
  `last_running_time` int(11) NOT NULL DEFAULT '0' COMMENT '上次运行时间',
  `remark` varchar(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL COMMENT '备注',
  `sort` int(11) NOT NULL DEFAULT 0 COMMENT '排序，越大越前',
  `status` tinyint(4) NOT NULL DEFAULT 0 COMMENT '任务状态状态[0:禁用;1启用]',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  `singleton` tinyint(1) NOT NULL DEFAULT 1 COMMENT '是否单次执行 (0 是 1 不是)',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `title`(`title`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE,
  INDEX `status`(`status`) USING BTREE,
  INDEX `type`(`type`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务表' ROW_FORMAT = DYNAMIC
SQL;

        return Db::statement($sql);
    }

    /**
     * 定时器任务流水表
     */
    private function createCrontabLogTable()
    {
        $table = $this->prefix . $this->crontabLogTable;
        $sql = <<<SQL
CREATE TABLE IF NOT EXISTS `{$table}`  (
  `id` bigint UNSIGNED NOT NULL AUTO_INCREMENT,
  `crontab_id` bigint UNSIGNED NOT NULL COMMENT '任务id',
  `target` varchar(255) NOT NULL COMMENT '任务调用目标字符串',
  `parameter` varchar(500)  COMMENT '任务调用参数', 
  `exception` text  COMMENT '任务执行或者异常信息输出',
  `return_code` tinyint(1) NOT NULL DEFAULT 0 COMMENT '执行返回状态[0成功; 1失败]',
  `running_time` varchar(10) NOT NULL COMMENT '执行所用时间',
  `create_time` int(11) NOT NULL DEFAULT 0 COMMENT '创建时间',
  `update_time` int(11) NOT NULL DEFAULT 0 COMMENT '更新时间',
  PRIMARY KEY (`id`) USING BTREE,
  INDEX `create_time`(`create_time`) USING BTREE,
  INDEX `crontab_id`(`crontab_id`) USING BTREE
) ENGINE = InnoDB AUTO_INCREMENT = 1 CHARACTER SET = utf8mb4 COLLATE = utf8mb4_general_ci COMMENT = '定时器任务执行日志表' ROW_FORMAT = DYNAMIC
SQL;

        return Db::statement($sql);
    }

}