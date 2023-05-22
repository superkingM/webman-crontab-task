<?php


namespace dumuchengshu\Task;

use support\Log;
use support\Redis;

/**
 * redis 实现并发锁
 * Class RedisLock
 * @package dumuchengshu\Task
 */
class RedisLock
{
    public $lockedNames = [];

    /**
     * 加锁
     * @param $name
     * @param int $timeout
     * @param int $expire
     * @param int $waitIntervalUs
     * @return bool
     */
    public function lock($name, $timeout = 10, $expire = 15, $waitIntervalUs = 100000)
    {
        if ($name == null) return false;

        //取得当前时间
        $now = time();
        //获取锁失败时的等待超时时刻
        $timeoutAt = $now + $timeout;
        //锁的最大生存时刻
        $expireAt = $now + $expire;

        $redisKey = "Lock:{$name}";


        while (true) {
            //将rediskey的最大生存时刻存到redis里，过了这个时刻该锁会被自动释放
            $result = Redis::setNx($redisKey, $expireAt);

            if ($result) {
                //设置key的失效时间
                Redis::expire($redisKey, $expire);
                //将锁标志放到lockedNames数组里
                $this->lockedNames[$name] = $expireAt;
                //以秒为单位，返回给定key的剩余生存时间
                $ttl = Redis::ttl($redisKey);
                //ttl小于0 表示key上没有设置生存时间（key是不会不存在的，因为前面setnx会自动创建）
                //如果出现这种状况，那就是进程的某个实例setnx成功后 crash 导致紧跟着的expire没有被调用
                //这时可以直接设置expire并把锁纳为己用
                if ($ttl < 0) {
                    Redis::set($redisKey, $expireAt);
                    $this->lockedNames[$name] = $expireAt;
                    return true;
                }
                return true;
            }

            /*****检测key是否未设置过期时间*****/
            if (Redis::get($redisKey) < $expireAt) {
                //设置key的失效时间
                Redis::expire($redisKey, 0);
            }

            /*****循环请求锁部分*****/
            //如果没设置锁失败的等待时间 或者 已超过最大等待时间了，那就退出
            if ($timeout <= 0 || $timeoutAt < microtime(true)) break;
            //隔 $waitIntervalUs 后继续 请求
            usleep($waitIntervalUs);

        }

        return false;
    }

    /**
     * 检测锁
     * @param $name
     * @return bool
     */
    public function isLocking($name)
    {
        return key_exists($name, $this->lockedNames);
    }

    /**
     * 解锁
     * @param $name
     * @return bool
     */
    public function unlock($name)
    {
        //先判断是否存在此锁
        if ($this->isLocking($name)) {
            //删除锁
            if (Redis::del("Lock:$name")) {
                //清掉lockedNames里的锁标志
                unset($this->lockedNames[$name]);
                return true;
            }
        }
        return false;
    }

    /**
     * 解除所有锁
     * @return bool
     */
    public function unlockAll()
    {
        //此标志是用来标志是否释放所有锁成功
        $allSuccess = true;
        foreach ($this->lockedNames as $name => $expireAt) {
            if (false === $this->unlock($name)) {
                $allSuccess = false;
            }
        }
        return $allSuccess;
    }

    /**
     * 业务锁
     * @param $name
     * @param \Closure $closure
     * @param int $timeout
     * @param int $expire
     * @param int $waitIntervalUs
     * @return mixed
     * @throws \Exception
     */
    public static function onLock($name, \Closure $closure, $timeout = 30, $expire = 60, $waitIntervalUs = 30000)
    {
        $self = new self();
        try {
            if ($self->lock($name, $timeout, $expire, $waitIntervalUs)) {
                $result = $closure();
                $self->unlock($name);
                return $result;
            }
            Log::error('请求超时');
        } catch (\Exception $exception) {
            $self->unlock($name);
            Log::error($exception);
        }
    }

    /**
     * 删除锁
     * @param $name
     */
    public static function delLock($name)
    {
        $self = new self();
        $self->unlock($name);
    }
}