<?php


namespace dumuchengshu\Task;


class Client
{
    private $client;
    protected static $instance = null;

    public function __construct()
    {
        /**
         * 可修改为tcp://127.0.0.1: + 端口
         */
        $this->client = stream_socket_client('tcp://' . config('plugin.dumuchengshu.task.app.task.listen'));
    }

    public static function instance()
    {
        if (!static::$instance) {
            static::$instance = new static();
        }
        return static::$instance;
    }

    /**
     * @param array $param
     * @return mixed
     */
    public function request(array $param)
    {
        fwrite($this->client, json_encode($param) . "\n"); // text协议末尾有个换行符"\n"
    }
}