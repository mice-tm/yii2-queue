<?php

namespace micetm\queue;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use yii\queue\amqp\Queue as BaseQueue;

class Queue extends BaseQueue
{

    public $maxMessagesCount;
    public $passive = false;
    public $durable = true;
    public $exclusive = false;
    public $auto_delete = false;
    public $exchangeType = 'direct';
    public $delay = false;

    /**
     * @return bool
     */
    public function healthCheck()
    {
        $this->open();
        $count = $this->countMessages();
        return $count < $this->maxMessagesCount;
    }

    /**
     * Returns messages amount in target queue
     * @return int
     */
    protected function countMessages()
    {
        $count = $this->declareQueue(
            $this->queueName,
            $this->passive,
            $this->durable,
            $this->exclusive,
            $this->auto_delete
        );
        if (is_array($count) && $count[0] == $this->queueName) {
            return $count[1];
        }
        return 0;
    }

    /**
     * Creates queue and exchange, and binds them together
     */
    public function createConnection()
    {
        $this->open();
    }

    /**
     * Opens connection and channel
     */
    protected function open()
    {
        if ($this->channel) {
            return;
        }
        $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->password);
        $this->channel = $this->connection->channel();
        $this->declareQueue($this->queueName, $this->passive, $this->durable, $this->exclusive, $this->auto_delete);
        $this->channel->exchange_declare($this->exchangeName, $this->exchangeType, $this->passive, $this->durable, $this->auto_delete);
        $this->channel->queue_bind($this->queueName, $this->exchangeName);
    }

    /**
     * Creates a queue or returns info on existing queue
     *
     * @param string $queueName
     * @param bool   $passive
     * @param bool   $durable
     * @param bool   $exclusive
     * @param bool   $auto_delete
     * @param bool   $no_wait
     * @param array  $arguments
     *
     * @return mixed|null queue info
     */
    protected function declareQueue(
        $queueName,
        $passive = false,
        $durable = true,
        $exclusive = false,
        $auto_delete = false,
        $no_wait = false,
        $arguments = []
    ) {
        if ($this->delay) {
            $arguments = array_merge($arguments, [
                'x-expires' => array('I', $this->delay*1000),
            ]);
        }
        return $this->channel->queue_declare(
            $queueName,
            $passive,
            $durable,
            $exclusive,
            $auto_delete,
            $no_wait,
            $arguments
        );
    }

    public function killQueue()
    {
        $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->password);
        $this->channel = $this->connection->channel();
        $this->channel->queue_unbind($this->queueName, $this->exchangeName);
        $this->channel->queue_delete($this->queueName);
        $this->channel->exchange_delete($this->exchangeName);
        $this->close();
    }
}
