<?php

namespace micetm\queue;

use PhpAmqpLib\Connection\AMQPStreamConnection;
use micetm\queue\Queue as BaseQueue;
use PhpAmqpLib\Message\AMQPMessage;

/**
 * Class DelayedQueue
 * @package common\components\amqp
 */
class DelayedQueue extends BaseQueue
{
    public $maxMessagesCount;
    public $isDelayed = true;
    public $queueRightNow = 'right.now.queue';
    public $exchangeRightNow = 'right.now.exchange';
    public $delay;
    public $passive = false;
    public $durable = true;
    public $exclusive = false;
    public $auto_delete = false;
    public $no_wait = false;

    public function healthCheck()
    {
        $this->open();
        $count = $this->countMessages();
        return $count < $this->maxMessagesCount;
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

        $this->declareQueue($this->queueRightNow, $this->passive, $this->durable, $this->exclusive, $this->auto_delete);
        $this->channel->exchange_declare(
            $this->exchangeRightNow,
            $this->exchangeType,
            $this->passive,
            $this->durable,
            $this->auto_delete
        );
        $this->channel->queue_bind($this->queueRightNow, $this->exchangeRightNow);

        $this->declareDelayedQueue();
        $this->channel->exchange_declare(
            $this->exchangeName,
            $this->exchangeType,
            $this->passive,
            $this->durable,
            $this->auto_delete
        );
        $this->channel->queue_bind($this->queueName, $this->exchangeName);
    }

    protected function getArguments()
    {
        if (empty($this->isDelayed)) {
            return [];
        }

        return array("x-delayed-type" => [ 'S', $this->exchangeType]);
    }

    /**
     * Returns messages amount in target queue
     * @return int
     */
    protected function countMessages()
    {
        $count = $this->declareQueue($this->queueRightNow, $this->passive, $this->durable, $this->exclusive, $this->auto_delete);
        if (is_array($count) && $count[0] == $this->queueName) {
            return $count[1];
        }
        return 0;
    }

    /**
     * Listens amqp-queue and runs new jobs.
     */
    public function listen()
    {
        $this->open();
        $callback = function (AMQPMessage $payload) {
            list($ttr, $message) = explode(';', $payload->body, 2);
            if ($this->handleMessage(null, $message, $ttr, 1)) {
                $payload->delivery_info['channel']->basic_ack($payload->delivery_info['delivery_tag']);
            }
        };
        $this->channel->basic_qos(null, 1, null);
        $this->channel->basic_consume($this->queueRightNow, '', false, false, false, false, $callback);
        while (count($this->channel->callbacks)) {
            $this->channel->wait();
        }
    }

    /**
     * Declares a queue if it doesn't exists
     * @return mixed|null
     */
    protected function declareDelayedQueue()
    {

        /**
         * x-message-ttl delay in seconds to milliseconds
         * x-dead-letter-exchange after message expiration in delay queue, move message to the right.now.queue
         */
        return $this->channel->queue_declare(
            $this->queueName,
            $this->passive,
            $this->durable,
            $this->exclusive,
            $this->auto_delete,
            $this->no_wait,
            array(
                'x-message-ttl' => array('I', $this->delay*1000),
                'x-dead-letter-exchange' => array('S', $this->exchangeRightNow)
            )
        );
    }

    /**
     * Kills queues and closes connections
     */
    public function killQueue()
    {
        $this->connection = new AMQPStreamConnection($this->host, $this->port, $this->user, $this->password);
        $this->channel = $this->connection->channel();
        $this->channel->queue_unbind(
            $this->queueName,
            $this->exchangeName
        );

        $this->channel->queue_unbind(
            $this->queueRightNow,
            $this->exchangeRightNow
        );

        $this->channel->queue_delete($this->queueName);
        $this->channel->queue_delete($this->queueRightNow);
        $this->channel->exchange_delete($this->exchangeName);
        $this->channel->exchange_delete($this->exchangeRightNow);
        $this->close();
    }
}
