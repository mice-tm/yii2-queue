Yii2 Queue Extension
====================

An extension for running tasks asyncronously via queues.

It supported queues based on **DB**, **Redis**, **RabbitMQ**, **Beanstalk** and **Gearman**.

Added delayed queue support for AMQP and health-checks for queues

Fork of [https://github.com/yiisoft/yii2-queue](https://github.com/yiisoft/yii2-queue)

Installation
------------

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Update composer.json
1) add to the require section of your `composer.json` file,

```
"mice-tm/yii2-queue": "dev-master"
```

2) then add a repositories section of your `composer.json` file.

```
{
  "type": "vcs",
  "url": "https://github.com/mice-tm/yii2-queue"
}
```


Basic Usage
-----------

Each task which is sent to queue should be defined as a separate class.
For example, if you need to download and save a file the class may look like the following:

```php
class DownloadJob extends Object implements \yii\queue\Job
{
    public $url;
    public $file;

    public function execute($queue)
    {
        file_put_contents($this->file, file_get_contents($this->url));
    }
}
```

Here's how to send a task into queue:

```php
Yii::$app->queue->push(new DownloadJob([
    'url' => 'http://example.com/image.jpg',
    'file' => '/tmp/image.jpg',
]));
```
Pushes job into queue that run after 5 min:

```php
Yii::$app->queue->delay(5 * 60)->push(new DownloadJob([
    'url' => 'http://example.com/image.jpg',
    'file' => '/tmp/image.jpg',
]));
```

The exact way task is executed depends on the driver used. The most part of drivers can be run using
console commands, which the component registers in your application.

Command that obtains and executes tasks in a loop until queue is empty:

```sh
yii queue/run
```

Command launches a daemon which infinitely queries the queue:

```sh
yii queue/listen
```

See documentation for more details about driver console commands and their options.

The component has ability to track status of a job which was pushed into queue.

```php
// Push a job into queue and get massage ID.
$id = Yii::$app->queue->push(new SomeJob());

// The job is waiting for execute.
Yii::$app->queue->isWaiting($id);

// Worker gets the job from queue, and executing it.
Yii::$app->queue->isReserved($id);

// Worker has executed the job.
Yii::$app->queue->isDone($id);
```