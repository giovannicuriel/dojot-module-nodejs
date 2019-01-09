"use strict";
const logger = require("@dojot/dojot-module-logger").logger;
const Kafka = require('node-rdkafka');

const TAG = { filename: "consumer" };

/**
 * Class wrapping a Kafka.KafkaConsumer object.
 */
class Consumer {

  /**
   * Builds a new Consumer.
   *
   * The configuration object should have the follwing attributes:
   * ```json
   * kafka: {
   *   producer: {
   *
   *   },
   *   consumer: {
   *
   *   }
   * }
   * ```
   * For instance, an example of such object would be:
   *
   * ```json
   * kafka: {
   *   producer: {
   *       "metadata.broker.list": "kafka:9092",
   *       "compression.codec": "gzip",
   *       "retry.backoff.ms": 200,
   *       "message.send.max.retries": 10,
   *       "socket.keepalive.enable": true,
   *       "queue.buffering.max.messages": 100000,
   *       "queue.buffering.max.ms": 1000,
   *       "batch.num.messages": 1000000,
   *       "dr_cb": true
   *   },
   *   consumer: {
   *     "group.id": "data-broker",
   *     "metadata.broker.list": "kafka:9092",
   *   }
   * }
   * ```
   * It is important to realize that the `kafka.consumer` and `kafka.producer`
   * configuration are directly passed to node-rdkafka library (which will
   * forward it to librdkafka). You should check [its
   * documentation](https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md)
   * to know which are all the possible settings it offers.
   * @param {config} consumerConfig the configuration to be used by this object
   *
   */
  constructor(consumerConfig) {
    logger.debug('Creating a new Kafka consumer...', TAG);
    this.consumer = new Kafka.KafkaConsumer(consumerConfig);
    this.isReady = false;

    this.messageCallbacks = {};
    this.subscriptions = [];
  }

  /**
   * Connect the consumer to a Kafka cluster
   *
   * This function will wait 5 seconds for the connection to be completed. If
   * this doesn't happen within that time, a timeout will cause the returned
   * promise to be rejected.
   *
   * @returns { Promise } A promise which will be resolved when the connection
   * is completed or rejected if takes too long to complete.
   */
  connect() {

    logger.info("Connecting the consumer ..");
    const readyPromise = new Promise((resolve, reject) => {
      const timeoutTrigger = setTimeout(() => {
        logger.warn("Failed to connect the consumer.");
        reject("timed out");
      }, 5000);

      this.consumer.on("ready", () => {
        logger.info("Consumer is connected");
        clearTimeout(timeoutTrigger);
        this.isReady = true;
        if (this.subscriptions.length != 0) {
          this.consumer.subscribe(this.subscriptions);
        }
        this.consumer.consume();
        resolve();
      });
    });

    this.consumer.connect();
    return readyPromise;
  }

  subscribe(topic, callback) {
    if (!(topic in this.messageCallbacks)) {
      this.messageCallbacks[topic] = [];
    }

    this.messageCallbacks[topic].push(callback);

    if (this.isReady == false) {
      this.subscriptions.push(topic);
    } else {
      logger.debug(`Unsubscribing from topics ${this.subscriptions}`);
      this.consumer.unsubscribe();
      logger.debug(`Adding new topic ${topic}`);
      this.subscriptions.push(topic);
      logger.debug(`Subscribing to topics ${this.subscriptions}`);
      this.consumer.subscribe(this.subscriptions);
      this.consumer.on('data', (kafkaMessage) => {
        if (kafkaMessage.topic == topic) {
          for (let callback of this.messageCallbacks[topic]) {
            callback(kafkaMessage);
          }
        }
      });
      console.log(`Subscribed to topic ${topic}`);
    }
  }

  /**
   * Consume a number of messages from a set of topics.
   *
   *
   *
   * @param {number} maxMessages Number of messages to be consumed.
   * @return Promise a primis
   */
  consume(maxMessages = 1) {
    return new Promise((resolve, reject) => {
      this.consumer.consume(maxMessages, (err, messages) => {
        if (err) {
          reject(err);
        } else {
          console.log("Message consumed!");
          resolve(messages);
        }
      });
    });
  }

  commit() {
    this.consumer.commit(null);
  }

  disconnect() {
    const disconnectPromise = new Promise((resolve, reject) => {
      const timeoutTrigger = setTimeout(() => {
        console.error("Unnable to disconnect the consumer.");
        reject();
      }, 100000);

      this.consumer.disconnect((err, info) => {

        if (err) {
          console.error(err);
          reject(err);
        } else {
          console.log("disconnected!");
          clearTimeout(timeoutTrigger);
          resolve(info);
        }
      });
    });

    return disconnectPromise;
  }

}

module.exports = Consumer;


