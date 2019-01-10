"use strict";
const logger = require("@dojot/dojot-module-logger").logger;
const Kafka = require('node-rdkafka');
const util = require("util");

const TAG = { filename: "consumer" };

/**
 * @typedef {object} Consumer~Message
 *
 * This description was retrieved from original
 * [node-rdkafka repository](https://github.com/Blizzard/node-rdkafka/blob/master/lib/kafka-consumer.js#L107)
 *
 * @property {buffer} value - the message buffer from Kafka.
 * @property {string} topic - the topic name
 * @property {number} partition - the partition on the topic the
 * message was on
 * @property {number} offset - the offset of the message
 * @property {string} key - the message key
 * @property {number} size - message size, in bytes.
 * @property {number} timestamp - message timestamp
 */

/**
 * Class wrapping a Kafka.KafkaConsumer object.
 */
class Consumer {

  /**
   * Builds a new Consumer.
   *
   * It is important to realize that the `kafka.consumer` and `kafka.producer`
   * configuration are directly passed to node-rdkafka library (which will
   * forward it to librdkafka). You should check [its
   * documentation](https://github.com/edenhill/librdkafka/blob/0.11.1.x/CONFIGURATION.md)
   * to know which are all the possible settings it offers.
   * @param {config} config the configuration to be used by this object
   */
  constructor(config, name) {
    logger.info(`Creating a new Kafka consumer named ${name}...`, TAG);
    logger.info("Configuration is:");
    logger.info(util.inspect(config.kafka.consumer, { depth: null}));
    this.consumer = new Kafka.KafkaConsumer(config.kafka.consumer);
    logger.info("... consumer created.");
    this.isReady = false;

    this.messageCallbacks = {};
    this.subscriptions = [];
    this.name = name;
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
    logger.info("Connecting the consumer...", TAG);
    const readyPromise = new Promise((resolve, reject) => {
      const timeoutTrigger = setTimeout(() => {
        logger.warn("Failed to connect the consumer.", TAG);
        reject("timed out");
      }, 5000);

      this.consumer.on("ready", () => {
        logger.info("Consumer is connected", TAG);
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

  /**
   * Subscribe to a particular topic in Kafka
   *
   * The callback function must have one parameter which will contain the
   * received message. This messsage will have the following attributes:
   *
   * @param {string} topic the topic which this consumer will be subscribed to.
   * @param {*} callback a callback to be invoked whenever a message is received.
   */
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
        logger.debug(`Received message by consumer ${this.name}`);
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
   * The message format is described in {@link Consumer~Message}.
   * @param {number} maxMessages Number of messages to be consumed.
   * @return {Promise} A promise which will be resolved with the list of
   * received messages.
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

  /**
   * Commit the current partition position.
   */
  commit() {
    this.consumer.commit(null);
  }

  /**
   * Disconnect the consumer from a Kafka cluster
   *
   * @returns {Promise} A promise which will be resolved if consumer was
   * successfully disconnected or rejected otherwise. The rejection will have
   * an attribute containing the error.
   */
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


