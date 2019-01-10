"use strict";

function retrieveKafkaHosts() {
  let ret = process.env.KAFKA_HOSTS || "kafka:9092";
  return ret.split(",");
}
/**

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
 *
 */


module.exports={
     kafka: {
        producer: {
            "bootstrap_servers": retrieveKafkaHosts(),
            "compression.codec": "gzip",
            "retry.backoff.ms": 200,
            "message.send.max.retries": 10,
            "socket.keepalive.enable": true,
            "queue.buffering.max.messages": 100000,
            "queue.buffering.max.ms": 1000,
            "batch.num.messages": 1000000,
            "dr_cb": true
        },

        consumer: {
            "group.id": process.env.KAFKA_GROUP_ID || "data-broker",
            "bootstrap_servers": retrieveKafkaHosts()
        }
    },
    databroker: {
      url: process.env.DATA_BROKER_URL || "http://data-broker",
      timeoutSleep: 2000,
      connectionRetries: 5,
    },
    auth: {
      url: process.env.AUTH_URL || "http://auth:5000",
      timeoutSleep: 5000,
      connectionRetries: 5,
    },
    deviceManager: {
      url: process.env.DEVICE_MANAGER_URL || "http://device-manager:5000",
      timeoutSleep: 5000,
      connectionRetries: 3,
    },
    dojot: {
      management: {
        username: process.env.DOJOT_MANAGEMENT_USER || "dojot-management",
        tenant: process.env.DOJOT_MANAGEMENT_TENANT || "dojot-management"
      },
      subjects: {
        tenancy: process.env.DOJOT_SUBJECT_TENANCY || "dojot.tenancy",
        devices: process.env.DOJOT_SUBJECT_DEVICES || "dojot.device-manager.device",
        deviceData: process.env.DOJOT_SUBJECT_DEVICE_DATA || "device-data",
      }
    }
};
