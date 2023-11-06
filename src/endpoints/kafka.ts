import { type Request, type Response } from "express";
import {
  Kafka,
  type Consumer,
  type RecordMetadata,
  type EachMessagePayload,
} from "kafkajs";

import { KAFKA_URL } from "../env";

const kafka = new Kafka({
  clientId: "upstash-kafka-local",
  brokers: [new URL(KAFKA_URL).host],
});

const producer = kafka.producer();

interface ProduceOneToTopicRequestParams {
  topic: string;
  message: string;
}

interface ProduceOneToTopicRequestQueryParams {
  key?: string;
}

interface ProduceOneToTopicResponseBody {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
}

export async function produceOneToTopic(
  req: Request<
    ProduceOneToTopicRequestParams,
    unknown,
    unknown,
    ProduceOneToTopicRequestQueryParams
  >,
  res: Response<ProduceOneToTopicResponseBody>,
) {
  const { params, query } = req;
  const { topic, message } = params;
  const { key } = query;

  await producer.connect();
  const [info] = await producer.send({
    topic,
    messages: [{ value: message, key }],
  });

  return res.json({
    topic,
    // biome-ignore lint/style/noNonNullAssertion: we know this should exist
    partition: info!.partition,
    // biome-ignore lint/style/noNonNullAssertion: we know this should exist
    offset: info?.offset ? parseInt(info!.offset, 10) : 0,
    timestamp: info?.timestamp ? parseInt(info.timestamp, 10) : Date.now(),
  });
}

interface ProduceManyToTopicRequestParams {
  topic: string;
}

interface ProduceManyToTopicRequestBody {
  partition?: number;
  timestamp?: number;
  key?: string;
  value: string;
  headers?: {
    key: string;
    value: string;
  }[];
}

type ProduceManyToTopicResponseBody = ProduceOneToTopicResponseBody[];

export async function produceManyToTopic(
  req: Request<
    ProduceManyToTopicRequestParams,
    unknown,
    ProduceManyToTopicRequestBody | ProduceManyToTopicRequestBody[]
  >,
  res: Response<ProduceManyToTopicResponseBody>,
) {
  const { body, params } = req;
  const { topic } = params;
  const messages = Array.isArray(body) ? body : [body];

  await producer.connect();
  const infos = await producer.send({
    topic,
    messages: messages.map((message) => ({
      ...message,
      timestamp: message.timestamp?.toString(),
      headers: Object.fromEntries(
        (message.headers ?? []).map((header) => [header.key, header.value]),
      ),
    })),
  });

  return res.json(
    infos.map((info) => ({
      topic,
      // biome-ignore lint/style/noNonNullAssertion: we know this should exist
      partition: info!.partition,
      // biome-ignore lint/style/noNonNullAssertion: we know this should exist
      offset: info?.offset ? parseInt(info!.offset, 10) : 0,
      timestamp: info?.timestamp ? parseInt(info.timestamp, 10) : Date.now(),
    })),
  );
}

interface ProduceManyRequestBody extends ProduceManyToTopicRequestBody {
  topic: string;
}

export async function produceMany(
  req: Request<
    unknown,
    unknown,
    ProduceManyRequestBody | ProduceManyRequestBody[]
  >,
  res: Response,
) {
  const { body } = req;
  const messages = Array.isArray(body) ? body : [body];
  const messageTopicMap = new Map<string, ProduceManyRequestBody[]>();

  await producer.connect();

  for (const message of messages) {
    if (!messageTopicMap.has(message.topic)) {
      messageTopicMap.set(message.topic, [message]);
    } else {
      messageTopicMap.get(message.topic)?.push(message);
    }
  }

  const infos: RecordMetadata[] = [];

  for (const topic of messageTopicMap.keys()) {
    const topicMessages = messageTopicMap.get(topic);

    if (topicMessages) {
      const nextInfos = await producer.send({
        topic,
        messages: topicMessages?.map((message) => ({
          ...message,
          timestamp: message.timestamp?.toString(),
          headers: Object.fromEntries(
            (message.headers ?? []).map((header) => [header.key, header.value]),
          ),
        })),
      });

      infos.push(...nextInfos);
    }
  }

  return res.json(
    infos.map((info) => ({
      topic: info.topicName,
      // biome-ignore lint/style/noNonNullAssertion: we know this should exist
      partition: info!.partition,
      // biome-ignore lint/style/noNonNullAssertion: we know this should exist
      offset: info?.offset ? parseInt(info!.offset, 10) : 0,
      timestamp: info?.timestamp ? parseInt(info.timestamp, 10) : Date.now(),
    })),
  );
}

const consumerMap = new Map<string, Consumer>();
const subscriptionMap = new Map<
  string,
  {
    enableAutoCommit: boolean;
    autoCommitInterval: number;
    autoOffsetReset: "earliest" | "latest" | "none";
  }
>();
const messageMap = new Map<string, EachMessagePayload[]>();

async function suscbribeGroupToTopics(
  groupId: string,
  topics: string[],
  options: {
    enableAutoCommit: boolean;
    autoCommitInterval: number;
    autoOffsetReset: "earliest" | "latest" | "none";
  },
) {
  let consumer: Consumer;
  let created = false;
  let optionsChanged = false;

  if (!consumerMap.has(groupId)) {
    consumer = kafka.consumer({ groupId });

    created = true;

    consumerMap.set(groupId, consumer);
  } else {
    // biome-ignore lint/style/noNonNullAssertion: we know this exists
    consumer = consumerMap.get(groupId)!;
  }

  await consumer.connect();

  for (const topic of topics) {
    const subscriptionKey = `${groupId}:${topic}`;

    if (!subscriptionMap.has(subscriptionKey)) {
      await consumer.subscribe({
        topic,
        fromBeginning: options.autoOffsetReset === "earliest",
      });

      subscriptionMap.set(subscriptionKey, options);
    } else {
      // biome-ignore lint/style/noNonNullAssertion: we know this exists
      const prevOptions = subscriptionMap.get(subscriptionKey)!;

      if (!optionsChanged) {
        optionsChanged =
          prevOptions.enableAutoCommit !== options.enableAutoCommit ||
          prevOptions.autoCommitInterval !== options.autoCommitInterval ||
          prevOptions.autoOffsetReset !== options.autoOffsetReset;
      }
    }
  }

  if (created || optionsChanged) {
    if (optionsChanged) {
      await consumer.stop();
    }

    await consumer.run({
      autoCommit: options.enableAutoCommit,
      autoCommitInterval: options.autoCommitInterval,
      eachMessage: async (message) => {
        const messageMapKey = `${groupId}:${message.topic}`;
        const messages = messageMap.get(messageMapKey) ?? [];

        messages.push(message);
        messageMap.set(messageMapKey, messages);
      },
    });
  }
}

interface ConsumeByTopicRequestParams {
  groupId: string;
  instanceId: string;
  topic: string;
}

interface ConsumeByTopicRequestQueryParams {
  timeout?: string;
}

type ConsumeByTopicResponseBody = {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  key: string;
  value: string;
  headers: { key: string; value: string }[];
}[];

export async function consumeByTopic(
  req: Request<
    ConsumeByTopicRequestParams,
    unknown,
    unknown,
    ConsumeByTopicRequestQueryParams
  >,
  res: Response<ConsumeByTopicResponseBody>,
) {
  const { headers, params, query } = req;
  const { groupId, topic } = params;
  const { timeout } = query;
  const {
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Enable-Auto-Commit"]: enableAutoCommit = "true",
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Commit-Interval"]: autoCommitInterval = "5000",
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Offset-Reset"]: autoOffsetReset = "latest",
  } = headers as {
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Enable-Auto-Commit"]: "true" | "false";
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Commit-Interval"]: string;
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Offset-Reset"]: "earliest" | "latest" | "none";
  };

  await suscbribeGroupToTopics(groupId, [topic], {
    enableAutoCommit: enableAutoCommit === "true",
    autoCommitInterval: autoCommitInterval
      ? parseInt(autoCommitInterval, 10)
      : 5000,
    autoOffsetReset,
  });

  const messageMapKey = `${groupId}:${topic}`;
  let sleep: Promise<void> | undefined;

  // biome-ignore lint/style/noNonNullAssertion: we know this exists
  if (!messageMap.has(messageMapKey) || messageMap.get(messageMapKey)!.length) {
    sleep = new Promise((resolve) => {
      setTimeout(() => resolve(), timeout ? parseInt(timeout, 10) : 5000);
    });
  }

  await sleep;

  const result: ConsumeByTopicResponseBody = [];
  const messages = messageMap.get(messageMapKey) ?? [];

  for (const message of messages) {
    result.push({
      topic: message.topic,
      partition: message.partition,
      offset: parseInt(message.message.offset, 10),
      timestamp: parseInt(message.message.timestamp, 10),
      key: message.message.key?.toString() ?? "",
      value: message.message.value?.toString() ?? "",
      headers: Object.entries(message.message.headers ?? {}).map(
        ([key, value]) => ({
          key,
          value: value?.toString() ?? "",
        }),
      ),
    });
  }

  messageMap.set(messageMapKey, []);

  return res.json(result);
}

interface ConsumRequestParams {
  groupId: string;
  instanceId: string;
}

interface ConsumRequestBody {
  topic?: string;
  topics?: string[];
  timeout?: number;
}

type ConsumeResponseBody = ConsumeByTopicResponseBody;

export async function consume(
  req: Request<ConsumRequestParams, unknown, ConsumRequestBody>,
  res: Response<ConsumeResponseBody>,
) {
  const { body, headers, params } = req;
  const { groupId } = params;
  const { topic, topics, timeout } = body;
  const messageTopics = Array.isArray(topics) ? topics : [topic as string];
  const {
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Enable-Auto-Commit"]: enableAutoCommit = "true",
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Commit-Interval"]: autoCommitInterval = "5000",
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Offset-Reset"]: autoOffsetReset = "latest",
  } = headers as {
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Enable-Auto-Commit"]: "true" | "false";
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Commit-Interval"]: string;
    // biome-ignore lint/complexity/useLiteralKeys: this is broken
    ["Kafka-Auto-Offset-Reset"]: "earliest" | "latest" | "none";
  };

  await suscbribeGroupToTopics(groupId, messageTopics, {
    enableAutoCommit: enableAutoCommit === "true",
    autoCommitInterval: autoCommitInterval
      ? parseInt(autoCommitInterval, 10)
      : 5000,
    autoOffsetReset,
  });

  let sleep: Promise<void> = Promise.resolve();

  for (const topic of messageTopics) {
    const messageMapKey = `${groupId}:${topic}`;

    if (
      !sleep &&
      // biome-ignore lint/style/noNonNullAssertion: we know this exists
      (!messageMap.has(messageMapKey) || messageMap.get(messageMapKey)!.length)
    ) {
      sleep = new Promise((resolve) => {
        setTimeout(() => resolve(), timeout);
      });
    }
  }

  await sleep;

  const result: ConsumeByTopicResponseBody = [];

  for (const topic of messageTopics) {
    const messageMapKey = `${groupId}:${topic}`;
    const messages = messageMap.get(messageMapKey) ?? [];

    for (const message of messages) {
      result.push({
        topic: message.topic,
        partition: message.partition,
        offset: parseInt(message.message.offset, 10),
        timestamp: parseInt(message.message.timestamp, 10),
        key: message.message.key?.toString() ?? "",
        value: message.message.value?.toString() ?? "",
        headers: Object.entries(message.message.headers ?? {}).map(
          ([key, value]) => ({
            key,
            value: value?.toString() ?? "",
          }),
        ),
      });
    }

    messageMap.set(messageMapKey, []);
  }

  return res.json(result);
}

interface CommitRequestParams {
  groupId: string;
  instanceId: string;
}

interface CommitRequestBody {
  topic: string;
  partition: number;
  offset: number;
}

interface CommitResponseBody {
  result: "Success";
  status: 200;
}

export async function commit(
  req: Request<
    CommitRequestParams,
    unknown,
    CommitRequestBody | CommitRequestBody[]
  >,
  res: Response<CommitResponseBody>,
) {
  const { body, params } = req;
  const { groupId } = params;
  const messages = Array.isArray(body) ? body : [body];
  const consumer = consumerMap.get(groupId) ?? kafka.consumer({ groupId });

  await consumer.connect();
  await consumer.commitOffsets(
    messages.map((message) => ({
      topic: message.topic,
      partition: message.partition,
      offset: message.offset.toString(),
    })),
  );

  return res.json({ result: "Success", status: 200 });
}
