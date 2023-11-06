import { json } from "body-parser";
import express from "express";
import morgan from "morgan";

import * as kafka from "./endpoints/kafka";
import { PORT } from "./env";

const app = express();

app.use(morgan("tiny"));
app.use(json());

app.get("/produce/:topic/:message", kafka.produceOneToTopic);
app.post("/produce/:topic", json(), kafka.produceManyToTopic);
app.post("/produce", json(), kafka.produceMany);

app.get("/consume/:groupId/:instanceId/:topic", kafka.consumeByTopic);
app.post("/consume/:groupId/:instanceId", kafka.consume);
app.post("/commit/:groupId/:instanceId", kafka.commit);

// Start server
app.listen(PORT, "0.0.0.0", () => {
  console.log(`Upstash Kafka Local listening at http://0.0.0.0:${PORT}`);
});
