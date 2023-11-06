import env from "env-var";

export const KAFKA_URL = env.get("KAFKA_URL").required().asUrlString();
export const PORT = env.get("PORT").default("8080").asPortNumber();
