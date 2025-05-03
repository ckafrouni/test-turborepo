import { drizzle } from "drizzle-orm/postgres-js";

import * as schema from "./schema";

if (!process.env.POSTGRES_URL) {
  throw new Error("Missing POSTGRES_URL");
}

export const db = drizzle({
  schema,
  connection: {
    url: process.env.POSTGRES_URL,
  },
  casing: "snake_case",
});
