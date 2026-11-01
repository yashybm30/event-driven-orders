import "reflect-metadata";
import { beforeAll, afterAll, test, expect } from "vitest";
import { NestFactory } from "@nestjs/core";
import { createRequire } from "node:module";
import { randomUUID } from "node:crypto";
import jwt from "jsonwebtoken";
import request from "supertest";
const require = createRequire(import.meta.url);
const { AppModule } = require("../dist/app");
const { configure } = require("../dist/platform");
const tenant = randomUUID(),
  otherTenant = randomUUID(),
  owner = randomUUID();
function token(sub = owner, tenantId = tenant, role = "admin") {
  return jwt.sign({ sub, tenantId, role }, process.env.JWT_SECRET!, {
    issuer: "portfolio-auth",
    audience: "portfolio-api",
    expiresIn: 300,
  });
}
let app: Awaited<ReturnType<typeof NestFactory.create>>;
let api: ReturnType<typeof request>;
beforeAll(async () => {
  process.env.JWT_SECRET = "integration-only-signing-key-".repeat(3);
  process.env.PLATFORM_ADMIN_SUB = owner;
  await require("../dist/database").migrate();
  app = await NestFactory.create(AppModule, { logger: false });
  configure(app);
  await app.listen(0, "127.0.0.1");
  api = request(app.getHttpServer());
});
afterAll(async () => {
  await app?.close();
});
test("orders persist once, reject conflicting replays, isolate tenants and confirm asynchronously", async () => {
  const headers = {
      Authorization: `Bearer ${token()}`,
      "Idempotency-Key": randomUUID(),
    },
    body = { items: [{ sku: "book", quantity: 2 }] };
  const first = await api
      .post("/v1/orders")
      .set(headers)
      .send(body)
      .expect(201),
    replay = await api.post("/v1/orders").set(headers).send(body).expect(201);
  expect(replay.body.id).toBe(first.body.id);
  expect(first.body.total_cents).toBe(3000);
  await api
    .post("/v1/orders")
    .set(headers)
    .send({ items: [{ sku: "keyboard", quantity: 1 }] })
    .expect(409);
  await api
    .get(`/v1/orders/${first.body.id}`)
    .set("Authorization", `Bearer ${token(randomUUID(), otherTenant)}`)
    .expect(404);
  const { Orders } = require("../dist/app");
  await app.get(Orders).publish();
  let status = "";
  for (let i = 0; i < 50; i++) {
    status = (await api.get(`/v1/orders/${first.body.id}`).set(headers)).body
      .status;
    if (status === "confirmed") break;
    await new Promise((r) => setTimeout(r, 100));
  }
  expect(status).toBe("confirmed");
  const { Database } = require("../dist/database");
  const events = await app
    .get(Database)
    .query("SELECT * FROM outbox WHERE order_id=$1 AND type=$2", [
      first.body.id,
      "order.created",
    ]);
  await app.get(Orders).process({
    data: { eventId: events.rows[0].id, orderId: first.body.id },
    attemptsMade: 0,
  });
  expect(
    Number(
      (
        await app
          .get(Database)
          .query("SELECT count(*) FROM processed_events WHERE event_id=$1", [
            events.rows[0].id,
          ])
      ).rows[0].count,
    ),
  ).toBe(1);
});

test("exhausted jobs enter a tenant-scoped dead-letter queue and can be safely redriven", async () => {
  process.env.FAIL_FIRST_ATTEMPTS = "5";
  const auth = {
    Authorization: `Bearer ${token()}`,
    "Idempotency-Key": randomUUID(),
  };
  const { Orders } = require("../dist/app");
  const { Database } = require("../dist/database");
  try {
    const order = await api
      .post("/v1/orders")
      .set(auth)
      .send({ items: [{ sku: "book", quantity: 1 }] })
      .expect(201);
    await app.get(Orders).publish();
    let failed: { event_id: string }[] = [];
    for (let i = 0; i < 70; i++) {
      failed = (await api.get("/v1/orders/dead-letters").set(auth)).body;
      if (failed.length) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(failed.length).toBeGreaterThan(0);
    const row = await app
      .get(Database)
      .query("SELECT id FROM outbox WHERE order_id=$1 AND type=$2", [
        order.body.id,
        "order.created",
      ]);
    const eventId = row.rows[0].id;
    await api
      .post(`/v1/orders/dead-letters/${eventId}/retry`)
      .set("Authorization", `Bearer ${token(randomUUID(), otherTenant)}`)
      .expect(404);
    process.env.FAIL_FIRST_ATTEMPTS = "0";
    await api
      .post(`/v1/orders/dead-letters/${eventId}/retry`)
      .set(auth)
      .expect(201);
    let confirmed = false;
    for (let i = 0; i < 50; i++) {
      confirmed =
        (await api.get(`/v1/orders/${order.body.id}`).set(auth)).body.status ===
        "confirmed";
      if (confirmed) break;
      await new Promise((r) => setTimeout(r, 100));
    }
    expect(confirmed).toBe(true);
    expect(
      (
        await app
          .get(Database)
          .query("SELECT retry_count FROM dead_letters WHERE event_id=$1", [
            eventId,
          ])
      ).rows[0].retry_count,
    ).toBe(1);
  } finally {
    delete process.env.FAIL_FIRST_ATTEMPTS;
  }
});
test("Kafka outbox events are published and consumed when broker mode is configured", async () => {
  if (!process.env.KAFKA_BROKERS) return;
  const { Orders } = require("../dist/app");
  const { Database } = require("../dist/database");
  const auth = {
    Authorization: `Bearer ${token()}`,
    "Idempotency-Key": randomUUID(),
  };
  const order = await api
    .post("/v1/orders")
    .set(auth)
    .send({ items: [{ sku: "monitor", quantity: 1 }] })
    .expect(201);
  let consumed = false;
  for (let i = 0; i < 100; i++) {
    await app.get(Orders).publish();
    const r = await app
      .get(Database)
      .query(
        "SELECT event_id FROM kafka_receipts WHERE payload->>'orderId'=$1",
        [order.body.id],
      );
    if (r.rowCount) {
      consumed = true;
      break;
    }
    await new Promise((r) => setTimeout(r, 100));
  }
  expect(consumed).toBe(true);
});
