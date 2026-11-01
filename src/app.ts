import {
  Body,
  ConflictException,
  Controller,
  Get,
  Headers,
  Injectable,
  Module,
  NotFoundException,
  OnModuleDestroy,
  OnModuleInit,
  Param,
  Post,
  Req,
  UseGuards,
  BadRequestException,
} from "@nestjs/common";
import {
  ArrayMaxSize,
  ArrayMinSize,
  IsArray,
  IsInt,
  IsString,
  Max,
  Min,
  ValidateNested,
  Matches,
} from "class-validator";
import { Type } from "class-transformer";
import { createHash, randomUUID } from "node:crypto";
import { Queue, Worker, Job } from "bullmq";
import { Kafka, Producer, Consumer } from "kafkajs";
import { Database } from "./database";
import { AuthGuard, AuthRequest, admin } from "./security";
class Item {
  @IsString() @Matches(/^[a-z0-9-]{1,50}$/) sku!: string;
  @IsInt() @Min(1) @Max(20) quantity!: number;
}
class NewOrder {
  @IsArray()
  @ArrayMinSize(1)
  @ArrayMaxSize(20)
  @ValidateNested({ each: true })
  @Type(() => Item)
  items!: Item[];
}
export const catalog: Record<string, number> = {
  book: 1500,
  keyboard: 7500,
  monitor: 25000,
};
export function price(items: Item[]) {
  if (
    items.length < 1 ||
    items.length > 20 ||
    items.some(
      (item) =>
        !Number.isInteger(item.quantity) ||
        item.quantity < 1 ||
        item.quantity > 20,
    )
  )
    throw new BadRequestException("Invalid order quantity");
  let total = 0;
  for (const item of items) {
    const unit = catalog[item.sku];
    if (!unit) throw new BadRequestException("Unknown SKU");
    total += unit * item.quantity;
  }
  return total;
}
export const queueConnection = () => {
  const url = new URL(process.env.REDIS_URL!);
  return {
    host: url.hostname,
    port: Number(url.port || 6379),
    password: url.password || undefined,
    db: Number(url.pathname.slice(1) || 0),
    ...(url.protocol === "rediss:" ? { tls: {} } : {}),
  };
};
@Injectable()
export class Orders implements OnModuleInit, OnModuleDestroy {
  readonly queue = new Queue("orders", {
    connection: queueConnection(),
    prefix: process.env.QUEUE_PREFIX ?? "portfolio",
  });
  private worker?: Worker;
  private timer?: NodeJS.Timeout;
  private running = false;
  private producer?: Producer;
  private consumer?: Consumer;
  constructor(private readonly db: Database) {}
  async onModuleInit() {
    if (process.env.KAFKA_BROKERS) {
      const kafka = new Kafka({
        clientId: "orders",
        brokers: process.env.KAFKA_BROKERS.split(","),
        retry: { retries: 3 },
      });
      this.producer = kafka.producer();
      await this.producer.connect();
      const admin = kafka.admin();
      await admin.connect();
      await admin.createTopics({
        topics: [
          { topic: "orders.events", numPartitions: 1, replicationFactor: 1 },
        ],
      });
      await admin.disconnect();
      this.consumer = kafka.consumer({
        groupId: process.env.KAFKA_GROUP ?? "orders-receipts",
      });
      await this.consumer.connect();
      await this.consumer.subscribe({
        topic: "orders.events",
        fromBeginning: true,
      });
      await this.consumer.run({
        eachMessage: async ({ message }) => {
          if (!message.value) return;
          const event = JSON.parse(message.value.toString()) as { id: string };
          if (!/^[a-f0-9-]{36}$/.test(event.id))
            throw new Error("Invalid event");
          await this.db.query(
            "INSERT INTO kafka_receipts(event_id,payload) VALUES($1,$2) ON CONFLICT DO NOTHING",
            [event.id, event],
          );
        },
      });
    }
    this.worker = new Worker("orders", (job) => this.process(job), {
      connection: queueConnection(),
      prefix: process.env.QUEUE_PREFIX ?? "portfolio",
      concurrency: 4,
    });
    this.worker.on("error", (e) =>
      console.error(
        JSON.stringify({ event: "worker-error", message: e.message }),
      ),
    );
    this.worker.on("failed", (job, error) => {
      if (job && job.attemptsMade >= (job.opts.attempts ?? 1))
        void this.db
          .query(
            "INSERT INTO dead_letters(event_id,payload,error) VALUES($1,$2,$3) ON CONFLICT DO NOTHING",
            [job.data.eventId, job.data, error.message.slice(0, 500)],
          )
          .catch(() => console.error("dead-letter-write-failed"));
    });
    this.timer = setInterval(() => {
      void this.publish().catch(() => console.error("outbox-publish-failed"));
    }, 500);
    this.timer.unref();
  }
  async process(job: Job) {
    if (job.attemptsMade < Number(process.env.FAIL_FIRST_ATTEMPTS ?? 0))
      throw new Error("Configured transient failure");
    return this.db.transaction(async (tx) => {
      const first = await tx.query(
        "INSERT INTO processed_events(event_id) VALUES($1) ON CONFLICT DO NOTHING RETURNING event_id",
        [job.data.eventId],
      );
      if (!first.rowCount) return { duplicate: true };
      const order = await tx.query(
        "UPDATE orders SET status=$2 WHERE id=$1 AND status=$3 RETURNING *",
        [job.data.orderId, "confirmed", "accepted"],
      );
      if (order.rowCount) {
        const eventId = randomUUID();
        await tx.query(
          "INSERT INTO outbox(id,order_id,type,payload) VALUES($1,$2,$3,$4)",
          [
            eventId,
            job.data.orderId,
            "order.confirmed",
            {
              id: eventId,
              orderId: job.data.orderId,
              tenantId: order.rows[0].tenant_id,
              type: "order.confirmed",
            },
          ],
        );
      }
      return { confirmed: true };
    });
  }
  async publish() {
    if (this.running) return;
    this.running = true;
    try {
      await this.db.transaction(async (tx) => {
        const rows = await tx.query(
          "SELECT * FROM outbox WHERE sent_at IS NULL ORDER BY id LIMIT 20 FOR UPDATE SKIP LOCKED",
        );
        for (const event of rows.rows) {
          if (event.type === "order.created")
            await this.queue.add(
              "confirm",
              { eventId: event.id, orderId: event.order_id },
              {
                jobId: event.id,
                attempts: 3,
                backoff: { type: "exponential", delay: 250 },
                removeOnComplete: { age: 86400 },
                removeOnFail: false,
              },
            );
          else {
            if (!this.producer) continue;
            await this.producer.send({
              topic: "orders.events",
              messages: [
                { key: event.order_id, value: JSON.stringify(event.payload) },
              ],
            });
          }
          await tx.query("UPDATE outbox SET sent_at=now() WHERE id=$1", [
            event.id,
          ]);
        }
      });
    } finally {
      this.running = false;
    }
  }
  async onModuleDestroy() {
    if (this.timer) clearInterval(this.timer);
    while (this.running) await new Promise((r) => setTimeout(r, 20));
    await this.worker?.close();
    await this.queue.close();
    await this.consumer?.disconnect();
    await this.producer?.disconnect();
  }
}
@Controller("v1/orders")
@UseGuards(AuthGuard)
class Api {
  constructor(
    private readonly db: Database,
    private readonly orders: Orders,
  ) {}
  @Post() async create(
    @Body() input: NewOrder,
    @Headers("idempotency-key") key: string,
    @Req() req: AuthRequest,
  ) {
    if (!key || !/^[a-zA-Z0-9-]{8,100}$/.test(key))
      throw new BadRequestException(
        "An 8–100 character Idempotency-Key is required",
      );
    const total = price(input.items),
      hash = createHash("sha256")
        .update(JSON.stringify(input.items))
        .digest("hex");
    return this.db.transaction(async (tx) => {
      const id = randomUUID();
      const inserted = await tx.query(
        "INSERT INTO orders(id,tenant_id,actor_id,idempotency_key,request_hash,items,total_cents) VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT(tenant_id,idempotency_key) DO NOTHING RETURNING *",
        [
          id,
          req.actor.tenantId,
          req.actor.sub,
          key,
          hash,
          JSON.stringify(input.items),
          total,
        ],
      );
      if (!inserted.rowCount) {
        const existing = await tx.query(
          "SELECT * FROM orders WHERE tenant_id=$1 AND idempotency_key=$2",
          [req.actor.tenantId, key],
        );
        if (existing.rows[0].request_hash !== hash)
          throw new ConflictException(
            "Idempotency key reused with a different order",
          );
        return existing.rows[0];
      }
      const eventId = randomUUID();
      await tx.query(
        "INSERT INTO outbox(id,order_id,type,payload) VALUES($1,$2,$3,$4)",
        [eventId, id, "order.created", { orderId: id }],
      );
      return inserted.rows[0];
    });
  }
  @Get() async list(@Req() req: AuthRequest) {
    return (
      await this.db.query(
        "SELECT id,status,total_cents,created_at FROM orders WHERE tenant_id=$1 AND actor_id=$2 ORDER BY created_at DESC LIMIT 50",
        [req.actor.tenantId, req.actor.sub],
      )
    ).rows;
  }
  @Get("dead-letters") async dead(@Req() req: AuthRequest) {
    admin(req.actor);
    return (
      await this.db.query(
        "SELECT d.* FROM dead_letters d JOIN outbox e ON e.id=d.event_id JOIN orders o ON o.id=e.order_id WHERE o.tenant_id=$1 LIMIT 50",
        [req.actor.tenantId],
      )
    ).rows;
  }
  @Post("dead-letters/:eventId/retry") async retry(
    @Param("eventId") eventId: string,
    @Req() req: AuthRequest,
  ) {
    admin(req.actor);
    if (!/^[a-f0-9-]{36}$/.test(eventId)) throw new NotFoundException();
    const record = await this.db.query(
      "SELECT d.event_id FROM dead_letters d JOIN outbox e ON e.id=d.event_id JOIN orders o ON o.id=e.order_id WHERE d.event_id=$1 AND o.tenant_id=$2",
      [eventId, req.actor.tenantId],
    );
    if (!record.rowCount) throw new NotFoundException();
    const job = await this.orders.queue.getJob(eventId);
    if (!job || (await job.getState()) !== "failed")
      throw new ConflictException("Job is not in a failed state");
    await job.retry("failed");
    await this.db.query(
      "UPDATE dead_letters SET retried_at=now(),retry_count=retry_count+1 WHERE event_id=$1",
      [eventId],
    );
    return { queued: true };
  }
  @Get(":id") async get(@Param("id") id: string, @Req() req: AuthRequest) {
    if (!/^[a-f0-9-]{36}$/.test(id)) throw new NotFoundException();
    const r = await this.db.query(
      "SELECT * FROM orders WHERE id=$1 AND tenant_id=$2 AND actor_id=$3",
      [id, req.actor.tenantId, req.actor.sub],
    );
    if (!r.rowCount) throw new NotFoundException();
    return r.rows[0];
  }
}
@Controller("health")
class Health {
  constructor(
    private readonly db: Database,
    private readonly orders: Orders,
  ) {}
  @Get("live") live() {
    return { status: "alive" };
  }
  @Get("ready") async ready() {
    await this.db.query("SELECT 1 FROM orders LIMIT 1");
    await this.orders.queue.getJobCounts();
    return { status: "ready" };
  }
}
@Module({
  controllers: [Api, Health],
  providers: [Database, Orders, AuthGuard],
})
export class AppModule {}
