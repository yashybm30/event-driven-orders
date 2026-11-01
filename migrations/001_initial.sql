CREATE TABLE orders(id UUID PRIMARY KEY,tenant_id UUID NOT NULL,actor_id UUID NOT NULL,idempotency_key TEXT NOT NULL,request_hash TEXT NOT NULL,items JSONB NOT NULL,total_cents INTEGER NOT NULL CHECK(total_cents>0),status TEXT NOT NULL DEFAULT 'accepted',created_at TIMESTAMPTZ NOT NULL DEFAULT now(),UNIQUE(tenant_id,idempotency_key));
CREATE TABLE outbox(id UUID PRIMARY KEY,order_id UUID NOT NULL REFERENCES orders(id),type TEXT NOT NULL,payload JSONB NOT NULL,sent_at TIMESTAMPTZ);
CREATE TABLE processed_events(event_id UUID PRIMARY KEY,processed_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE dead_letters(event_id UUID PRIMARY KEY,payload JSONB NOT NULL,error TEXT NOT NULL,created_at TIMESTAMPTZ NOT NULL DEFAULT now());
CREATE TABLE kafka_receipts(event_id UUID PRIMARY KEY,payload JSONB NOT NULL,received_at TIMESTAMPTZ NOT NULL DEFAULT now());
