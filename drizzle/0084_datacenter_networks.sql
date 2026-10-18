-- CLICK-HUMANS-1018: every prefix a cloud or hosting operator announces, refreshed
-- daily from RIPEstat (src/lib/self-send/datacenter-networks.ts). A click from one
-- of them is a link scanner's (`datacenter_network`). Hand-written: the GiST
-- index is not tracked by drizzle-kit.
CREATE TABLE IF NOT EXISTS "datacenter_networks" (
  "asn" integer NOT NULL,
  "prefix" cidr NOT NULL,
  "operator" text NOT NULL,
  "operator_domains" text[] DEFAULT '{}'::text[] NOT NULL,
  "fetched_at" timestamp with time zone NOT NULL,
  CONSTRAINT "datacenter_networks_asn_prefix_pk" PRIMARY KEY ("asn", "prefix")
);
--> statement-breakpoint
CREATE INDEX IF NOT EXISTS "datacenter_networks_prefix_gist_idx" ON "datacenter_networks" USING gist ("prefix" inet_ops);
