// NDX WhatsApp Yard — Cloudflare Worker
//
//  /link?key=...        QR page to link WhatsApp (Settings > Linked Devices > Link a Device)
//  /mcp/<key>           Claude connector (read the group-message database, view photos,
//                       and send a message to a group)
//
// The Bridge Durable Object keeps the whatsmeow container running, moves buffered
// group messages into D1 every minute, and backs the WhatsApp login up to R2.
import { DurableObject } from "cloudflare:workers";
import QRCode from "qrcode";

declare global {
	interface Env {
		ACCESS_KEY?: string;
	}
}

const PORT = 8080;
const TICK_MS = 60_000; // flush cadence
const BACKUP_EVERY_MS = 10 * 60_000;
const GROUPS_EVERY_MS = 30 * 60_000;
const INACTIVITY_MS = 6 * 60 * 60_000; // max allowed; the 1-minute alarm keeps it alive anyway
const SESSION_KEY = "whatsapp/session.db";
const TZ = "America/Los_Angeles";
const MEDIA_PREFIX = "media/"; // R2 key prefix for saved photos
const MEDIA_PER_TICK = 10;
const MAX_IMAGE_BYTES = 4_500_000;
// One-time photo catch-up for messages stored before photo saving existed.
const BACKFILL_VERSION = 2; // 1 ran against the old bridge and did nothing
// The bridge image this Worker expects (bridge/main.go bridgeVersion).
const BRIDGE_VERSION = 2;
const RESTART_COOLDOWN_MS = 30 * 60_000;
const BACKFILL_DAYS = 14;
const BACKFILL_COUNT = 100;
// Send limits, to keep the unofficial bridge from looking like a bot to WhatsApp.
const SEND_MAX_PER_HOUR = 20;
const SEND_MIN_GAP_MS = 5_000;
const SEND_MAX_CHARS = 2_000;

const SCHEMA = [
	`CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY, chat_jid TEXT NOT NULL, chat_name TEXT, sender_jid TEXT, sender_name TEXT,
		ts INTEGER NOT NULL, text TEXT, kind TEXT, quoted_id TEXT, quoted_text TEXT, from_me INTEGER DEFAULT 0,
		stored_at INTEGER)`,
	`CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (chat_jid, ts)`,
	`CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts)`,
	`CREATE TABLE IF NOT EXISTS groups (jid TEXT PRIMARY KEY, name TEXT, participants INTEGER, updated_at INTEGER)`,
];
// Added later; each may already exist, so errors are ignored.
const MIGRATIONS = [
	`ALTER TABLE messages ADD COLUMN media TEXT`, // base64 WhatsApp ImageMessage (download keys)
	`ALTER TABLE messages ADD COLUMN media_type TEXT`,
	`ALTER TABLE messages ADD COLUMN media_saved INTEGER`, // 1 = photo copied to R2, -1 = download failed
];
// Columns the connector reads (never the bulky media blob).
const COLS = `m.id, m.chat_jid, m.chat_name, m.sender_jid, m.sender_name, m.ts, m.text, m.kind,
	m.quoted_id, m.quoted_text, m.from_me, (m.media IS NOT NULL OR m.media_saved = 1) AS has_media`;

type BridgeStatus = {
	version?: number;
	state: string;
	detail: string;
	qr: string;
	jid: string;
	started: boolean;
	buffered: number;
	groups: number;
	last_event: number;
};

type StatusReport = {
	bridge: Partial<BridgeStatus> & { state: string; detail: string };
	stored_messages: number;
	groups_with_messages: number;
	last_message: string | null;
	last_tick: number | null;
	last_backup: number | null;
	last_error: string | null;
};

type BufferedMsg = {
	id: string;
	chat_jid: string;
	chat_name: string;
	sender_jid: string;
	sender_name: string;
	ts: number;
	text: string;
	kind: string;
	quoted_id?: string;
	quoted_text?: string;
	from_me: boolean;
	media?: string;
	media_type?: string;
};

type SendArgs = { group?: string; text?: string; reply_to?: string };

// ───────────────────────── Bridge Durable Object ─────────────────────────

export class Bridge extends DurableObject<Env> {
	private starting: Promise<void> | undefined;
	private schemaReady = false;

	constructor(ctx: DurableObjectState, env: Env) {
		super(ctx, env);
		const c = ctx.container;
		if (c?.running) {
			void ctx.blockConcurrencyWhile(() => c.setInactivityTimeout(INACTIVITY_MS));
			this.watchExit();
		}
	}

	/** Called by cron and by the QR page: make sure the loop is alive. */
	async poke(): Promise<void> {
		await this.ensureRunning();
		if ((await this.ctx.storage.getAlarm()) === null) {
			await this.ctx.storage.setAlarm(Date.now() + 5_000);
		}
	}

	async alarm(): Promise<void> {
		try {
			await this.ensureRunning();
			await this.ensureSchema();
			const status = await this.bootSession();
			await this.flush();
			if (await this.restartIfOutdated(status)) return;
			if (status?.state === "connected") {
				await this.maybeBackup();
				await this.maybeSyncGroups();
				if ((status.version ?? 1) >= BRIDGE_VERSION) await this.maybeBackfillPhotos();
				await this.saveMedia();
			}
			await this.ctx.storage.put("lastTick", Date.now());
		} catch (err) {
			console.error("tick failed", err);
			await this.ctx.storage.put("lastError", `${new Date().toISOString()} ${String(err)}`);
		} finally {
			await this.ctx.storage.setAlarm(Date.now() + TICK_MS);
		}
	}

	async status(): Promise<StatusReport> {
		await this.poke();
		let bridge: StatusReport["bridge"] = { state: "starting", detail: "container booting" };
		try {
			bridge = await this.call<BridgeStatus>("/status");
		} catch (err) {
			bridge = { state: "starting", detail: String(err) };
		}
		await this.ensureSchema();
		const stats = await this.env.DB.prepare(
			`SELECT COUNT(*) AS n, MAX(ts) AS last_ts, COUNT(DISTINCT chat_jid) AS chats FROM messages`,
		).first<{ n: number; last_ts: number | null; chats: number }>();
		return {
			bridge,
			stored_messages: stats?.n ?? 0,
			groups_with_messages: stats?.chats ?? 0,
			last_message: stats?.last_ts ? fmtTime(stats.last_ts) : null,
			last_tick: (await this.ctx.storage.get<number>("lastTick")) ?? null,
			last_backup: (await this.ctx.storage.get<number>("lastBackup")) ?? null,
			last_error: (await this.ctx.storage.get<string>("lastError")) ?? null,
		};
	}

	/** Forget the WhatsApp login everywhere and show a fresh QR code. */
	async reset(): Promise<void> {
		await this.ensureRunning();
		await this.env.BLOBS.delete(SESSION_KEY);
		await this.ctx.storage.delete("lastBackup");
		await this.call("/reset", { method: "POST" });
	}

	/** Returns a photo as base64, from R2 or freshly downloaded from WhatsApp. */
	async getMedia(id: string): Promise<{ data: string; type: string; bytes: number }> {
		await this.ensureSchema();
		const row = await this.env.DB.prepare(`SELECT id, media, media_type FROM messages WHERE id = ?`)
			.bind(id)
			.first<{ id: string; media: string | null; media_type: string | null }>();
		if (!row) throw new Error(`No message with id ${id}`);
		let obj = await this.env.BLOBS.get(MEDIA_PREFIX + id);
		let bytes: ArrayBuffer;
		let type = row.media_type || "image/jpeg";
		if (obj) {
			bytes = await obj.arrayBuffer();
			type = obj.httpMetadata?.contentType || type;
		} else {
			if (!row.media) throw new Error("That message has no downloadable photo (photos from before Nov 25, 2026 weren't saved).");
			await this.ensureRunning();
			bytes = await this.downloadAndSave(id, row.media, type);
		}
		if (bytes.byteLength > MAX_IMAGE_BYTES) throw new Error(`Photo is too large to show (${bytes.byteLength} bytes).`);
		return { data: toBase64(bytes), type, bytes: bytes.byteLength };
	}

	/** Sends one text message to one group, with rate limits. */
	async send(chatJid: string, text: string, reply?: { id: string; sender: string; text: string }): Promise<{ id: string; ts: number }> {
		const now = Date.now();
		const log = ((await this.ctx.storage.get<number[]>("sendLog")) ?? []).filter((t) => now - t < 3600_000);
		if (log.length >= SEND_MAX_PER_HOUR) throw new Error(`Send limit reached (${SEND_MAX_PER_HOUR} per hour). Try again later.`);
		const lastSend = log.length ? log[log.length - 1] : 0;
		if (now - lastSend < SEND_MIN_GAP_MS) await sleep(SEND_MIN_GAP_MS - (now - lastSend));
		await this.ensureRunning();
		const res = await this.call<{ ok: boolean; id: string; ts: number }>("/send", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({
				chat_jid: chatJid,
				text,
				reply_to_id: reply?.id,
				reply_to_sender: reply?.sender,
				reply_to_text: reply?.text,
			}),
		});
		log.push(Date.now());
		await this.ctx.storage.put("sendLog", log);
		await this.ensureSchema();
		await this.flush(); // put the sent message in D1 right away
		return { id: res.id, ts: res.ts };
	}

	// ── internals ──

	/**
	 * A running container keeps its old image across deploys. If it reports an older
	 * bridge version, back up the login, stop it, and let the next tick start the new
	 * image (the login is restored from R2, so no QR scan). At most once per cooldown.
	 */
	private async restartIfOutdated(status: BridgeStatus): Promise<boolean> {
		if ((status.version ?? 1) >= BRIDGE_VERSION) return false;
		const last = (await this.ctx.storage.get<number>("lastForcedRestart")) ?? 0;
		if (Date.now() - last < RESTART_COOLDOWN_MS) return false;
		await this.ctx.storage.put("lastForcedRestart", Date.now());
		if (status.state === "connected") {
			await this.ctx.storage.delete("lastBackup");
			await this.maybeBackup(); // fresh login backup before stopping
		}
		console.log(`bridge v${status.version ?? 1} is outdated; restarting container for v${BRIDGE_VERSION}`);
		await this.ctx.container!.destroy();
		return true;
	}

	private async downloadAndSave(id: string, media: string, type: string): Promise<ArrayBuffer> {
		const res = await this.ctx.container!.getTcpPort(PORT).fetch("http://container/media", {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ media }),
		});
		if (!res.ok) {
			const why = await res.text();
			if (res.status === 502) {
				await this.env.DB.prepare(`UPDATE messages SET media_saved = -1 WHERE id = ?`).bind(id).run();
			}
			throw new Error(`Couldn't download the photo: ${why.trim()}`);
		}
		const bytes = await res.arrayBuffer();
		await this.env.BLOBS.put(MEDIA_PREFIX + id, bytes, {
			httpMetadata: { contentType: res.headers.get("content-type") || type },
		});
		await this.env.DB.prepare(`UPDATE messages SET media_saved = 1 WHERE id = ?`).bind(id).run();
		return bytes;
	}

	/**
	 * One-time catch-up for photos stored before photo saving existed: ask the phone to
	 * resend recent history for each group that has such photos. The resent messages
	 * only fill in the missing download keys (see the ON CONFLICT in flush).
	 * Runs one group per tick; set storage key "backfillVersion" lower to run again.
	 */
	private async maybeBackfillPhotos(): Promise<void> {
		if (((await this.ctx.storage.get<number>("backfillVersion")) ?? 0) >= BACKFILL_VERSION) return;
		const done = (await this.ctx.storage.get<string[]>(`backfillDone:v${BACKFILL_VERSION}`)) ?? [];
		const chats = await this.env.DB.prepare(
			`SELECT chat_jid FROM messages
			 WHERE kind = 'image' AND media IS NULL AND ts > unixepoch() - ?
			 GROUP BY chat_jid ORDER BY MAX(ts) DESC`,
		)
			.bind(BACKFILL_DAYS * 86400)
			.all<{ chat_jid: string }>();
		const next = chats.results.find((c) => !done.includes(c.chat_jid));
		if (!next) {
			await this.ctx.storage.put("backfillVersion", BACKFILL_VERSION);
			await this.ctx.storage.delete(`backfillDone:v${BACKFILL_VERSION}`);
			console.log("photo backfill finished");
			return;
		}
		// Anchor on the newest message we have; the phone sends older ones.
		const anchor = await this.env.DB.prepare(
			`SELECT id, ts, from_me FROM messages WHERE chat_jid = ? ORDER BY ts DESC LIMIT 1`,
		)
			.bind(next.chat_jid)
			.first<{ id: string; ts: number; from_me: number }>();
		if (!anchor) {
			done.push(next.chat_jid);
			await this.ctx.storage.put(`backfillDone:v${BACKFILL_VERSION}`, done);
			return;
		}
		try {
			await this.call("/backfill", {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({
					chat_jid: next.chat_jid,
					anchor_id: anchor.id.slice(next.chat_jid.length + 1),
					anchor_ts: anchor.ts,
					anchor_from_me: !!anchor.from_me,
					count: BACKFILL_COUNT,
				}),
			});
			console.log(`photo backfill requested for ${next.chat_jid}`);
			done.push(next.chat_jid);
			await this.ctx.storage.put(`backfillDone:v${BACKFILL_VERSION}`, done);
		} catch (err) {
			console.warn("photo backfill request failed", next.chat_jid, String(err));
		}
	}

	/** Copies new photos into R2 soon after they arrive, before WhatsApp's links expire. */
	private async saveMedia(): Promise<void> {
		const rows = await this.env.DB.prepare(
			`SELECT id, media, media_type FROM messages
			 WHERE media IS NOT NULL AND media_saved IS NULL AND ts > unixepoch() - 30*86400
			 ORDER BY ts DESC LIMIT ?`,
		)
			.bind(MEDIA_PER_TICK)
			.all<{ id: string; media: string; media_type: string | null }>();
		for (const r of rows.results) {
			try {
				await this.downloadAndSave(r.id, r.media, r.media_type || "image/jpeg");
			} catch (err) {
				console.warn("photo save failed", r.id, String(err));
			}
		}
	}


	private async ensureRunning(): Promise<void> {
		this.starting ??= this.startAndWait().finally(() => {
			this.starting = undefined;
		});
		await this.starting;
	}

	private async startAndWait(): Promise<void> {
		const c = this.ctx.container!;
		if (!c.running) {
			c.start({ image: c.images.base, instance: "lite", enableInternet: true });
			this.watchExit();
		}
		await c.setInactivityTimeout(INACTIVITY_MS);
		for (let i = 0; i < 150; i++) {
			try {
				const res = await c.getTcpPort(PORT).fetch("http://container/health");
				if (res.ok) return;
			} catch {
				// not listening yet
			}
			await sleep(200);
		}
		throw new Error("bridge container did not become ready");
	}

	private watchExit(): void {
		this.ctx.waitUntil(
			this.ctx
				.container!.monitor()
				.then(() => console.log("bridge container exited"))
				.catch((err) => console.error("bridge container errored", err)),
		);
	}

	private async call<T = unknown>(path: string, init?: RequestInit): Promise<T> {
		const res = await this.ctx.container!.getTcpPort(PORT).fetch(`http://container${path}`, init);
		if (!res.ok) throw new Error(`${path}: HTTP ${res.status} ${await res.text()}`);
		const type = res.headers.get("content-type") ?? "";
		return (type.includes("json") ? res.json() : res.arrayBuffer()) as Promise<T>;
	}

	/** After a (re)start, hand the container its saved login so no QR scan is needed. */
	private async bootSession(): Promise<BridgeStatus> {
		let status = await this.call<BridgeStatus>("/status");
		if (status.state === "waiting_session" && !status.started) {
			const saved = await this.env.BLOBS.get(SESSION_KEY);
			const body = saved ? await saved.arrayBuffer() : new ArrayBuffer(0);
			await this.call("/session/import", { method: "POST", body });
			console.log(saved ? "restored saved WhatsApp session" : "no saved session; QR code will be shown");
			status = await this.call<BridgeStatus>("/status");
		}
		return status;
	}

	private async ensureSchema(): Promise<void> {
		if (this.schemaReady) return;
		await ensureDbSchema(this.env.DB);
		this.schemaReady = true;
	}

	private async flush(): Promise<void> {
		const insert = this.env.DB.prepare(
			`INSERT OR IGNORE INTO messages
			 (id, chat_jid, chat_name, sender_jid, sender_name, ts, text, kind, quoted_id, quoted_text, from_me, stored_at,
			  media, media_type)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			 ON CONFLICT(id) DO UPDATE SET
			   media = COALESCE(messages.media, excluded.media),
			   media_type = COALESCE(messages.media_type, excluded.media_type)`,
		);
		for (let round = 0; round < 20; round++) {
			const items = await this.call<{ seq: number; msg: BufferedMsg }[]>("/messages?after=0&limit=500");
			if (!items.length) return;
			const now = Math.floor(Date.now() / 1000);
			for (let i = 0; i < items.length; i += 100) {
				const chunk = items.slice(i, i + 100).map(({ msg: m }) =>
					insert.bind(
						m.id, m.chat_jid, m.chat_name || null, m.sender_jid || null, m.sender_name || null, m.ts,
						m.text || null, m.kind || null, m.quoted_id || null, m.quoted_text || null, m.from_me ? 1 : 0, now,
						m.media || null, m.media_type || null,
					),
				);
				await this.env.DB.batch(chunk);
			}
			const upto = items[items.length - 1].seq;
			await this.call(`/ack?upto=${upto}`, { method: "POST" });
		}
	}

	private async maybeBackup(): Promise<void> {
		const last = (await this.ctx.storage.get<number>("lastBackup")) ?? 0;
		if (Date.now() - last < BACKUP_EVERY_MS) return;
		const data = await this.call<ArrayBuffer>("/session/export");
		if (data.byteLength < 4096) return; // empty/unlinked store; don't overwrite a good backup
		await this.env.BLOBS.put(SESSION_KEY, data);
		await this.ctx.storage.put("lastBackup", Date.now());
		console.log(`backed up WhatsApp session (${data.byteLength} bytes)`);
	}

	private async maybeSyncGroups(): Promise<void> {
		const last = (await this.ctx.storage.get<number>("lastGroups")) ?? 0;
		if (Date.now() - last < GROUPS_EVERY_MS) return;
		const groups = await this.call<{ jid: string; name: string; participants: number }[]>("/groups");
		const now = Math.floor(Date.now() / 1000);
		const upsert = this.env.DB.prepare(
			`INSERT INTO groups (jid, name, participants, updated_at) VALUES (?, ?, ?, ?)
			 ON CONFLICT(jid) DO UPDATE SET name = excluded.name, participants = excluded.participants, updated_at = excluded.updated_at`,
		);
		for (let i = 0; i < groups.length; i += 100) {
			await this.env.DB.batch(groups.slice(i, i + 100).map((g) => upsert.bind(g.jid, g.name, g.participants, now)));
		}
		await this.ctx.storage.put("lastGroups", Date.now());
	}
}

// ───────────────────────── Schema ─────────────────────────

let dbSchemaReady = false;
/** Creates tables and adds newer columns. Safe to run any number of times. */
async function ensureDbSchema(db: D1Database): Promise<void> {
	if (dbSchemaReady) return;
	await db.batch(SCHEMA.map((s) => db.prepare(s)));
	const cols = await db.prepare(`SELECT name FROM pragma_table_info('messages')`).all<{ name: string }>();
	const have = new Set(cols.results.map((c) => c.name));
	for (const m of MIGRATIONS) {
		const col = m.match(/ADD COLUMN (\w+)/)?.[1];
		if (col && !have.has(col)) await db.prepare(m).run();
	}
	dbSchemaReady = true;
}

// ───────────────────────── Worker entry ─────────────────────────

export default {
	async fetch(request, env, ctx): Promise<Response> {
		const url = new URL(request.url);
		if (!env.ACCESS_KEY) {
			return new Response("Set the ACCESS_KEY secret on this Worker first.", { status: 503 });
		}
		const bridge = env.BRIDGE.getByName("main");

		// Claude connector
		const mcp = url.pathname.match(/^\/mcp\/([^/]+)\/?$/);
		if (mcp) {
			if (!safeEqual(decodeURIComponent(mcp[1]), env.ACCESS_KEY)) return new Response("Not found", { status: 404 });
			return handleMcp(request, env, bridge as unknown as DurableObjectStub<Bridge>);
		}

		// Linking page
		if (url.pathname.startsWith("/link")) {
			if (!safeEqual(url.searchParams.get("key") ?? "", env.ACCESS_KEY)) return new Response("Not found", { status: 404 });
			const stub = bridge as unknown as DurableObjectStub<Bridge>;
			if (url.pathname === "/link/status") {
				const s = await stub.status();
				const qr = s.bridge.qr;
				const svg = qr ? await QRCode.toString(qr, { type: "svg", margin: 2, width: 280 }) : "";
				return Response.json({ ...s, bridge: { ...s.bridge, qr: undefined }, svg });
			}
			if (url.pathname === "/link/reset" && request.method === "POST") {
				await stub.reset();
				return Response.json({ ok: true });
			}
			ctx.waitUntil(stub.poke());
			return new Response(LINK_PAGE, { headers: { "content-type": "text/html; charset=utf-8" } });
		}

		// Only the bare root answers. Everything else (incl. /.well-known/oauth-* discovery)
		// must 404 so Claude treats the connector as no-auth instead of looking for a sign-in service.
		if (url.pathname === "/") return new Response("NDX WhatsApp Yard", { status: 200 });
		return new Response("Not found", { status: 404 });
	},

	async scheduled(_event, env, ctx): Promise<void> {
		const stub = env.BRIDGE.getByName("main") as unknown as DurableObjectStub<Bridge>;
		ctx.waitUntil(stub.poke());
	},
} satisfies ExportedHandler<Env>;

// ───────────────────────── Claude connector (MCP) ─────────────────────────

const TOOLS = [
	{
		name: "get_messages",
		description:
			"Read WhatsApp group messages from NDX's parts/shop groups, oldest first. Leave `group` empty to get every group (use this for 'what happened in the last hour'). Times are Pacific.",
		inputSchema: {
			type: "object",
			properties: {
				group: { type: "string", description: "Group name (partial match OK) or group JID. Empty = all groups." },
				since_hours: { type: "number", description: "How far back to look, in hours. Default 24." },
				limit: { type: "number", description: "Max messages (newest kept). Default 300, max 1000." },
			},
		},
		annotations: { readOnlyHint: true },
	},
	{
		name: "search_messages",
		description:
			"Search WhatsApp group messages for words (all words must appear), e.g. a part number, machine number, vendor or 'ETA'. Times are Pacific.",
		inputSchema: {
			type: "object",
			properties: {
				query: { type: "string", description: "Words to find, e.g. '336 hydraulic pump'." },
				group: { type: "string", description: "Optional group name filter (partial match OK)." },
				days: { type: "number", description: "How many days back. Default 30." },
				limit: { type: "number", description: "Max results. Default 100." },
			},
			required: ["query"],
		},
		annotations: { readOnlyHint: true },
	},
	{
		name: "get_media",
		description:
			"Show a photo from a WhatsApp group message. Messages with a photo end in `(photo)` and every message ends in `#<id>`; pass that id. Use it to look at part photos, nameplates, damage, packing slips.",
		inputSchema: {
			type: "object",
			properties: {
				message_id: { type: "string", description: "The id shown after # on the message line." },
			},
			required: ["message_id"],
		},
		annotations: { readOnlyHint: true },
	},
	{
		name: "send_message",
		description:
			"Send a text message to ONE WhatsApp group as Kaden. Only send text Kaden has seen and approved word for word in this conversation. Groups only. To reply to a specific message, pass its id from get_messages/search_messages as reply_to. Limited to 20 sends per hour.",
		inputSchema: {
			type: "object",
			properties: {
				group: { type: "string", description: "Group name (must match one group; partial OK if unique) or group JID." },
				text: { type: "string", description: "The exact message to send. Max 2,000 characters." },
				reply_to: { type: "string", description: "Optional: id of the message to quote-reply to (shown after #)." },
			},
			required: ["group", "text"],
		},
		annotations: { readOnlyHint: false, destructiveHint: false, idempotentHint: false, openWorldHint: true },
	},
	{
		name: "list_groups",
		description: "List the WhatsApp groups being synced, with message counts and last activity.",
		inputSchema: { type: "object", properties: {} },
		annotations: { readOnlyHint: true },
	},
	{
		name: "bridge_status",
		description: "Health of the WhatsApp sync: linked or not, messages stored, last message, last backup, errors.",
		inputSchema: { type: "object", properties: {} },
		annotations: { readOnlyHint: true },
	},
];

type RpcReq = { jsonrpc: "2.0"; id?: string | number | null; method: string; params?: any };

async function handleMcp(request: Request, env: Env, bridge: DurableObjectStub<Bridge>): Promise<Response> {
	if (request.method === "GET") return new Response("Method not allowed", { status: 405, headers: { allow: "POST" } });
	if (request.method === "DELETE") return new Response(null, { status: 204 });
	if (request.method !== "POST") return new Response("Method not allowed", { status: 405 });

	let body: RpcReq | RpcReq[];
	try {
		body = await request.json();
	} catch {
		return Response.json({ jsonrpc: "2.0", id: null, error: { code: -32700, message: "Parse error" } }, { status: 400 });
	}
	const batch = Array.isArray(body) ? body : [body];
	const out = [];
	for (const msg of batch) {
		const res = await handleRpc(msg, env, bridge);
		if (res) out.push(res);
	}
	if (!out.length) return new Response(null, { status: 202 });
	return Response.json(Array.isArray(body) ? out : out[0]);
}

async function handleRpc(msg: RpcReq, env: Env, bridge: DurableObjectStub<Bridge>) {
	const reply = (result: unknown) => ({ jsonrpc: "2.0", id: msg.id ?? null, result });
	if (msg.id === undefined || msg.id === null) return null; // notification
	switch (msg.method) {
		case "initialize":
			return reply({
				protocolVersion: msg.params?.protocolVersion ?? "2025-06-18",
				capabilities: { tools: {} },
				serverInfo: { name: "ndx-whatsapp-yard", version: "1.1.0" },
				instructions:
					"Access to NDX's WhatsApp group chats (parts orders, ETAs, shop projects). Times are Pacific, 12-hour. Each message line ends in #<id>; photos are marked (photo) and can be viewed with get_media. send_message posts as Kaden: show him the exact text and group first and send only after he approves it.",
			});
		case "ping":
			return reply({});
		case "tools/list":
			return reply({ tools: TOOLS });
		case "tools/call": {
			try {
				const out = await callTool(msg.params?.name, msg.params?.arguments ?? {}, env, bridge);
				if (typeof out !== "string") return reply({ content: out });
				return reply({ content: [{ type: "text", text: out }] });
			} catch (err) {
				return reply({ content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true });
			}
		}
		default:
			return { jsonrpc: "2.0", id: msg.id, error: { code: -32601, message: `Unknown method ${msg.method}` } };
	}
}

type Content = { type: "text"; text: string } | { type: "image"; data: string; mimeType: string };

async function callTool(name: string, args: any, env: Env, bridge: DurableObjectStub<Bridge>): Promise<string | Content[]> {
	const db = env.DB;
	await ensureDbSchema(db);
	switch (name) {
		case "get_media": {
			const row = await findMessage(db, args.message_id);
			const img = await bridge.getMedia(row.id);
			return [
				{ type: "image", data: img.data, mimeType: img.type },
				{ type: "text", text: `Photo from ${row.gname}, ${row.who} at ${fmtTime(row.ts)}. ${row.text ?? ""}`.trim() },
			];
		}

		case "send_message": {
			const a = args as SendArgs;
			const text = String(a.text ?? "").trim();
			if (!text) return "Nothing to send: text is empty.";
			if (text.length > SEND_MAX_CHARS) return `Message is too long (${text.length} characters, max ${SEND_MAX_CHARS}).`;
			const group = await resolveOneGroup(db, a.group);
			let reply: { id: string; sender: string; text: string } | undefined;
			if (a.reply_to) {
				const q = await findMessage(db, a.reply_to);
				if (q.chat_jid !== group.jid) return `Message #${a.reply_to} is in ${q.gname}, not ${group.name}.`;
				reply = { id: q.id.slice(q.chat_jid.length + 1), sender: q.sender_jid ?? "", text: q.text ?? "" };
			}
			const sent = await bridge.send(group.jid, text, reply);
			return `Sent to ${group.name} at ${fmtTime(sent.ts)}${reply ? " (as a reply)" : ""}. #${sent.id}`;
		}

		case "bridge_status":
			return JSON.stringify(await bridge.status(), null, 2);

		case "list_groups": {
			const rows = await db
				.prepare(
					`SELECT COALESCE(g.name, m.chat_name, m.chat_jid) AS name, m.chat_jid AS jid,
					        COUNT(*) AS total, MAX(m.ts) AS last_ts,
					        SUM(CASE WHEN m.ts > unixepoch() - 7*86400 THEN 1 ELSE 0 END) AS last7
					 FROM messages m LEFT JOIN groups g ON g.jid = m.chat_jid
					 GROUP BY m.chat_jid ORDER BY last_ts DESC`,
				)
				.all<{ name: string; jid: string; total: number; last_ts: number; last7: number }>();
			const quiet = await db
				.prepare(`SELECT name, jid FROM groups WHERE jid NOT IN (SELECT DISTINCT chat_jid FROM messages) ORDER BY name`)
				.all<{ name: string; jid: string }>();
			const lines = rows.results.map(
				(r) => `${r.name} — last ${fmtTime(r.last_ts)}, ${r.last7} msgs past 7 days (${r.total} total)`,
			);
			for (const q of quiet.results) lines.push(`${q.name} — no messages stored yet`);
			return lines.length ? lines.join("\n") : "No groups synced yet. Check bridge_status.";
		}

		case "get_messages": {
			const hours = clamp(Number(args.since_hours) || 24, 0.1, 24 * 365);
			const limit = clamp(Number(args.limit) || 300, 1, 1000);
			const since = Math.floor(Date.now() / 1000 - hours * 3600);
			const { clause, binds, label } = await groupFilter(db, args.group);
			if (clause === "NONE") return `No group matches "${args.group}". Use list_groups to see names.`;
			const rows = await db
				.prepare(
					`SELECT * FROM (
					   SELECT ${COLS}, COALESCE(g.name, m.chat_name, m.chat_jid) AS gname
					   FROM messages m LEFT JOIN groups g ON g.jid = m.chat_jid
					   WHERE m.ts >= ? ${clause} ORDER BY m.ts DESC LIMIT ?
					 ) ORDER BY ts ASC`,
				)
				.bind(since, ...binds, limit)
				.all<Row>();
			if (!rows.results.length) return `No messages in ${label} in the last ${hours} hours.`;
			return formatRows(rows.results, `${rows.results.length} messages in ${label}, last ${hours} hours`);
		}

		case "search_messages": {
			const words = String(args.query ?? "").trim().split(/\s+/).filter(Boolean).slice(0, 8);
			if (!words.length) return "Give me something to search for.";
			const days = clamp(Number(args.days) || 30, 1, 3650);
			const limit = clamp(Number(args.limit) || 100, 1, 500);
			const since = Math.floor(Date.now() / 1000 - days * 86400);
			const { clause, binds, label } = await groupFilter(db, args.group);
			if (clause === "NONE") return `No group matches "${args.group}". Use list_groups to see names.`;
			const wordClause = words.map(() => `AND (m.text LIKE ? OR m.quoted_text LIKE ?)`).join(" ");
			const wordBinds = words.flatMap((w) => [`%${w}%`, `%${w}%`]);
			const rows = await db
				.prepare(
					`SELECT ${COLS}, COALESCE(g.name, m.chat_name, m.chat_jid) AS gname
					 FROM messages m LEFT JOIN groups g ON g.jid = m.chat_jid
					 WHERE m.ts >= ? ${clause} ${wordClause} ORDER BY m.ts DESC LIMIT ?`,
				)
				.bind(since, ...binds, ...wordBinds, limit)
				.all<Row>();
			if (!rows.results.length) return `Nothing matching "${words.join(" ")}" in ${label} in the last ${days} days.`;
			return formatRows(rows.results.reverse(), `${rows.results.length} matches for "${words.join(" ")}" in ${label}`);
		}
	}
	throw new Error(`Unknown tool ${name}`);
}

type Row = BufferedMsg & { gname: string; from_me: any; has_media?: number };

/** Finds a message by the short id shown after # (or the full chat:id). */
async function findMessage(db: D1Database, raw: unknown) {
	const id = String(raw ?? "").trim().replace(/^#/, "");
	if (!id) throw new Error("message_id is required.");
	const row = await db
		.prepare(
			`SELECT ${COLS}, COALESCE(g.name, m.chat_name, m.chat_jid) AS gname
			 FROM messages m LEFT JOIN groups g ON g.jid = m.chat_jid
			 WHERE m.id = ?1 OR m.id LIKE '%:' || ?1 ORDER BY m.ts DESC LIMIT 1`,
		)
		.bind(id)
		.first<Row>();
	if (!row) throw new Error(`No message with id #${id}.`);
	return { ...row, who: row.from_me ? "Me" : row.sender_name || shortJid(row.sender_jid) };
}

/** Resolves a group name to exactly one group, or explains why it can't. */
async function resolveOneGroup(db: D1Database, group: unknown): Promise<{ jid: string; name: string }> {
	const g = String(group ?? "").trim();
	if (!g) throw new Error("group is required.");
	const hits = await db
		.prepare(
			`SELECT jid, name FROM groups WHERE name LIKE ?1 OR jid = ?2
			 UNION SELECT DISTINCT chat_jid, chat_name FROM messages WHERE chat_name LIKE ?1 OR chat_jid = ?2`,
		)
		.bind(`%${g}%`, g)
		.all<{ jid: string; name: string | null }>();
	const byJid = new Map<string, string>();
	for (const h of hits.results) if (!byJid.has(h.jid) || h.name) byJid.set(h.jid, h.name || h.jid);
	const list = [...byJid].map(([jid, name]) => ({ jid, name }));
	if (!list.length) throw new Error(`No group matches "${g}". Nothing was sent.`);
	if (list.length === 1) return list[0];
	const exact = list.filter((x) => x.name.toLowerCase() === g.toLowerCase());
	if (exact.length === 1) return exact[0];
	throw new Error(`"${g}" matches ${list.length} groups: ${list.map((x) => x.name).join(", ")}. Nothing was sent; use the full name.`);
}

async function groupFilter(db: D1Database, group: unknown): Promise<{ clause: string; binds: string[]; label: string }> {
	const g = String(group ?? "").trim();
	if (!g) return { clause: "", binds: [], label: "all groups" };
	if (g.endsWith("@g.us")) return { clause: "AND m.chat_jid = ?", binds: [g], label: g };
	const hits = await db
		.prepare(
			`SELECT jid, name FROM groups WHERE name LIKE ?1
			 UNION SELECT DISTINCT chat_jid, chat_name FROM messages WHERE chat_name LIKE ?1`,
		)
		.bind(`%${g}%`)
		.all<{ jid: string; name: string }>();
	const jids = [...new Set(hits.results.map((h) => h.jid))];
	if (!jids.length) return { clause: "NONE", binds: [], label: g };
	const names = [...new Set(hits.results.map((h) => h.name))].slice(0, 5).join(", ");
	return { clause: `AND m.chat_jid IN (${jids.map(() => "?").join(",")})`, binds: jids, label: names };
}

function formatRows(rows: Row[], header: string): string {
	const multi = new Set(rows.map((r) => r.chat_jid)).size > 1;
	const lines = rows.map((r) => {
		const who = r.from_me ? "Me" : r.sender_name || shortJid(r.sender_jid);
		const where = multi ? `{${r.gname}} ` : "";
		const reply = r.quoted_text ? `  (replying to: "${oneLine(r.quoted_text, 120)}")` : "";
		const photo = r.has_media ? " (photo)" : "";
		const id = r.id.slice(r.chat_jid.length + 1);
		return `[${fmtTime(r.ts)}] ${where}${who}: ${r.text ?? ""}${reply}${photo} #${id}`;
	});
	const groupLine = multi ? "" : `Group: ${rows[0].gname}\n`;
	return `${header}\n${groupLine}\n${lines.join("\n")}`;
}

// ───────────────────────── helpers ─────────────────────────

const timeFmt = new Intl.DateTimeFormat("en-US", {
	timeZone: TZ,
	weekday: "short",
	month: "numeric",
	day: "numeric",
	hour: "numeric",
	minute: "2-digit",
	hour12: true,
});
function fmtTime(unix: number): string {
	return timeFmt.format(new Date(unix * 1000));
}
function shortJid(j?: string | null): string {
	return (j ?? "unknown").split("@")[0];
}
function oneLine(s: string, n: number): string {
	const t = s.replace(/\s+/g, " ").trim();
	return t.length > n ? t.slice(0, n) + "…" : t;
}
function clamp(n: number, lo: number, hi: number): number {
	return Math.min(hi, Math.max(lo, n));
}
function toBase64(buf: ArrayBuffer): string {
	const bytes = new Uint8Array(buf);
	let bin = "";
	for (let i = 0; i < bytes.length; i += 0x8000) bin += String.fromCharCode(...bytes.subarray(i, i + 0x8000));
	return btoa(bin);
}
function sleep(ms: number) {
	return new Promise((r) => setTimeout(r, ms));
}
function safeEqual(a: string, b: string): boolean {
	if (a.length !== b.length) return false;
	let diff = 0;
	for (let i = 0; i < a.length; i++) diff |= a.charCodeAt(i) ^ b.charCodeAt(i);
	return diff === 0;
}

const LINK_PAGE = `<!doctype html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>NDX WhatsApp Yard — Link</title>
<style>
 body{font-family:system-ui,-apple-system,sans-serif;background:#f6f7f9;color:#1d2330;margin:0;padding:24px;display:flex;justify-content:center}
 .card{background:#fff;border-radius:14px;box-shadow:0 2px 12px rgba(0,0,0,.08);padding:24px;max-width:420px;width:100%}
 h1{font-size:20px;margin:0 0 4px} .sub{color:#667;font-size:14px;margin:0 0 18px}
 .pill{display:inline-block;padding:4px 10px;border-radius:99px;font-weight:600;font-size:13px}
 .ok{background:#e3f6e8;color:#17803d} .wait{background:#fff4d6;color:#8a5a00} .bad{background:#fde8e8;color:#b42318}
 #qr{display:flex;justify-content:center;margin:18px 0} #qr svg{width:280px;height:280px}
 ol{padding-left:20px;line-height:1.6;font-size:15px} dl{display:grid;grid-template-columns:auto 1fr;gap:6px 12px;font-size:14px}
 dt{color:#667} button{margin-top:18px;background:none;border:1px solid #ccd;border-radius:8px;padding:8px 12px;color:#556;cursor:pointer}
</style></head><body><div class="card">
<h1>NDX WhatsApp Yard</h1><p class="sub">Sync of your WhatsApp groups</p>
<div id="state"><span class="pill wait">Starting…</span></div>
<div id="qr"></div>
<div id="steps" style="display:none"><ol>
 <li>Open WhatsApp on your phone</li><li>Tap <b>Settings</b> → <b>Linked Devices</b> → <b>Link a Device</b></li>
 <li>Point your phone at this code</li></ol></div>
<dl id="stats"></dl>
<button id="reset">Re-link (forget this login)</button>
</div><script>
const key=new URLSearchParams(location.search).get('key');
const q=s=>document.querySelector(s);
async function tick(){
 try{
  const r=await fetch('/link/status?key='+encodeURIComponent(key)); const s=await r.json(); const b=s.bridge||{};
  let pill='<span class="pill wait">'+(b.state||'starting')+'</span>';
  if(b.state==='connected') pill='<span class="pill ok">Linked ✓</span>';
  if(b.state==='qr') pill='<span class="pill wait">Waiting for scan</span>';
  if(b.state==='logged_out'||b.state==='error') pill='<span class="pill bad">'+b.state.replace('_',' ')+'</span>';
  q('#state').innerHTML=pill+(b.detail?' <span class="sub">'+b.detail+'</span>':'');
  q('#qr').innerHTML=s.svg||''; q('#steps').style.display=s.svg?'block':'none';
  q('#stats').innerHTML='<dt>Groups</dt><dd>'+(b.groups??'—')+'</dd><dt>Messages stored</dt><dd>'+s.stored_messages+
   '</dd><dt>Last message</dt><dd>'+(s.last_message||'—')+'</dd><dt>Login backup</dt><dd>'+(s.last_backup?new Date(s.last_backup).toLocaleString():'not yet')+'</dd>';
 }catch(e){ q('#state').innerHTML='<span class="pill wait">Starting…</span>'; }
}
q('#reset').onclick=async()=>{ if(!confirm('Forget this WhatsApp login and show a new QR code?'))return;
 await fetch('/link/reset?key='+encodeURIComponent(key),{method:'POST'}); tick(); };
tick(); setInterval(tick,3000);
</script></body></html>`;
