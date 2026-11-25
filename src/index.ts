// NDX WhatsApp Yard — Cloudflare Worker
//
//  /link?key=...        QR page to link WhatsApp (Settings > Linked Devices > Link a Device)
//  /mcp/<key>           Claude connector (read-only tools over the group-message database)
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

const SCHEMA = [
	`CREATE TABLE IF NOT EXISTS messages (
		id TEXT PRIMARY KEY, chat_jid TEXT NOT NULL, chat_name TEXT, sender_jid TEXT, sender_name TEXT,
		ts INTEGER NOT NULL, text TEXT, kind TEXT, quoted_id TEXT, quoted_text TEXT, from_me INTEGER DEFAULT 0,
		stored_at INTEGER)`,
	`CREATE INDEX IF NOT EXISTS idx_messages_chat_ts ON messages (chat_jid, ts)`,
	`CREATE INDEX IF NOT EXISTS idx_messages_ts ON messages (ts)`,
	`CREATE TABLE IF NOT EXISTS groups (jid TEXT PRIMARY KEY, name TEXT, participants INTEGER, updated_at INTEGER)`,
];

type BridgeStatus = {
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
};

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
			if (status?.state === "connected") {
				await this.maybeBackup();
				await this.maybeSyncGroups();
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

	// ── internals ──

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
		await this.env.DB.batch(SCHEMA.map((s) => this.env.DB.prepare(s)));
		this.schemaReady = true;
	}

	private async flush(): Promise<void> {
		const insert = this.env.DB.prepare(
			`INSERT OR IGNORE INTO messages
			 (id, chat_jid, chat_name, sender_jid, sender_name, ts, text, kind, quoted_id, quoted_text, from_me, stored_at)
			 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
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
				serverInfo: { name: "ndx-whatsapp-yard", version: "1.0.0" },
				instructions:
					"Read-only access to NDX's WhatsApp group chats (parts orders, ETAs, shop projects). Times are Pacific, 12-hour. Nothing here can send messages.",
			});
		case "ping":
			return reply({});
		case "tools/list":
			return reply({ tools: TOOLS });
		case "tools/call": {
			try {
				const text = await callTool(msg.params?.name, msg.params?.arguments ?? {}, env, bridge);
				return reply({ content: [{ type: "text", text }] });
			} catch (err) {
				return reply({ content: [{ type: "text", text: `Error: ${String(err)}` }], isError: true });
			}
		}
		default:
			return { jsonrpc: "2.0", id: msg.id, error: { code: -32601, message: `Unknown method ${msg.method}` } };
	}
}

async function callTool(name: string, args: any, env: Env, bridge: DurableObjectStub<Bridge>): Promise<string> {
	const db = env.DB;
	switch (name) {
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
					   SELECT m.*, COALESCE(g.name, m.chat_name, m.chat_jid) AS gname
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
					`SELECT m.*, COALESCE(g.name, m.chat_name, m.chat_jid) AS gname
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

type Row = BufferedMsg & { gname: string; from_me: any };

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
		return `[${fmtTime(r.ts)}] ${where}${who}: ${r.text ?? ""}${reply}`;
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
<h1>NDX WhatsApp Yard</h1><p class="sub">Read-only sync of your WhatsApp groups</p>
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
