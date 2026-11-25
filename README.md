# NDX WhatsApp Yard

Read-only sync of NDX's WhatsApp **group** chats (parts orders, ETAs, shop projects) into Cloudflare D1, plus a Claude connector so any Claude chat or scheduled task can read them.

- **Bridge** (`bridge/`): Go + [whatsmeow](https://github.com/tulir/whatsmeow). It links to WhatsApp as a companion device, like WhatsApp Web, under the name **NDX Yard Bridge**. It stores group messages only, skips one-on-one chats, and never sends messages, read receipts or presence.
- **Worker** (`src/index.ts`): runs the bridge in one always-on Cloudflare Container. Every minute it moves new messages into D1, and every 10 minutes it backs the WhatsApp login up to R2, so restarts and deploys don't need a new QR scan.

Cloudflare resources (already created): D1 `ndx-whatsapp`, R2 `ndx-whatsapp-blobs`. Containers need the Workers Paid plan.

## Setup

1. **Deploy:** connect this repo in Cloudflare under Workers & Pages → Create → Import a repository. Keep the default deploy command, `npx wrangler deploy`. The first deploy takes a few minutes while the container image builds.
2. **Secret:** on the `ndx-whatsapp` Worker, go to Settings → Variables and Secrets and add a **Secret** named `ACCESS_KEY`. Make it a long random string, 30+ characters, from a password manager.
3. **Link WhatsApp:** open `https://ndx-whatsapp.<your-subdomain>.workers.dev/link?key=<ACCESS_KEY>`. On your phone, go to WhatsApp → Settings → Linked Devices → Link a Device and scan the code.
4. **Claude connector:** in Claude, go to Settings → Connectors → Add custom connector. Name it `WhatsApp Yard`, with URL `https://ndx-whatsapp.<your-subdomain>.workers.dev/mcp/<ACCESS_KEY>`.

## Connector tools (all read-only)

| Tool | Use |
|---|---|
| `get_messages` | Messages from one group (partial name OK) or all groups for the last N hours |
| `search_messages` | Find a part number, machine, vendor, "ETA", etc. |
| `list_groups` | Groups being synced, last activity, message counts |
| `bridge_status` | Linked or not, messages stored, last backup, last error |

Times are Pacific, in 12-hour format.

## Notes

- The `/link` page shows status. If WhatsApp ever unlinks the bridge, it shows **logged out**. Tap **Re-link** and scan again.
- WhatsApp only syncs some recent history when a device first links. Everything after that is captured.
- Photos, documents and voice notes are stored as labels with their captions and file names. The media files themselves aren't downloaded.
- Container size is `lite`. If the first history sync with a lot of groups runs out of memory, change `instance: "lite"` to `"basic"` in `src/index.ts`.
- Keep the `ACCESS_KEY` private. Anyone with it can read the group messages.
