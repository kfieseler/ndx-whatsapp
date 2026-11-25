// NDX WhatsApp Yard bridge.
//
// Links to WhatsApp as a companion device (like WhatsApp Web) and buffers every
// GROUP message so the Cloudflare Durable Object can move it into D1.
// It never sends read receipts or presence. It sends a message only when the
// Worker calls POST /send (the Claude connector's send_message tool), and only
// to groups.
package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/store/sqlstore"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

const (
	dataDir     = "/data"
	sessionPath = dataDir + "/session.db"
	bufferPath  = dataDir + "/buffer.db"
	deviceName  = "NDX Yard Bridge"
)

type bridge struct {
	mu        sync.Mutex
	state     string // waiting_session | starting | qr | connected | disconnected | logged_out | error
	detail    string
	qr        string
	selfJID   string
	started   bool
	lastEvent time.Time
	groups    map[string]string // jid -> name

	client *whatsmeow.Client
	buf    *sql.DB
}

type bufferedMsg struct {
	ID         string `json:"id"`
	ChatJID    string `json:"chat_jid"`
	ChatName   string `json:"chat_name"`
	SenderJID  string `json:"sender_jid"`
	SenderName string `json:"sender_name"`
	TS         int64  `json:"ts"`
	Text       string `json:"text"`
	Kind       string `json:"kind"`
	QuotedID   string `json:"quoted_id,omitempty"`
	QuotedText string `json:"quoted_text,omitempty"`
	FromMe     bool   `json:"from_me"`
	// Photos only: the WhatsApp ImageMessage (base64 protobuf), which holds the
	// keys needed to download and decrypt the picture later via POST /media.
	Media     string `json:"media,omitempty"`
	MediaType string `json:"media_type,omitempty"`
}

func main() {
	if err := os.MkdirAll(dataDir, 0o700); err != nil {
		log.Fatal(err)
	}
	buf, err := sql.Open("sqlite3", "file:"+bufferPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		log.Fatal(err)
	}
	buf.SetMaxOpenConns(1)
	if _, err := buf.Exec(`CREATE TABLE IF NOT EXISTS buf (seq INTEGER PRIMARY KEY AUTOINCREMENT, msg_id TEXT UNIQUE, payload TEXT NOT NULL)`); err != nil {
		log.Fatal(err)
	}

	b := &bridge{state: "waiting_session", buf: buf, groups: map[string]string{}}

	// Name shown under Linked Devices on the phone.
	store.SetOSInfo(deviceName, [3]uint32{1, 0, 0})

	mux := http.NewServeMux()
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok")) })
	mux.HandleFunc("/status", b.handleStatus)
	mux.HandleFunc("/session/import", b.handleImport)
	mux.HandleFunc("/session/export", b.handleExport)
	mux.HandleFunc("/start", b.handleStart)
	mux.HandleFunc("/reset", b.handleReset)
	mux.HandleFunc("/messages", b.handleMessages)
	mux.HandleFunc("/ack", b.handleAck)
	mux.HandleFunc("/groups", b.handleGroups)
	mux.HandleFunc("/send", b.handleSend)
	mux.HandleFunc("/media", b.handleMedia)

	srv := &http.Server{Addr: ":8080", Handler: mux}
	go func() {
		sig := make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		<-sig
		log.Println("shutting down")
		b.mu.Lock()
		if b.client != nil {
			b.client.Disconnect()
		}
		b.mu.Unlock()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(ctx)
	}()
	log.Println("bridge listening on :8080")
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatal(err)
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func (b *bridge) setState(state, detail string) {
	b.mu.Lock()
	b.state, b.detail = state, detail
	if state != "qr" {
		b.qr = ""
	}
	b.lastEvent = time.Now()
	b.mu.Unlock()
	log.Printf("state=%s %s", state, detail)
}

// ---------- HTTP handlers ----------

func (b *bridge) handleStatus(w http.ResponseWriter, r *http.Request) {
	var buffered int
	b.buf.QueryRow(`SELECT COUNT(*) FROM buf`).Scan(&buffered)
	b.mu.Lock()
	defer b.mu.Unlock()
	writeJSON(w, map[string]any{
		"state":      b.state,
		"detail":     b.detail,
		"qr":         b.qr,
		"jid":        b.selfJID,
		"started":    b.started,
		"buffered":   buffered,
		"groups":     len(b.groups),
		"last_event": b.lastEvent.Unix(),
	})
}

// POST body = saved session.db (may be empty when there is no saved session).
func (b *bridge) handleImport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	b.mu.Lock()
	started := b.started
	b.mu.Unlock()
	if started {
		http.Error(w, "already started", 409)
		return
	}
	data, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	if len(data) > 0 {
		if err := os.WriteFile(sessionPath, data, 0o600); err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		log.Printf("imported saved session (%d bytes)", len(data))
	}
	b.start()
	writeJSON(w, map[string]any{"ok": true, "imported_bytes": len(data)})
}

func (b *bridge) handleStart(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	b.start()
	writeJSON(w, map[string]any{"ok": true})
}

// Returns a consistent copy of session.db for backup to R2.
func (b *bridge) handleExport(w http.ResponseWriter, r *http.Request) {
	if _, err := os.Stat(sessionPath); err != nil {
		http.Error(w, "no session", 404)
		return
	}
	tmp := fmt.Sprintf("%s/export-%d.db", dataDir, time.Now().UnixNano())
	defer os.Remove(tmp)
	db, err := sql.Open("sqlite3", "file:"+sessionPath+"?_busy_timeout=5000")
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer db.Close()
	if _, err := db.Exec(`VACUUM INTO ?`, tmp); err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	data, err := os.ReadFile(tmp)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Write(data)
}

// Wipes the WhatsApp login so a fresh QR code appears (used to re-link).
func (b *bridge) handleReset(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	b.mu.Lock()
	if b.client != nil {
		b.client.Disconnect()
		b.client = nil
	}
	b.started = false
	b.mu.Unlock()
	os.Remove(sessionPath)
	os.Remove(sessionPath + "-wal")
	os.Remove(sessionPath + "-shm")
	b.setState("waiting_session", "reset")
	b.start()
	writeJSON(w, map[string]any{"ok": true})
}

func (b *bridge) handleMessages(w http.ResponseWriter, r *http.Request) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if limit <= 0 || limit > 1000 {
		limit = 500
	}
	rows, err := b.buf.Query(`SELECT seq, payload FROM buf WHERE seq > ? ORDER BY seq LIMIT ?`, after, limit)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	defer rows.Close()
	type item struct {
		Seq int64           `json:"seq"`
		Msg json.RawMessage `json:"msg"`
	}
	out := []item{}
	for rows.Next() {
		var it item
		var payload string
		if err := rows.Scan(&it.Seq, &payload); err == nil {
			it.Msg = json.RawMessage(payload)
			out = append(out, it)
		}
	}
	writeJSON(w, out)
}

func (b *bridge) handleAck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	upto, err := strconv.ParseInt(r.URL.Query().Get("upto"), 10, 64)
	if err != nil {
		http.Error(w, "bad upto", 400)
		return
	}
	res, err := b.buf.Exec(`DELETE FROM buf WHERE seq <= ?`, upto)
	if err != nil {
		http.Error(w, err.Error(), 500)
		return
	}
	n, _ := res.RowsAffected()
	writeJSON(w, map[string]any{"deleted": n})
}

func (b *bridge) handleGroups(w http.ResponseWriter, r *http.Request) {
	b.mu.Lock()
	cli := b.client
	b.mu.Unlock()
	if cli == nil || !cli.IsLoggedIn() {
		http.Error(w, "not connected", 503)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	groups, err := cli.GetJoinedGroups(ctx)
	if err != nil {
		http.Error(w, err.Error(), 502)
		return
	}
	type g struct {
		JID          string `json:"jid"`
		Name         string `json:"name"`
		Participants int    `json:"participants"`
	}
	out := make([]g, 0, len(groups))
	b.mu.Lock()
	for _, gi := range groups {
		b.groups[gi.JID.String()] = gi.Name
		out = append(out, g{gi.JID.String(), gi.Name, len(gi.Participants)})
	}
	b.mu.Unlock()
	writeJSON(w, out)
}

type sendReq struct {
	ChatJID string `json:"chat_jid"`
	Text    string `json:"text"`
	// Optional reply: the WhatsApp message ID (without the "chat:" prefix),
	// who sent it, and its text so WhatsApp can show the quote.
	ReplyToID     string `json:"reply_to_id,omitempty"`
	ReplyToSender string `json:"reply_to_sender,omitempty"`
	ReplyToText   string `json:"reply_to_text,omitempty"`
}

// handleSend posts one text message to a group. Groups only.
func (b *bridge) handleSend(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	var req sendReq
	if err := json.NewDecoder(io.LimitReader(r.Body, 64<<10)).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), 400)
		return
	}
	req.Text = strings.TrimSpace(req.Text)
	if req.Text == "" {
		http.Error(w, "empty text", 400)
		return
	}
	chat, err := types.ParseJID(req.ChatJID)
	if err != nil || chat.Server != types.GroupServer {
		http.Error(w, "chat_jid must be a group (@g.us)", 400)
		return
	}
	b.mu.Lock()
	cli := b.client
	b.mu.Unlock()
	if cli == nil || !cli.IsLoggedIn() {
		http.Error(w, "not connected", 503)
		return
	}

	msg := &waE2E.Message{}
	if req.ReplyToID != "" {
		ctxInfo := &waE2E.ContextInfo{
			StanzaID:      proto.String(req.ReplyToID),
			QuotedMessage: &waE2E.Message{Conversation: proto.String(req.ReplyToText)},
		}
		if req.ReplyToSender != "" {
			ctxInfo.Participant = proto.String(req.ReplyToSender)
		}
		msg.ExtendedTextMessage = &waE2E.ExtendedTextMessage{Text: proto.String(req.Text), ContextInfo: ctxInfo}
	} else {
		msg.Conversation = proto.String(req.Text)
	}

	ctx, cancel := context.WithTimeout(r.Context(), 30*time.Second)
	defer cancel()
	resp, err := cli.SendMessage(ctx, chat, msg)
	if err != nil {
		http.Error(w, "send failed: "+err.Error(), 502)
		return
	}
	log.Printf("sent message %s to %s", resp.ID, chat)

	// Our own sends don't come back as events, so store a copy for the database.
	b.mu.Lock()
	chatName := b.groups[chat.String()]
	self := b.selfJID
	b.mu.Unlock()
	if sj, err := types.ParseJID(self); err == nil {
		self = sj.ToNonAD().String()
	}
	m := bufferedMsg{
		ID:         chat.String() + ":" + resp.ID,
		ChatJID:    chat.String(),
		ChatName:   chatName,
		SenderJID:  self,
		SenderName: "Me",
		TS:         resp.Timestamp.Unix(),
		Text:       req.Text,
		Kind:       "text",
		QuotedID:   req.ReplyToID,
		QuotedText: truncate(req.ReplyToText, 300),
		FromMe:     true,
	}
	payload, _ := json.Marshal(m)
	if _, err := b.buf.Exec(`INSERT OR IGNORE INTO buf (msg_id, payload) VALUES (?, ?)`, m.ID, string(payload)); err != nil {
		log.Printf("buffer insert (sent): %v", err)
	}
	writeJSON(w, map[string]any{"ok": true, "id": resp.ID, "ts": resp.Timestamp.Unix()})
}

// handleMedia downloads and decrypts one photo. POST body: {"media": "<base64 ImageMessage>"}.
// Returns the image bytes.
func (b *bridge) handleMedia(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", 405)
		return
	}
	var req struct {
		Media string `json:"media"`
	}
	if err := json.NewDecoder(io.LimitReader(r.Body, 256<<10)).Decode(&req); err != nil {
		http.Error(w, "bad json: "+err.Error(), 400)
		return
	}
	raw, err := base64.StdEncoding.DecodeString(req.Media)
	if err != nil {
		http.Error(w, "bad media", 400)
		return
	}
	img := &waE2E.ImageMessage{}
	if err := proto.Unmarshal(raw, img); err != nil {
		http.Error(w, "bad media", 400)
		return
	}
	b.mu.Lock()
	cli := b.client
	b.mu.Unlock()
	if cli == nil || !cli.IsLoggedIn() {
		http.Error(w, "not connected", 503)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 60*time.Second)
	defer cancel()
	data, err := cli.Download(ctx, img)
	if err != nil {
		// 404/410 from WhatsApp means the file has expired off its servers.
		http.Error(w, "download failed: "+err.Error(), 502)
		return
	}
	w.Header().Set("Content-Type", firstNonEmpty(img.GetMimetype(), "image/jpeg"))
	w.Write(data)
}

// imageOf finds a photo inside a message (including view-once / ephemeral wrappers).
func imageOf(msg *waE2E.Message) *waE2E.ImageMessage {
	if msg == nil {
		return nil
	}
	if img := msg.GetImageMessage(); img != nil {
		return img
	}
	if inner := msg.GetEphemeralMessage().GetMessage(); inner != nil {
		return imageOf(inner)
	}
	if inner := msg.GetViewOnceMessage().GetMessage(); inner != nil {
		return imageOf(inner)
	}
	if inner := msg.GetViewOnceMessageV2().GetMessage(); inner != nil {
		return imageOf(inner)
	}
	return nil
}

// ---------- WhatsApp client ----------

func (b *bridge) start() {
	b.mu.Lock()
	if b.started {
		b.mu.Unlock()
		return
	}
	b.started = true
	b.mu.Unlock()
	b.setState("starting", "")

	go func() {
		ctx := context.Background()
		container, err := sqlstore.New(ctx, "sqlite3", "file:"+sessionPath+"?_foreign_keys=on&_busy_timeout=5000", waLog.Noop)
		if err != nil {
			b.fail("session store: " + err.Error())
			return
		}
		device, err := container.GetFirstDevice(ctx)
		if err != nil {
			b.fail("device: " + err.Error())
			return
		}
		cli := whatsmeow.NewClient(device, waLog.Stdout("WA", "WARN", false))
		cli.AddEventHandler(b.onEvent)
		b.mu.Lock()
		b.client = cli
		b.mu.Unlock()

		if cli.Store.ID == nil {
			qrChan, err := cli.GetQRChannel(ctx)
			if err != nil {
				b.fail("qr: " + err.Error())
				return
			}
			if err := cli.Connect(); err != nil {
				b.fail("connect: " + err.Error())
				return
			}
			for item := range qrChan {
				switch item.Event {
				case "code":
					b.mu.Lock()
					b.state, b.qr, b.detail = "qr", item.Code, "scan with WhatsApp > Linked Devices"
					b.lastEvent = time.Now()
					b.mu.Unlock()
				case "success":
					b.setState("connected", "linked")
				default:
					// timeout / error: allow a fresh start on the next poll.
					b.mu.Lock()
					b.started = false
					b.client = nil
					b.mu.Unlock()
					cli.Disconnect()
					b.setState("waiting_session", "qr "+item.Event)
					b.start()
					return
				}
			}
		} else {
			if err := cli.Connect(); err != nil {
				b.fail("connect: " + err.Error())
			}
		}
	}()
}

func (b *bridge) fail(msg string) {
	b.mu.Lock()
	b.started = false
	b.client = nil
	b.mu.Unlock()
	b.setState("error", msg)
}

func (b *bridge) onEvent(raw any) {
	switch evt := raw.(type) {
	case *events.Connected:
		b.mu.Lock()
		if b.client != nil && b.client.Store.ID != nil {
			b.selfJID = b.client.Store.ID.String()
		}
		b.mu.Unlock()
		b.setState("connected", "")
		go b.refreshGroups()
	case *events.Disconnected:
		b.setState("disconnected", "reconnecting")
	case *events.LoggedOut:
		b.setState("logged_out", "unlinked from phone — scan a new QR code")
	case *events.StreamReplaced:
		b.setState("error", "session opened somewhere else")
	case *events.JoinedGroup:
		b.mu.Lock()
		b.groups[evt.JID.String()] = evt.Name
		b.mu.Unlock()
	case *events.GroupInfo:
		if evt.Name != nil {
			b.mu.Lock()
			b.groups[evt.JID.String()] = evt.Name.Name
			b.mu.Unlock()
		}
	case *events.Message:
		b.bufferMessage(evt)
	case *events.HistorySync:
		b.onHistory(evt)
	}
}

func (b *bridge) refreshGroups() {
	b.mu.Lock()
	cli := b.client
	b.mu.Unlock()
	if cli == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	groups, err := cli.GetJoinedGroups(ctx)
	if err != nil {
		log.Printf("group refresh: %v", err)
		return
	}
	b.mu.Lock()
	for _, g := range groups {
		b.groups[g.JID.String()] = g.Name
	}
	b.mu.Unlock()
	log.Printf("groups known: %d", len(groups))
}

func (b *bridge) onHistory(evt *events.HistorySync) {
	b.mu.Lock()
	cli := b.client
	b.mu.Unlock()
	if cli == nil || evt.Data == nil {
		return
	}
	n := 0
	for _, conv := range evt.Data.GetConversations() {
		chat, err := types.ParseJID(conv.GetID())
		if err != nil || chat.Server != types.GroupServer {
			continue
		}
		if name := conv.GetName(); name != "" {
			b.mu.Lock()
			if b.groups[chat.String()] == "" {
				b.groups[chat.String()] = name
			}
			b.mu.Unlock()
		}
		for _, hm := range conv.GetMessages() {
			wm := hm.GetMessage()
			if wm == nil {
				continue
			}
			parsed, err := cli.ParseWebMessage(chat, wm)
			if err != nil {
				continue
			}
			if b.bufferMessage(parsed) {
				n++
			}
		}
	}
	if n > 0 {
		log.Printf("history sync: buffered %d group messages", n)
	}
}

// bufferMessage stores a group message. Returns true if something was stored.
func (b *bridge) bufferMessage(evt *events.Message) bool {
	info := evt.Info
	if info.Chat.Server != types.GroupServer {
		return false // groups only; personal chats are ignored
	}
	text, kind, quotedID, quotedText := extract(evt.Message)
	if kind == "" {
		return false
	}
	b.mu.Lock()
	chatName := b.groups[info.Chat.String()]
	cli := b.client
	b.mu.Unlock()

	sender := info.Sender.ToNonAD()
	senderName := info.PushName
	if senderName == "" && cli != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		lookup := sender
		if !info.SenderAlt.IsEmpty() && sender.Server == types.HiddenUserServer {
			lookup = info.SenderAlt.ToNonAD()
		}
		if c, err := cli.Store.Contacts.GetContact(ctx, lookup); err == nil {
			senderName = firstNonEmpty(c.FullName, c.PushName, c.FirstName, c.BusinessName)
		}
		cancel()
	}
	if info.IsFromMe && senderName == "" {
		senderName = "Me"
	}

	m := bufferedMsg{
		ID:         info.Chat.String() + ":" + info.ID,
		ChatJID:    info.Chat.String(),
		ChatName:   chatName,
		SenderJID:  sender.String(),
		SenderName: senderName,
		TS:         info.Timestamp.Unix(),
		Text:       text,
		Kind:       kind,
		QuotedID:   quotedID,
		QuotedText: truncate(quotedText, 300),
		FromMe:     info.IsFromMe,
	}
	if img := imageOf(evt.Message); img != nil && img.GetDirectPath() != "" {
		if raw, err := proto.Marshal(img); err == nil {
			m.Media = base64.StdEncoding.EncodeToString(raw)
			m.MediaType = firstNonEmpty(img.GetMimetype(), "image/jpeg")
		}
	}
	payload, _ := json.Marshal(m)
	if _, err := b.buf.Exec(`INSERT OR IGNORE INTO buf (msg_id, payload) VALUES (?, ?)`, m.ID, string(payload)); err != nil {
		log.Printf("buffer insert: %v", err)
		return false
	}
	b.mu.Lock()
	b.lastEvent = time.Now()
	b.mu.Unlock()
	return true
}

// extract pulls readable text out of a WhatsApp message.
func extract(msg *waE2E.Message) (text, kind, quotedID, quotedText string) {
	if msg == nil {
		return
	}
	var ctxInfo *waE2E.ContextInfo
	switch {
	case msg.GetConversation() != "":
		text, kind = msg.GetConversation(), "text"
	case msg.GetExtendedTextMessage() != nil:
		e := msg.GetExtendedTextMessage()
		text, kind, ctxInfo = e.GetText(), "text", e.GetContextInfo()
	case msg.GetImageMessage() != nil:
		m := msg.GetImageMessage()
		text, kind, ctxInfo = withLabel("[photo]", m.GetCaption()), "image", m.GetContextInfo()
	case msg.GetVideoMessage() != nil:
		m := msg.GetVideoMessage()
		text, kind, ctxInfo = withLabel("[video]", m.GetCaption()), "video", m.GetContextInfo()
	case msg.GetDocumentMessage() != nil:
		m := msg.GetDocumentMessage()
		label := "[document"
		if m.GetFileName() != "" {
			label += ": " + m.GetFileName()
		}
		text, kind, ctxInfo = withLabel(label+"]", m.GetCaption()), "document", m.GetContextInfo()
	case msg.GetDocumentWithCaptionMessage() != nil:
		return extract(msg.GetDocumentWithCaptionMessage().GetMessage())
	case msg.GetAudioMessage() != nil:
		m := msg.GetAudioMessage()
		text, kind, ctxInfo = fmt.Sprintf("[voice note, %ds]", m.GetSeconds()), "audio", m.GetContextInfo()
	case msg.GetLocationMessage() != nil:
		m := msg.GetLocationMessage()
		text, kind = withLabel(fmt.Sprintf("[location %.5f,%.5f]", m.GetDegreesLatitude(), m.GetDegreesLongitude()), strings.TrimSpace(m.GetName()+" "+m.GetAddress())), "location"
	case msg.GetLiveLocationMessage() != nil:
		text, kind = "[live location]", "location"
	case msg.GetContactMessage() != nil:
		text, kind = "[contact: "+msg.GetContactMessage().GetDisplayName()+"]", "contact"
	case msg.GetPollCreationMessage() != nil:
		text, kind = "[poll] "+msg.GetPollCreationMessage().GetName(), "poll"
	case msg.GetPollCreationMessageV3() != nil:
		text, kind = "[poll] "+msg.GetPollCreationMessageV3().GetName(), "poll"
	case msg.GetStickerMessage() != nil:
		text, kind = "[sticker]", "sticker"
	case msg.GetProtocolMessage() != nil && msg.GetProtocolMessage().GetEditedMessage() != nil:
		pm := msg.GetProtocolMessage()
		t, _, _, _ := extract(pm.GetEditedMessage())
		return "[edited] " + t, "edit", pm.GetKey().GetID(), ""
	default:
		return // reactions, receipts, deletes, system messages: skip
	}
	if ctxInfo != nil && ctxInfo.GetStanzaID() != "" {
		quotedID = ctxInfo.GetStanzaID()
		if q := ctxInfo.GetQuotedMessage(); q != nil {
			quotedText, _, _, _ = extract(q)
		}
	}
	return
}

func withLabel(label, caption string) string {
	if caption == "" {
		return label
	}
	return label + " " + caption
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func truncate(s string, n int) string {
	r := []rune(s)
	if len(r) <= n {
		return s
	}
	return string(r[:n]) + "…"
}
