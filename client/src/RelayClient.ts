/**
 * RelayClient — reference client implementation (Section 99, 153).
 *
 * The client is part of the protocol, not just a UI convenience. It is
 * responsible for the half of the reliability contract that lives outside
 * the server:
 *
 *   - heartbeat responses (PONG)
 *   - reconnect with capped exponential backoff + jitter (Section 57, 100)
 *   - session resumption from the last acknowledged sequence (Section 19)
 *   - subscription restoration after reconnect (Section 101)
 *   - duplicate suppression by message_id (Section 26, 85, 103)
 *   - sequence gap detection (Section 102)
 *
 * Delivery is AT_LEAST_ONCE. Duplicates are possible by design; this
 * client suppresses them within a bounded window so application handlers
 * see each message once in the common case, but applications whose
 * processing is not idempotent must still guard themselves.
 */

export type DeliveryState = "connecting" | "connected" | "reconnecting" | "closed";

export interface RelayMessage {
  message_id: string;
  channel: string;
  sequence: number;
  timestamp: string;
  payload: Record<string, unknown>;
}

export interface RelayClientOptions {
  url: string;
  token: string;
  /** Capped exponential backoff, Section 57. */
  baseDelayMs?: number;
  maxDelayMs?: number;
  /** Bounded dedup window — never an unbounded map (Section 26, 95). */
  dedupWindowSize?: number;
  /** Acknowledge automatically after the handler returns. */
  autoAck?: boolean;
  onStateChange?: (state: DeliveryState) => void;
  onGap?: (info: { channel: string; expected: number; received: number }) => void;
  onDuplicate?: (info: { channel: string; messageId: string }) => void;
  onError?: (info: { code: string; message: string }) => void;
}

type MessageHandler = (msg: RelayMessage) => void | Promise<void>;

export class RelayClient {
  private ws: WebSocket | null = null;
  private opts: Required<Omit<RelayClientOptions,
    "onStateChange" | "onGap" | "onDuplicate" | "onError">> & RelayClientOptions;

  private sessionId: string | null = null;
  private connectionId: string | null = null;
  private nodeId: string | null = null;

  private subscriptions = new Set<string>();
  private handlers = new Map<string, Set<MessageHandler>>();

  /** Last sequence we have acknowledged, per channel — the resume point. */
  private lastAck = new Map<string, number>();
  /** Highest sequence seen, per channel — used for gap detection. */
  private lastSeen = new Map<string, number>();

  /** Bounded FIFO of recently seen message ids for duplicate suppression. */
  private seenIds = new Set<string>();
  private seenOrder: string[] = [];

  private attempt = 0;
  private state: DeliveryState = "closed";
  private closedByUser = false;
  private reconnectTimer: ReturnType<typeof setTimeout> | null = null;

  public stats = {
    messagesReceived: 0,
    duplicatesSuppressed: 0,
    gapsDetected: 0,
    reconnects: 0,
    resumes: 0,
  };

  constructor(options: RelayClientOptions) {
    this.opts = {
      baseDelayMs: 250,
      maxDelayMs: 30_000,
      dedupWindowSize: 5_000,
      autoAck: true,
      ...options,
    } as RelayClient["opts"];
  }

  // --- lifecycle -----------------------------------------------------------

  connect(): void {
    this.closedByUser = false;
    this.setState(this.sessionId ? "reconnecting" : "connecting");

    const ws = new WebSocket(this.opts.url);
    this.ws = ws;

    ws.onopen = () => {
      ws.send(JSON.stringify({
        type: "CONNECT",
        token: this.opts.token,
        session_id: this.sessionId,
      }));
    };

    ws.onmessage = (event) => this.handleFrame(JSON.parse(event.data as string));

    ws.onclose = () => {
      this.ws = null;
      if (this.closedByUser) {
        this.setState("closed");
        return;
      }
      this.scheduleReconnect();
    };

    ws.onerror = () => { /* surfaced through onclose */ };
  }

  disconnect(): void {
    this.closedByUser = true;
    if (this.reconnectTimer) clearTimeout(this.reconnectTimer);
    this.send({ type: "DISCONNECT", reason: "client_request" });
    this.ws?.close();
    this.setState("closed");
  }

  /**
   * Capped exponential backoff with full jitter (Section 57, 100). Full
   * jitter — not a fixed multiplier — is what actually spreads a
   * reconnect storm out; 10,000 clients backing off identically still
   * arrive together.
   */
  private scheduleReconnect(): void {
    this.setState("reconnecting");
    const exp = Math.min(this.opts.baseDelayMs * 2 ** this.attempt, this.opts.maxDelayMs);
    const delay = Math.random() * exp;
    this.attempt += 1;
    this.stats.reconnects += 1;
    this.reconnectTimer = setTimeout(() => this.connect(), delay);
  }

  // --- protocol ------------------------------------------------------------

  private handleFrame(frame: any): void {
    switch (frame.type) {
      case "CONNECTED":
        this.onConnected(frame);
        break;

      case "PING":
        this.send({ type: "PONG" });
        break;

      case "MESSAGE":
        void this.onMessage(frame as RelayMessage);
        break;

      case "RESUMED":
        this.stats.resumes += 1;
        break;

      case "SUBSCRIBED":
      case "UNSUBSCRIBED":
        break;

      case "DISCONNECT":
        // Server asked us to go away (draining, slow consumer). Reconnect
        // logic in onclose decides whether to come back.
        break;

      case "ERROR":
        if (frame.code === "RESUME_GAP" || frame.code === "RESUME_WINDOW_EXPIRED") {
          // Recovery is not possible from our position: the application
          // must resynchronize from its own source of truth. We reset the
          // resume point so we do not keep asking for messages that are
          // gone (Section 45, 140).
          this.lastAck.clear();
          this.lastSeen.clear();
        }
        this.opts.onError?.({ code: frame.code, message: frame.message ?? "" });
        break;
    }
  }

  private onConnected(frame: any): void {
    const resuming = this.sessionId === frame.session_id && this.sessionId !== null;
    this.sessionId = frame.session_id;
    this.connectionId = frame.connection_id;
    this.nodeId = frame.node_id;
    this.attempt = 0;
    this.setState("connected");

    if (resuming && this.lastAck.size > 0) {
      // Resume before resubscribing: the server replays from our acked
      // position and (re)attaches the subscriptions it replays for.
      this.send({
        type: "RESUME",
        session_id: this.sessionId,
        last_ack_by_channel: Object.fromEntries(this.lastAck),
      });
    }

    // Restore every subscription (Section 101) — the user should never
    // have to re-subscribe manually after a reconnect.
    for (const channel of this.subscriptions) {
      this.send({ type: "SUBSCRIBE", channel });
    }
  }

  private async onMessage(msg: RelayMessage): Promise<void> {
    this.stats.messagesReceived += 1;

    // Duplicate suppression (Section 26, 103).
    if (this.seenIds.has(msg.message_id)) {
      this.stats.duplicatesSuppressed += 1;
      this.opts.onDuplicate?.({ channel: msg.channel, messageId: msg.message_id });
      if (this.opts.autoAck) this.ack(msg.channel, msg.sequence);
      return;
    }
    this.rememberId(msg.message_id);

    // Gap detection (Section 102).
    const previous = this.lastSeen.get(msg.channel);
    if (previous !== undefined && msg.sequence > previous + 1) {
      this.stats.gapsDetected += 1;
      this.opts.onGap?.({
        channel: msg.channel,
        expected: previous + 1,
        received: msg.sequence,
      });
    }
    if (previous === undefined || msg.sequence > previous) {
      this.lastSeen.set(msg.channel, msg.sequence);
    }

    for (const handler of this.handlers.get(msg.channel) ?? []) {
      await handler(msg);
    }

    if (this.opts.autoAck) this.ack(msg.channel, msg.sequence);
  }

  /** Bounded dedup window: evict oldest once full (Section 26, 95). */
  private rememberId(id: string): void {
    this.seenIds.add(id);
    this.seenOrder.push(id);
    while (this.seenOrder.length > this.opts.dedupWindowSize) {
      const evicted = this.seenOrder.shift()!;
      this.seenIds.delete(evicted);
    }
  }

  // --- public API ----------------------------------------------------------

  subscribe(channel: string, handler?: MessageHandler): void {
    this.subscriptions.add(channel);
    if (handler) {
      if (!this.handlers.has(channel)) this.handlers.set(channel, new Set());
      this.handlers.get(channel)!.add(handler);
    }
    this.send({ type: "SUBSCRIBE", channel });
  }

  unsubscribe(channel: string): void {
    this.subscriptions.delete(channel);
    this.handlers.delete(channel);
    this.lastAck.delete(channel);
    this.lastSeen.delete(channel);
    this.send({ type: "UNSUBSCRIBE", channel });
  }

  /**
   * Cumulative acknowledgement (Section 24). Acking sequence N implicitly
   * acks everything at or below N on that channel, and is what reopens
   * the server's flow-control window for this connection.
   */
  ack(channel: string, sequence: number): void {
    const current = this.lastAck.get(channel) ?? 0;
    if (sequence <= current) return;
    this.lastAck.set(channel, sequence);
    this.send({ type: "ACK", channel, sequence });
  }

  onMessageFor(channel: string, handler: MessageHandler): void {
    if (!this.handlers.has(channel)) this.handlers.set(channel, new Set());
    this.handlers.get(channel)!.add(handler);
  }

  get info() {
    return {
      state: this.state,
      sessionId: this.sessionId,
      connectionId: this.connectionId,
      nodeId: this.nodeId,
      subscriptions: [...this.subscriptions],
    };
  }

  private send(frame: Record<string, unknown>): void {
    if (this.ws && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(frame));
    }
  }

  private setState(state: DeliveryState): void {
    this.state = state;
    this.opts.onStateChange?.(state);
  }
}
