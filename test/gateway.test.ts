// Discord Gateway client tests.
// Run with: npm test
//
// The gateway talks to the network through `globalThis.WebSocket`, so these
// tests install a fake socket and drive the protocol by hand. That is the only
// way to exercise the parts that matter for a long-running listener: a
// handshake that never completes, and one that does.

import { test } from 'node:test';
import assert from 'node:assert/strict';

import { DiscordGateway, classifyGatewayClose, nextGatewayDelay } from '../src/gateway.js';
import { Logger } from '../src/utils.js';

const quiet = new Logger('error');

type Listener = (event: any) => void;

/** Minimal WebSocket stand-in: no network, but real event semantics. */
class FakeSocket {
  static instances: FakeSocket[] = [];

  readonly url: string;
  readonly listeners = new Map<string, Listener[]>();
  sent: any[] = [];
  closeCalls: { code?: number; reason?: string }[] = [];
  readyState = 1;

  constructor(url: string) {
    this.url = url;
    FakeSocket.instances.push(this);
  }

  static last(): FakeSocket {
    const socket = FakeSocket.instances[FakeSocket.instances.length - 1];
    assert.ok(socket, 'no socket was created');
    return socket;
  }

  addEventListener(type: string, listener: Listener): void {
    const list = this.listeners.get(type) ?? [];
    list.push(listener);
    this.listeners.set(type, list);
  }

  send(data: string): void {
    this.sent.push(JSON.parse(data));
  }

  close(code?: number, reason?: string): void {
    this.closeCalls.push({ code, reason });
    this.readyState = 3;
    this.emit('close', { code: code ?? 1000, reason: reason ?? '' });
  }

  emit(type: string, event: any): void {
    for (const listener of this.listeners.get(type) ?? []) listener(event);
  }

  /** Server HELLO, which starts the heartbeat and triggers IDENTIFY/RESUME. */
  hello(heartbeatIntervalMs = 60_000): void {
    this.emit('message', {
      data: JSON.stringify({ op: 10, d: { heartbeat_interval: heartbeatIntervalMs } }),
    });
  }

  /** Server READY, which is what the connect promise waits for. */
  ready(): void {
    this.emit('message', {
      data: JSON.stringify({
        op: 0,
        s: 1,
        t: 'READY',
        d: {
          session_id: 'session-1',
          resume_gateway_url: 'wss://resume.example',
          user: { id: '1', username: 'tester' },
        },
      }),
    });
  }

  message(id: string, channelId: string): void {
    this.emit('message', {
      data: JSON.stringify({
        op: 0,
        s: 2,
        t: 'MESSAGE_CREATE',
        d: {
          id,
          channel_id: channelId,
          content: `live-${id}`,
          timestamp: new Date().toISOString(),
          author: { id: 'u1', username: 'tester', discriminator: '0' },
        },
      }),
    });
  }
}

function installFakeWebSocket(): () => void {
  const original = (globalThis as { WebSocket?: unknown }).WebSocket;
  (globalThis as { WebSocket?: unknown }).WebSocket = FakeSocket;
  FakeSocket.instances = [];
  return () => {
    (globalThis as { WebSocket?: unknown }).WebSocket = original;
    FakeSocket.instances = [];
  };
}

// ==================== Pure protocol helpers ====================

test('classifyGatewayClose separates fatal codes from resumable ones', () => {
  // Fatal: nothing about retrying will help.
  assert.equal(classifyGatewayClose(4004), 'fatal'); // authentication failed
  assert.equal(classifyGatewayClose(4013), 'fatal'); // invalid intents
  // A new session is needed, but a reconnect is worth it.
  assert.equal(classifyGatewayClose(4007), 'identify'); // invalid seq
  assert.equal(classifyGatewayClose(4009), 'identify'); // session timed out
  assert.equal(classifyGatewayClose(1006), 'resume'); // abnormal closure
  assert.equal(classifyGatewayClose(4000), 'resume'); // we closed it ourselves
});

test('gateway reconnect delay is bounded and grows with attempts', () => {
  for (let attempt = 0; attempt < 8; attempt++) {
    const delay = nextGatewayDelay(attempt, 100, 5000);
    assert.ok(delay >= 0 && delay <= 5000, `delay ${delay} out of range`);
  }
  assert.ok(nextGatewayDelay(6, 100, 1e9) > nextGatewayDelay(0, 100, 1e9));
});

// ==================== Handshake ====================

test('a socket that never completes the handshake is dropped, not awaited forever', async () => {
  const restore = installFakeWebSocket();
  try {
    const gateway = new DiscordGateway({
      token: 'token',
      log: quiet,
      handshakeTimeoutMs: 25,
      // Fail on the first attempt instead of sleeping through the backoff.
      maxConnectAttempts: 0,
    });

    // The fake accepts the connection and then says nothing at all.
    await assert.rejects(
      () => gateway.start(),
      /handshake timed out/i,
      'a silent socket must not leave the connect promise pending',
    );

    const socket = FakeSocket.last();
    assert.equal(socket.closeCalls.length, 1, 'the dead socket should be closed');
    assert.equal(socket.readyState, 3);
    assert.equal(gateway.getState(), 'fatal');
    // The abandoned socket must not schedule a reconnect of its own: the caller
    // is already retrying, and two loops would double the connection rate.
    assert.equal(FakeSocket.instances.length, 1, 'a leaking reconnect raced the retry');
  } finally {
    restore();
  }
});

test('a handshake that completes in time is accepted and identifies', async () => {
  const restore = installFakeWebSocket();
  try {
    const gateway = new DiscordGateway({
      token: 'token',
      log: quiet,
      handshakeTimeoutMs: 5000,
      maxConnectAttempts: 0,
    });

    const received: string[] = [];
    gateway.onMessage((message) => received.push(message.id));

    const started = gateway.start();
    const socket = FakeSocket.last();

    socket.hello();
    // IDENTIFY (op 2) is sent in reply to HELLO, before the session is ready.
    assert.ok(
      socket.sent.some((payload) => payload.op === 2),
      'expected IDENTIFY after HELLO',
    );
    // No heartbeat yet: the first beat is jittered across the interval.
    assert.equal(socket.sent.some((payload) => payload.op === 1), false);

    socket.ready();
    await started;

    assert.equal(gateway.getState(), 'ready');
    assert.equal(socket.closeCalls.length, 0, 'a healthy socket must stay open');

    // Dispatches after READY reach the message handler.
    socket.message('123', '456');
    assert.deepEqual(received, ['123']);

    gateway.close();
    assert.equal(gateway.getState(), 'closed');
    assert.equal(socket.closeCalls.length, 1);
  } finally {
    restore();
  }
});
