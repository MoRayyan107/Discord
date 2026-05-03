# Java Chat Server

A production-aware, multi-threaded chat server and client system built in Java.

Originally developed as a group project for CS313 — System Concurrency at university.
Independently expanded to demonstrate real concurrency patterns used in production systems.

---

## Architecture

```
+--------+        TCP Socket         +------------------+
| Client | <-----------------------> |      Server      |
+--------+                           | (Thread Pool 200)|
                                     +------------------+
                                              |
                                 one ClientHandler per client
                                              |
                                    +------------------+
                                    |  ClientHandler   |
                                    | (rate limiting)  |
                                    +------------------+
                                              |
                                    +------------------+
                                    |  SafeGroupChat   |
                                    |                  |
                                    | - member list    |
                                    | - msg history    |
                                    | - BlockingQueue  |
                                    | - dispatcher     |
                                    +------------------+
```

**Message flow when a user sends a message:**
```
User types message
  → Client.java sends over TCP socket
    → ClientHandler reads it, checks rate limit
      → calls sendMessage() on group
        → message dropped into group's BlockingQueue
          → dispatcher thread picks it up
            → delivers to all group members
```

**Key design principle:** The user's thread never blocks on delivery.
It drops the message and immediately goes back to listening.

---

## What Was Built & Why

### Original (University Project)
- TCP socket server with one raw thread per client
- Group chat with `ReentrantLock` for thread safety
- `SafeGroupChat` and `UnsafeGroupChat` to demonstrate race conditions
- Message history replay on group join
- File transfer and video streaming between clients

### Expanded Independently

#### Phase 1 — Thread Pool
**Problem:** Raw thread per client is unbounded. At scale, memory and OS scheduling collapse.

**Solution:** Fixed `ExecutorService` thread pool of 200 threads. Threads are reused — when a client disconnects, that thread picks up the next waiting connection.

**Why 200?** Chat servers are I/O-bound. Threads spend most of their time blocked on `socket.read()`, not computing. So we can safely run far more threads than CPU cores.

```java
private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_COUNT);
// ...
threadPool.execute(handler); // replaces: new Thread(handler).start()
```

#### Phase 2 — BlockingQueue per Group
**Problem:** The sender's thread looped through all group members and wrote to each socket directly. One slow client could freeze the sender's thread entirely.

**Solution:** Each `SafeGroupChat` owns a `LinkedBlockingQueue<String[]>` and a dedicated dispatcher thread. `sendMessage()` drops the message in the queue and returns immediately. The dispatcher drains the queue and handles delivery.

```
Before: sender's thread → loops all members → writes to each socket (can block)
After:  sender's thread → queue.offer(message) → done
        dispatcher      → queue.take() → loops members → writes to sockets
```

#### Phase 3 — Rate Limiting + Graceful Shutdown
**Rate limiting:** Each `ClientHandler` tracks message count and window start time. Max 5 messages per 10 seconds. Excess messages are rejected with a countdown telling the user when they can send again.

**Graceful shutdown:** A JVM shutdown hook catches `Ctrl+C`. The thread pool stops accepting new tasks (`shutdown()`) and waits up to 45 seconds for in-flight work to finish (`awaitTermination()`). Dispatcher threads are daemon threads — they die cleanly with the server.

---

## Commands

| Command | Description |
|---|---|
| `!help` | Show all commands |
| `!quit` | Disconnect from server |
| `!status` | Show all users' statuses |
| `!status <text>` | Set your status |
| `!friend <username>` | Send a friend request |
| `!friends` | List your friends |
| `!create <groupName>` | Create a group and auto-join |
| `!join <groupName>` | Join a group (replays history) |
| `!leave` | Leave current group |
| `!gm <groupName> <message>` | Send to a group without joining |
| `!sendfile <username>` | Send a file to a user |
| `!stream <username>` | Stream video to a user |
| `exit` | Disconnect |

---

## Thread Safety

| Class | Mechanism | Why |
|---|---|---|
| `SafeGroupChat` | `ReentrantLock` | Protects member list and message history from concurrent modification |
| `ClientHandler` | `Collections.synchronizedList` | Shared client list accessed by multiple handler threads |
| `groupChats` map | `ConcurrentHashMap` | Multiple handlers create/join groups concurrently |
| `sequenceNumber` | `AtomicInteger` | Lock-free thread-safe counter for message ordering |
| `UnsafeGroupChat` | None (intentional) | Used in tests to demonstrate what race conditions look like |

---

## Requirements

- Java 17+
- No external dependencies — standard library only

---

## How to Run

Open two terminals.

**Terminal 1 — Server:**
```bash
javac src/Server.java src/ClientHandler.java
java -cp src Server
```

**Terminal 2 — Client:**
```bash
javac src/Client.java
java -cp src Client
```

Server listens on port `8888` by default.

---

## Quick Test

1. Start server
2. Start three clients (A, B, C)
3. A: `!create groupA`
4. B: `!join groupA`
5. C: `!create groupB`
6. A or B send `hello` — only groupA members see it
7. C sends `hey` — only groupB members see it
8. Test rate limit: send 6+ messages rapidly — 6th should be rejected with countdown
9. `Ctrl+C` server — observe graceful shutdown

---

## Contributors

**Original university project:**
Mohamed Sharif, Mohammad Rayyan, Russell Hall, Ghassan Shalayel, John Holland, Ethan Holland

**Production expansion (Phases 1–3):**
Mohammad Rayyan