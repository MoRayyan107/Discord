# Discord-style Chat Server — Project Notes
> CS313 System Concurrency | Java | Expanded for Internship Portfolio

---

## 1. Project Overview

A multi-threaded chat server and client system built in Java.
Originally built for CS313 (System Concurrency). Expanded independently for internship prep.

**What it does:**
- Multiple clients connect to a server over TCP
- Clients can create and join named group chats
- Messages sent in a group are broadcast to all members
- Each group maintains its own message history (replayed when you join)
- Thread safety demonstrated via `SafeGroupChat` (locked) and `UnsafeGroupChat` (intentionally broken — shows race conditions)

**Why it matters:**
- Demonstrates real concurrency problems and solutions
- Mirrors how production chat systems work at a fundamental level
- Every design decision has a reason — not just "it works"
- Conceptually the same class of problems Discord, Slack, etc. solve at scale

---

## 2. Architecture — How Everything Connects

```
+--------+        TCP Socket         +--------+
| Client | <-----------------------> | Server |
+--------+                           +--------+
                                          |
                          creates one per connected client
                                          |
                                  +---------------+
                                  | ClientHandler |
                                  +---------------+
                                          |
                          reads commands, routes messages
                                          |
                              +---------------------+
                              |   SafeGroupChat     |
                              |  (one per group)    |
                              |                     |
                              |  - member list      |
                              |  - message history  |
                              |  - BlockingQueue    | <-- Phase 2 addition
                              +---------------------+
```

**Who does what:**

| Class | Where it runs | Responsibility |
|---|---|---|
| `Client.java` | User's machine | Reads user input, sends to server, prints received messages |
| `Server.java` | Server machine | Listens for connections, hands clients to the thread pool |
| `ClientHandler.java` | Server (one per client) | Reads that client's messages, parses commands, routes to group |
| `SafeGroupChat.java` | Server (one per group) | Owns members, history, message queue — thread safe |
| `UnsafeGroupChat.java` | Server | Same but no sync — used to demonstrate race conditions |

**Key rule:** Clients never talk to each other directly. All traffic goes through the server.

**Message flow — A sends "hello" to groupA:**
```
A types "hello"
  → Client.java sends over socket
    → ClientHandler (A's handler on server) reads it
      → calls broadcastMessage()
        → drops message into groupA's BlockingQueue
          → groupA's dispatcher thread picks it up
            → loops through members, writes to B and C's sockets
              → B and C's Client.java prints it
```

---

## 3. Core Concepts

### 3.1 Sockets & TCP

A **socket** is one end of a two-way communication link between two programs over a network.

**TCP** (Transmission Control Protocol) means:
- Connection is established before any data is sent (handshake)
- Data arrives in order, nothing gets lost
- The right choice for chat — you can't have messages arriving out of order or disappearing

```java
// Server side — waits for connections
ServerSocket serverSocket = new ServerSocket(8888);
Socket clientSocket = serverSocket.accept(); // blocks until someone connects

// Client side — connects to server
Socket socket = new Socket("192.168.x.x", 8888);
```

Once you have a `Socket`, you wrap it in streams to read/write:
```java
BufferedReader in  = new BufferedReader(new InputStreamReader(socket.getInputStream()));
PrintWriter   out  = new PrintWriter(socket.getOutputStream(), true);
```

---

### 3.2 Threads — What They Are & The Problem

A **thread** is an independent path of execution inside your program. Multiple threads run concurrently (or in parallel on multi-core CPUs).

**Why we need threads in a chat server:**
- `serverSocket.accept()` blocks — it waits until a client connects
- `socket.read()` blocks — it waits until data arrives
- Without threads, one blocking call freezes the entire server
- With threads, each client gets its own thread — one blocking doesn't affect others

**The problem with raw threads (what the original code did):**
```java
// Original — one new OS thread per client
Thread thread = new Thread(handler);
thread.start();
```

- Every thread costs ~1MB of stack memory
- OS has to schedule and context-switch between all of them
- 500 clients = 500 threads = memory pressure + scheduling overhead
- No upper limit — unbounded thread creation collapses under load

This is called **unbounded thread creation** and it's a classic mistake that works in demos but fails in production.

---

### 3.3 Thread Pool — Why & How

A thread pool flips the model:
- Instead of creating a thread per client, create N threads at startup
- Threads sit idle waiting for work
- New client arrives → hand work to a free thread
- Thread finishes → goes back to pool, picks up next task
- Threads are **reused**, not created and destroyed

```java
// Fixed pool — predictable ceiling, right for servers
ExecutorService threadPool = Executors.newFixedThreadPool(200);

// Hand work to the pool — pool decides which thread runs it
threadPool.execute(handler); // handler implements Runnable
```

**Why 200?**

Chat servers are **I/O-bound** — threads spend most of their time blocked on `socket.read()` waiting for users to type. They're not doing heavy computation.

Rule of thumb:
```
Thread count = CPU cores × (1 + wait time / compute time)
```

If a thread waits 100ms on network and spends 1ms processing:
- Ratio = 100
- On an 8-core machine → ~800 threads theoretically optimal
- But memory caps this — 200 is a reasonable, defensible starting point
- In production: make it configurable and tune based on load testing

**CPU-bound vs I/O-bound:**
| | CPU-bound | I/O-bound |
|---|---|---|
| Example | Image processing, sorting | Chat server, web server, DB queries |
| Threads blocked on | Nothing — always computing | Network/disk reads |
| Optimal thread count | ≈ CPU core count | Much higher than core count |

**The executor pattern:**
The pool doesn't care what the `Runnable` does — it just runs it. Your concurrency layer (`ExecutorService`) and application logic (`ClientHandler`) are completely separate. This is loosely called the **executor pattern**.

---

### 3.4 BlockingQueue — Why & How

**The problem with direct delivery:**

In the original code, when A sends a message, A's handler thread loops through every group member and writes to their socket directly:
```java
for (ClientHandler client : members) {
    client.sendToClient(message); // A's thread does this for everyone
}
```

If there are 50 members and one has a slow connection, **A's thread blocks** waiting on that slow write. A can't receive anything while frozen. One slow client affects everyone.

**What BlockingQueue changes:**

A's thread drops the message into a queue and immediately moves on. A dedicated **dispatcher thread** per group drains the queue and does the delivery.

```
Before:
A's thread → loops all members → writes to each socket (can block here)

After:
A's thread → queue.offer(message) → done, back to listening
Dispatcher → picks from queue → loops all members → writes to sockets
```

**`put()` vs `offer()`:**

| Method | Queue full behaviour | Use case |
|---|---|---|
| `put(msg)` | Blocks until space available | Risk: freezes the calling thread |
| `offer(msg)` | Returns false immediately | Non-blocking, you control what happens |
| `offer(msg, 100, MILLISECONDS)` | Tries for 100ms, then gives up | Best of both — not instant drop, not infinite block |

Use `offer()` with a timeout. If the queue is full, the server is overloaded — better to log it or tell the user "server busy" than to freeze A's thread.

**Where the queue lives:**
- One `BlockingQueue` per group, inside `SafeGroupChat`
- One dispatcher thread per group, also owned by `SafeGroupChat`
- `ClientHandler` just calls `sendMessage()` as before — it doesn't know or care about the queue internally

---

### 3.5 Synchronization — Locks & Thread Safety

When multiple threads access shared data, you get **race conditions** — unpredictable behaviour depending on which thread runs first.

**Example race condition (UnsafeGroupChat):**
```
Thread A reads member list: [B, C]
Thread D joins group — list becomes [B, C, D]
Thread A writes to [B, C] — D misses the message
```

Two ways to fix this in Java:

**1. `synchronized` keyword:**
```java
synchronized (clients) {
    for (ClientHandler client : clients) {
        client.sendToClient(message);
    }
}
```
Simple but coarse — only one thread can enter at a time. Fine for simple cases.

**2. `ReentrantLock` (what SafeGroupChat uses):**
```java
private final ReentrantLock lock = new ReentrantLock();

lock.lock();
try {
    // protected code
} finally {
    lock.unlock(); // always unlock, even if exception thrown
}
```

More flexible than `synchronized`:
- Can try to acquire without blocking (`tryLock()`)
- Can be acquired and released in different methods
- `finally` block guarantees unlock even on exception — important

**`SafeGroupChat` vs `UnsafeGroupChat`:**
The unsafe version is intentional — it's there to demonstrate what race conditions look like in tests. This is good design because it shows you understand *why* safety matters, not just how to add a lock.

---

## 4. Class by Class

### `Server.java`
- Entry point
- Creates `ServerSocket` on port 8888
- Owns the `ExecutorService` thread pool (instance variable, not static)
- Accept loop: `serverSocket.accept()` → creates `ClientHandler` → `threadPool.execute(handler)`
- Pool size defined as a named constant `THREAD_POOL_COUNT = 200`

### `ClientHandler.java`
- Implements `Runnable` — this is what the thread pool executes
- Owns the `BufferedReader` and `PrintWriter` for one client's socket
- `run()` loop: reads lines from client, parses commands, routes to the right method
- Key methods:
  - `broadcastMessage()` — sends to group or general chat
  - `sendToClient()` — writes directly to this client's socket
  - Command handlers for `!create`, `!join`, `!quit`, `!status`, etc.

### `SafeGroupChat.java`
- Represents one group chat
- Owns: member list, message history, `ReentrantLock`
- Phase 2 addition: owns `BlockingQueue` and dispatcher thread
- `sendMessage()` — puts message in queue (after Phase 2)
- `addMember()` / `removeMember()` — protected by lock
- History replay on join — new members see previous messages

### `UnsafeGroupChat.java`
- Same as `SafeGroupChat` but no synchronization
- Used in tests to demonstrate race conditions
- Exists to prove you understand the problem, not just the solution

### `Client.java`
- Runs on user's machine
- Two threads: one reads from keyboard and sends to server, one reads from server and prints
- Supports all commands: `!help`, `!create`, `!join`, `!gm`, `!quit`, `!status`, `exit`

---

## 5. The Expansion — What Was Added & Why

### Phase 1 — Thread Pool ✅

**What changed:** `Server.java` — replaced raw thread creation with `ExecutorService`

**Before:**
```java
Thread thread = new Thread(handler);
thread.start();
```

**After:**
```java
// Instance variable
private final ExecutorService threadPool = Executors.newFixedThreadPool(THREAD_POOL_COUNT);

// In accept loop
threadPool.execute(handler);
```

**Why:** Unbounded thread creation doesn't scale. Pool reuses threads, caps memory usage, and gives you control over concurrency level.

---

### Phase 2 — BlockingQueue per Group (in progress)

**What changes:** `SafeGroupChat.java` — `sendMessage()` no longer delivers directly, it enqueues. A dispatcher thread does delivery.

**Why:** Decouples senders from receivers. A slow client can't freeze a sender's thread. This is the pattern behind real message brokers (Kafka, RabbitMQ conceptually).

---

### Phase 3 — Rate Limiting + Graceful Shutdown (planned)

**Rate limiting:** Per-user message rate cap using `AtomicLong` timestamp tracking. Prevents one user spamming and flooding others' queues.

**Graceful shutdown:** When server stops — finish draining queues, don't cut clients mid-message. `threadPool.shutdown()` then `threadPool.awaitTermination()`.

**Why:** The difference between "I built a chat app" and "I built a production-aware chat app."

---

## 6. Interview Q&A

**Q: Walk me through your server architecture.**

A: The server listens on a TCP socket. When a client connects, instead of spawning a raw thread, it hands the `ClientHandler` to a fixed thread pool of 200 threads. The pool reuses threads — when a client disconnects, that thread picks up the next waiting connection. `ClientHandler` is the server's representative for one client — it reads commands and routes messages to the appropriate group. Each group is a `SafeGroupChat` object with a `ReentrantLock` protecting membership and history, and a `BlockingQueue` so message delivery is decoupled from message sending.

---

**Q: Why a thread pool instead of creating a thread per client?**

A: Raw thread creation is unbounded — each thread costs ~1MB of stack memory and the OS has to schedule all of them. At 500 clients that's 500 threads most of which are idle. A fixed pool caps this. Since our server is I/O-bound (threads mostly wait on socket reads), we can run more threads than CPU cores — 200 is a reasonable starting point, tunable based on load testing.

---

**Q: What's the difference between `SafeGroupChat` and `UnsafeGroupChat`?**

A: `SafeGroupChat` uses a `ReentrantLock` to protect the member list and message history from concurrent modification. `UnsafeGroupChat` has no synchronization — it's intentionally broken and used in tests to demonstrate race conditions. Having both shows I understand *why* thread safety matters, not just how to add a lock.

---

**Q: Why `ReentrantLock` over `synchronized`?**

A: `ReentrantLock` is more flexible. It supports `tryLock()` so a thread can attempt to acquire without blocking, and it can be locked/unlocked in different methods. The `finally` block pattern guarantees the lock is always released even if an exception is thrown — `synchronized` handles this automatically, but `ReentrantLock` makes the intent explicit. For a simple case `synchronized` is fine, but `ReentrantLock` gives you more control as complexity grows.

---

**Q: What is a race condition? Give me an example from your project.**

A: A race condition is when the behaviour of a program depends on the relative timing of threads — the outcome is unpredictable. In `UnsafeGroupChat`, if thread A reads the member list while thread B is adding a new member, A might iterate over a half-updated list. The new member might miss a message or A's thread might throw a `ConcurrentModificationException`. The `ReentrantLock` in `SafeGroupChat` prevents this by ensuring only one thread can modify or read the member list at a time.

---

**Q: What would happen if your thread pool was too small?**

A: Connections would still be accepted by the server (the TCP handshake completes), but the clients would sit connected and silent — no handler thread is processing them yet. They'd appear connected but messages wouldn't go through until a thread became free. If the pool queue fills up too, new tasks get rejected. This is why pool size and queue depth need to be monitored in production.

---

**Q: Why `offer()` instead of `put()` for the BlockingQueue?**

A: `put()` blocks the calling thread if the queue is full — in a server context that would freeze the sender's handler thread, meaning they can't receive anything either. `offer()` with a timeout tries for a set duration then gives up, letting you handle the "queue full" case explicitly — log it, notify the user, drop the message. It's a more controlled failure mode than silently blocking.

---

*Notes written alongside project development — not definitions, but understanding built through doing.*
