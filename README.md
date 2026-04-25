# Discord Project

A multi-threaded chat server and client system (Java) created for CS313 — System Concurrency. The project demonstrates:
- socket programming (TCP),
- multithreading and client handling,
- synchronization techniques (locks, concurrent collections),
- and per-group message history.

Contributors
---
- Mohamed Sharif
- Mohammad Rayyan
- Russell Hall
- Ghassan Shalayel
- John Holland
- Ethan Holland

What this repository contains
---
- A simple server (`Server.java`) that listens for TCP clients (default port 8888).
- A console client (`Client.java`) that connects, sends commands and receives messages.
- A `ClientHandler` that parses commands and routes messages to either all clients or per-group.
- Group chat implementations in `src/groupchat/`:
  - `SafeGroupChat` (uses locks to protect membership/history)
  - `UnsafeGroupChat` (no synchronization — used for teaching/tests)
- Small unit-like tests and examples in `src/groupchat/GroupChatTest.java` and `src/messagehistory/MessageHistoryTest.java`.

Features / Behavior
---
- Multiple clients can connect concurrently.
- Clients can create and join named group chats. Each group maintains its own message history.

- Commands supported by the client:

| Command                   | Description                                    |
|---------------------------|------------------------------------------------|
| `!help`                   | Show this help message.                        |
|`!quit`                     | Disconnect from the Group.                     |
|`!status`                   | Show current connection and group status.      |
| `!status <statMessage>`         | Update your status message (not broadcasted).  |
| `!create <groupName>`       | Create a group and auto-join it.               |
| `!join <groupName>`         | Join an existing group and replay its history. |
| `!gm <groupName> <message>` | Explicitly send a message to a named group.    |
| `exit`                      | Disconnect from the server.                    |


Notes about thread-safety and tests
---
- `SafeClasses` uses a ReentrantLock to protect its member list and message history.
- `UnsafeClasses` is intentionally not synchronized and will show race conditions under concurrent access (used by tests to demonstrate failures).

Requirements
---
- Java 11 or later is recommended (the project uses modern language features in some places).
- No external dependencies — the project uses the Java standard library.

How to compile and run (simple)
---
Open two terminals/powershell windows.

1) Server terminal (compile and run server):

```powershell
javac src\Server.java src\ClientHandler.java
java -cp src Server
```

2) Client terminal (compile and run client):

```powershell
javac src\Client.java
java -cp src Client
```

Notes:
- The commands above assume you're running them from the project root and that the `src` folder contains `.java` files at top-level. Adjust classpath or compile commands if you use an IDE.
- Server listens on port 8888 by default. Change the code if you want a different port.

How to test group behavior (manual quick test)
---
1. Start server.
2. Start three clients (A, B, C).
3. A: `!create groupA` (A auto-joins)
4. B: `!join groupA`
5. C: `!create groupB` and `!join groupB`
6. From A or B, send plain text: `hello` — only A & B should see it.
7. From C, send plain text: `hey` — only members of `groupB` should see it.
8. Use `!gm groupB secret` to send to another group explicitly.

License / attribution
---
This repository was created as a university coursework project. Use for learning and experimental purposes.
