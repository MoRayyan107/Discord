# Stage 2: Refactor to P2P and Multithreading

## Quick Homework for Tonight (1 hour total)

Watch these before tomorrow:

**1. What's P2P?** (10 mins)
https://www.youtube.com/results?search_query=peer+to+peer+vs+client+server

**2. Java P2P Chat** (20 mins)
https://www.youtube.com/results?search_query=java+peer+to+peer+chat+tutorial

**3. Threading for Chat** (15 mins)
https://www.youtube.com/results?search_query=java+multithreading+chat+application

**4. Java Threads Basics** (10 mins)
https://www.youtube.com/results?search_query=java+runnable+interface+tutorial

---

# Stage 3: Features Implementation (Safe and Unsafe Versions)

---

## Whats the Current Behavior?
- Default port is ```8888``` but can be changed in the code.
- Server and Multiple Clients can be run on same machine in localhost.
- clients can join, create groups and send messages to groups.

## WHat We Need To Do?
- Refactor the Code for Client Handler Specially to be able to handle multiple clients at the same time using Threads.
- Fix the pottential issues pointed out in the "Potential Errors" section below.


## What We Did:
- Refactored ClientHandler — private handlers for each command, switch case routing. 
- Fixed concurrent modification on clients list — Collections.synchronizedList + synchronized broadcast. 
- Fixed message history replay — only triggered on /join, not on server connect.
- Fixed group creator not receiving messages — auto-join on /create. 
- Implemented SafeMessageHistory and UnsafeMessageHistory to demonstrate thread safety. 
- Added ChatParticipant interface to decouple group chat from ClientHandler. 
- Dead client removed from list on broadcast failure.

---

## What You Need For This Project

- energy drinks highly recommended

---


### Potential errors 

- Bugs:
  - No bugs to be found yet, if found please report them to us and we will fix them as soon as possible.

- Fixed Bugs:
  - Major Bug solved and Code ahas been cleaned up and refactored
  - Test bug fixed on MessageGroupChat Test cases
  - Fixed clients from Group A receiving messages from Group B (was due to shared message history on server connect instead of per-group join).
  - Fixed group creator not receiving messages (was due to not auto-joining on /create).
