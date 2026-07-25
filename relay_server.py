import asyncio, json, random, string, os
import websockets

PORT = int(os.environ.get("PORT", 8765))

# সব connected player
clients = {}   # websocket → {id, state, match_id}
# state: "idle" / "waiting" / "in_game"

# Waiting queue — match এর জন্য অপেক্ষা করছে
waiting = []   # [websocket, ...]

# Active matches
matches = {}   # match_id → {players: [ws,...], host: id}

MIN_PLAYERS = 2   # কতজন হলে match শুরু হবে (পরে বাড়িয়ে ২০ করে দিতে পারেন)

def new_id():
    return ''.join(random.choices(string.digits, k=6))

def new_match_id():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

async def send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except:
        pass

async def broadcast_match(match_id, data, exclude=None):
    if match_id not in matches:
        return
    for ws in matches[match_id]["players"]:
        if ws != exclude:
            await send(ws, data)

async def try_start_match():
    while len(waiting) >= MIN_PLAYERS:
        match_players = waiting[:MIN_PLAYERS]
        del waiting[:MIN_PLAYERS]

        match_id = new_match_id()
        host_ws  = match_players[0]
        host_id  = clients[host_ws]["id"]

        matches[match_id] = {
            "players" : match_players,
            "host"    : host_id,
            "alive"   : len(match_players)
        }

        player_ids = [clients[w]["id"] for w in match_players]

        for ws in match_players:
            clients[ws]["state"]    = "in_game"
            clients[ws]["match_id"] = match_id
            await send(ws, {
                "type"     : "match_found",
                "match_id" : match_id,
                "host"     : host_id,
                "players"  : player_ids,
                "you"      : clients[ws]["id"]
            })

        print(f"[MATCH] {match_id} শুরু — players: {player_ids}, host: {host_id}")

async def handle(ws):
    pid = new_id()
    clients[ws] = {"id": pid, "state": "idle", "match_id": None}
    await send(ws, {"type": "connected", "id": pid})
    print(f"[+] {pid} connected")

    try:
        async for message in ws:
            try:
                msg = json.loads(message)
            except:
                continue

            t = msg.get("type", "")

            # ── Matchmaking শুরু ──────────────
            if t == "find_match":
                if ws not in waiting and clients[ws]["state"] == "idle":
                    waiting.append(ws)
                    clients[ws]["state"] = "waiting"
                    await send(ws, {
                        "type"    : "searching",
                        "waiting" : len(waiting)
                    })
                    print(f"[Q] {pid} queue তে ঢুকলো — total: {len(waiting)}")
                    await try_start_match()

            # ── Queue বাতিল ───────────────────
            elif t == "cancel_search":
                if ws in waiting:
                    waiting.remove(ws)
                    clients[ws]["state"] = "idle"
                    await send(ws, {"type": "search_cancelled"})
                    print(f"[Q] {pid} queue থেকে বের হলো")

            # ── WebRTC Signaling: শুধু SDP offer/answer + ICE candidate ──
            # gameplay data এখন এখান দিয়ে যাবে না — শুধু connection
            # স্থাপনের তথ্য এখানে relay হয়। একবার WebRTC connect হয়ে
            # গেলে players সরাসরি P2P-তে কথা বলবে, সার্ভার লাগবে না।
            elif t == "signal":
                mid = clients[ws].get("match_id")
                target_id = msg.get("target_id")
                if mid and mid in matches and target_id:
                    for target_ws in matches[mid]["players"]:
                        if clients[target_ws]["id"] == target_id:
                            await send(target_ws, {
                                "type"        : "signal",
                                "from"        : pid,
                                "signal_type" : msg.get("signal_type"),  # "offer" | "answer" | "ice_candidate"
                                "data"        : msg.get("data")
                            })
                            break

            # ── P2P connection সফল হয়েছে জানানো (optional, debug/UI-এর জন্য) ──
            elif t == "p2p_connected":
                mid = clients[ws].get("match_id")
                if mid:
                    await broadcast_match(mid, {
                        "type" : "player_p2p_ready",
                        "id"   : pid
                    }, exclude=ws)

            # ── Ping ──────────────────────────
            elif t == "ping":
                await send(ws, {"type": "pong"})

    except Exception as e:
        print(f"[!] {pid} error: {e}")
    finally:
        # Disconnect হলে
        if ws in waiting:
            waiting.remove(ws)

        mid = clients[ws].get("match_id")
        if mid and mid in matches:
            matches[mid]["players"] = [
                w for w in matches[mid]["players"] if w != ws
            ]
            matches[mid]["alive"] -= 1

            if len(matches[mid]["players"]) == 0:
                del matches[mid]
                print(f"[MATCH] {mid} শেষ")
            else:
                # host নিজেই চলে গিয়ে থাকলে নতুন host বেছে দাও
                if matches[mid]["host"] == pid:
                    new_host_ws = matches[mid]["players"][0]
                    new_host_id = clients[new_host_ws]["id"]
                    matches[mid]["host"] = new_host_id
                    await broadcast_match(mid, {
                        "type"     : "host_changed",
                        "new_host" : new_host_id
                    })
                    print(f"[MATCH] {mid} নতুন host: {new_host_id}")

                # বাকিদের জানাও
                await broadcast_match(mid, {
                    "type"   : "player_left",
                    "id"     : pid,
                    "alive"  : matches[mid]["alive"]
                })

        del clients[ws]
        print(f"[-] {pid} disconnected")

async def main():
    print(f"[SERVER] WebRTC Signaling + Matchmaking চালু — port {PORT}")
    print(f"[SERVER] Min players per match: {MIN_PLAYERS}")
    async with websockets.serve(handle, "0.0.0.0", PORT):
        await asyncio.Future()

asyncio.run(main())
