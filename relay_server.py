import asyncio, json, random, string, os
import websockets

PORT = int(os.environ.get("PORT", 8765))

# সব connected player
clients = {}   # websocket → {id, state, match_id, score}
# state: "idle" / "waiting" / "in_game"
# score: ক্লায়েন্টের নিজের রিপোর্ট করা নেট/ডিভাইস quality — বেশি = ভালো host candidate

waiting = []   # [websocket, ...]  matchমেকিং queue

matches = {}   # match_id → {players: [ws,...], host: id, peer_map: {server_id: multiplayer_id}}

MIN_PLAYERS = 2   # কতজন হলে match শুরু হবে


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


def build_peer_map(host_ws, other_ws_list):
    """
    host সবসময় Godot multiplayer id 1 পায় (WebRTCMultiplayerPeer-এ server = id 1)।
    বাকিদের score অনুযায়ী (বেশি score আগে) 2, 3, 4... করে ID দেওয়া হয় —
    যাতে সবার কাছে deterministic একই map যায়।
    """
    peer_map = {clients[host_ws]["id"]: 1}
    ordered = sorted(other_ws_list, key=lambda w: clients[w]["score"], reverse=True)
    for i, ws in enumerate(ordered, start=2):
        peer_map[clients[ws]["id"]] = i
    return peer_map


async def try_start_match():
    while len(waiting) >= MIN_PLAYERS:
        match_players = waiting[:MIN_PLAYERS]
        del waiting[:MIN_PLAYERS]

        match_id = new_match_id()

        # সবচেয়ে বেশি score-ওয়ালা প্লেয়ার host/game-server হবে
        host_ws = max(match_players, key=lambda w: clients[w]["score"])
        host_id = clients[host_ws]["id"]
        others = [w for w in match_players if w != host_ws]
        peer_map = build_peer_map(host_ws, others)

        matches[match_id] = {
            "players":  match_players,
            "host":     host_id,
            "peer_map": peer_map,
        }

        player_ids = [clients[w]["id"] for w in match_players]

        for ws in match_players:
            clients[ws]["state"]    = "in_game"
            clients[ws]["match_id"] = match_id
            await send(ws, {
                "type":     "match_found",
                "match_id": match_id,
                "host":     host_id,
                "players":  player_ids,
                "peer_map": peer_map,
                "you":      clients[ws]["id"]
            })

        print(f"[MATCH] {match_id} শুরু — players: {player_ids}, host: {host_id}, peer_map: {peer_map}")


async def handle(ws):
    pid = new_id()
    clients[ws] = {"id": pid, "state": "idle", "match_id": None, "score": 0.0}
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
                # ক্লায়েন্ট নিজের নেট/ডিভাইস quality score পাঠায় — host বাছাইয়ে ব্যবহার হবে
                try:
                    clients[ws]["score"] = float(msg.get("score", 0))
                except (TypeError, ValueError):
                    clients[ws]["score"] = 0.0

                if ws not in waiting and clients[ws]["state"] == "idle":
                    waiting.append(ws)
                    clients[ws]["state"] = "waiting"
                    await send(ws, {
                        "type":    "searching",
                        "waiting": len(waiting)
                    })
                    print(f"[Q] {pid} queue তে ঢুকলো (score={clients[ws]['score']:.1f}) — total: {len(waiting)}")
                    await try_start_match()

            # ── Queue বাতিল ───────────────────
            elif t == "cancel_search":
                if ws in waiting:
                    waiting.remove(ws)
                    clients[ws]["state"] = "idle"
                    await send(ws, {"type": "search_cancelled"})
                    print(f"[Q] {pid} queue থেকে বের হলো")

            # ── WebRTC Signaling relay ────────
            elif t == "signal":
                mid = clients[ws].get("match_id")
                target_id = msg.get("target_id")
                if mid and mid in matches and target_id:
                    for target_ws in matches[mid]["players"]:
                        if clients[target_ws]["id"] == target_id:
                            await send(target_ws, {
                                "type":        "signal",
                                "from":        pid,
                                "signal_type": msg.get("signal_type"),
                                "data":        msg.get("data")
                            })
                            break

            # ── debug/UI info ──────────────────
            elif t == "p2p_connected":
                mid = clients[ws].get("match_id")
                if mid:
                    await broadcast_match(mid, {
                        "type": "player_p2p_ready",
                        "id":   pid
                    }, exclude=ws)

            # ── Ping (latency মাপার জন্যও ব্যবহার হয়) ──
            elif t == "ping":
                await send(ws, {"type": "pong"})

    except Exception as e:
        print(f"[!] {pid} error: {e}")
    finally:
        if ws in waiting:
            waiting.remove(ws)

        mid = clients[ws].get("match_id")
        if mid and mid in matches:
            was_host = matches[mid]["host"] == pid
            matches[mid]["players"] = [
                w for w in matches[mid]["players"] if w != ws
            ]

            if len(matches[mid]["players"]) == 0:
                del matches[mid]
                print(f"[MATCH] {mid} শেষ")
            else:
                remaining = matches[mid]["players"]

                if was_host:
                    # host চলে গেছে — বাকিদের মধ্যে সবচেয়ে ভালো score-ওয়ালা নতুন host
                    new_host_ws = max(remaining, key=lambda w: clients[w]["score"])
                    new_host_id = clients[new_host_ws]["id"]
                    new_peer_map = build_peer_map(
                        new_host_ws, [w for w in remaining if w != new_host_ws]
                    )

                    matches[mid]["host"]     = new_host_id
                    matches[mid]["peer_map"] = new_peer_map

                    await broadcast_match(mid, {
                        "type":     "host_migrated",
                        "host":     new_host_id,
                        "peer_map": new_peer_map,
                    })
                    print(f"[MATCH] {mid} — host সরে যাওয়ায় নতুন host: {new_host_id}")
                else:
                    # সাধারণ প্লেয়ার চলে গেলে topology ভাঙার দরকার নেই,
                    # host একাই সবার সাথে connected — শুধু জানিয়ে দাও
                    await broadcast_match(mid, {
                        "type": "player_left",
                        "id":   pid,
                    })

        del clients[ws]
        print(f"[-] {pid} disconnected")


async def main():
    print(f"[SERVER] WebRTC Signaling + Matchmaking (host-authoritative) চালু — port {PORT}")
    print(f"[SERVER] Min players per match: {MIN_PLAYERS}")
    async with websockets.serve(handle, "0.0.0.0", PORT):
        await asyncio.Future()


asyncio.run(main())
