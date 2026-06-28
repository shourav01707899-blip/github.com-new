import asyncio, json, random, string, os
import websockets

PORT = int(os.environ.get("PORT", 8765))

clients = {}  # websocket → {id, room}
rooms   = {}  # room_code → [player_id, ...]

def new_id():
    return ''.join(random.choices(string.digits, k=6))

def new_room():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

async def send(ws, data):
    try:
        await ws.send(json.dumps(data))
    except:
        pass

async def broadcast(room_code, data, exclude=None):
    for ws, info in list(clients.items()):
        if info["room"] == room_code and info["id"] != exclude:
            await send(ws, data)

async def handle(ws):
    pid = new_id()
    clients[ws] = {"id": pid, "room": None}
    await send(ws, {"type": "connected", "id": pid})
    print(f"[+] {pid} connected")

    try:
        async for message in ws:
            try:
                msg = json.loads(message)
            except:
                continue

            t = msg.get("type", "")

            if t == "create_room":
                code = new_room()
                rooms[code] = [pid]
                clients[ws]["room"] = code
                await send(ws, {"type": "room_created", "room": code, "host": pid})
                print(f"[R] Room {code} created by {pid}")

            elif t == "join_room":
                code = msg.get("room", "").upper()
                if code in rooms:
                    rooms[code].append(pid)
                    clients[ws]["room"] = code
                    host = rooms[code][0]
                    await send(ws, {
                        "type": "room_joined",
                        "room": code,
                        "host": host,
                        "players": rooms[code]
                    })
                    await broadcast(code, {"type": "player_joined", "id": pid}, exclude=pid)
                    print(f"[R] {pid} joined {code}")
                else:
                    await send(ws, {"type": "error", "msg": "room_not_found"})

            elif t == "relay":
                room = clients[ws]["room"]
                if room:
                    await broadcast(room, {
                        "type": "relay",
                        "from": pid,
                        "data": msg.get("data")
                    }, exclude=pid)

            elif t == "ping":
                await send(ws, {"type": "pong"})

    except Exception as e:
        print(f"[!] {pid} error: {e}")
    finally:
        room = clients[ws]["room"]
        del clients[ws]

        if room and room in rooms:
            rooms[room] = [p for p in rooms[room] if p != pid]
            if not rooms[room]:
                del rooms[room]
                print(f"[R] Room {room} closed")
            else:
                new_host = rooms[room][0]
                print(f"[H] New host: {new_host}")
                await broadcast(room, {
                    "type": "host_changed",
                    "new_host": new_host
                })

        print(f"[-] {pid} disconnected")

async def main():
    print(f"WebSocket Relay running on port {PORT}")
    async with websockets.serve(handle, "0.0.0.0", PORT):
        await asyncio.Future()

asyncio.run(main())
