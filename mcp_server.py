import asyncio
import json
import websockets
import uuid # For message IDs, though client might generate them

# In-memory storage for connected agents
# { 'agent_id': websocket_connection }
connected_agents = {}
# { websocket_connection: 'agent_id' }
reverse_agent_lookup = {}


async def register_agent(websocket, agent_id):
    """Registers a new agent."""
    if agent_id in connected_agents:
        # Agent ID already taken
        error_msg = {
            "message_id": str(uuid.uuid4()),
            "type": "register_nack",
            "payload": {"reason": f"Agent ID '{agent_id}' already taken."}
        }
        await websocket.send(json.dumps(error_msg))
        return False

    connected_agents[agent_id] = websocket
    reverse_agent_lookup[websocket] = agent_id
    print(f"Agent '{agent_id}' registered from {websocket.remote_address}")

    ack_msg = {
        "message_id": str(uuid.uuid4()),
        "type": "register_ack",
        "payload": {"status": "Registration successful"}
    }
    await websocket.send(json.dumps(ack_msg))
    return True

async def unregister_agent(websocket):
    """Unregisters an agent when they disconnect."""
    if websocket in reverse_agent_lookup:
        agent_id = reverse_agent_lookup[websocket]
        del connected_agents[agent_id]
        del reverse_agent_lookup[websocket]
        print(f"Agent '{agent_id}' unregistered from {websocket.remote_address}")
        # Optionally, notify other agents or save status

async def route_message(websocket, message_data):
    """Routes a message to the intended recipient."""
    sender_aid = reverse_agent_lookup.get(websocket)
    if not sender_aid:
        # This case should ideally not be hit if register flow is enforced strictly before any other message.
        # However, if a message somehow bypasses that (e.g. due to a logic flaw or unexpected client behavior)
        print(f"Warning: Message received from websocket {websocket.remote_address} which is not fully registered or lookup failed.")
        # We might try to parse it if it's a register message, otherwise reject.
        try:
            msg_peek = json.loads(message_data)
            if msg_peek.get("type") != "register":
                print(f"Rejecting non-register message from unknown websocket {websocket.remote_address}")
                return
        except json.JSONDecodeError:
            print(f"Invalid JSON from unknown websocket {websocket.remote_address}")
            return


    try:
        msg = json.loads(message_data)
        msg_type = msg.get("type")

        # This part is now primarily handled by the initial message check in `handler`
        # if msg_type == "register":
        #     agent_id_to_register = msg.get("payload", {}).get("aid")
        #     if agent_id_to_register:
        #         await register_agent(websocket, agent_id_to_register)
        #     else: # ... error handling ...
        #     return

        # Ensure sender is registered for any message type that isn't registration itself.
        # The main handler already ensures registration happens first.
        # This sender_aid is critical for routing and security.
        if not sender_aid: # Should have been set during registration.
             error_msg = {
                "message_id": str(uuid.uuid4()),
                "type": "error",
                "payload": {"reason": "Action attempted by a non-registered or unidentified connection."}
             }
             await websocket.send(json.dumps(error_msg))
             print(f"Error: Message from non-registered agent or missing sender_aid for websocket {websocket.remote_address}")
             return


        if msg_type == "direct_message":
            receiver_aid = msg.get("receiver_aid")
            if not receiver_aid:
                error_response = {
                    "message_id": str(uuid.uuid4()),
                    "type": "error",
                    "original_message_id": msg.get("message_id"),
                    "payload": {"reason": "Missing 'receiver_aid' in direct_message."}
                }
                await websocket.send(json.dumps(error_response))
                return

            if msg.get("sender_aid") != sender_aid:
                error_response = {
                    "message_id": str(uuid.uuid4()),
                    "type": "error",
                    "original_message_id": msg.get("message_id"),
                    "payload": {"reason": f"Message sender_aid '{msg.get('sender_aid')}' does not match connection's registered AID '{sender_aid}'."}
                }
                await websocket.send(json.dumps(error_response))
                print(f"Warning: AID mismatch for {sender_aid}. Message sender_aid: {msg.get('sender_aid')}")
                return


            recipient_ws = connected_agents.get(receiver_aid)
            if recipient_ws:
                print(f"Routing message from '{sender_aid}' to '{receiver_aid}'")
                await recipient_ws.send(message_data)
            else:
                print(f"Agent '{receiver_aid}' not found or not connected.")
                error_response = {
                    "message_id": str(uuid.uuid4()),
                    "type": "error",
                    "original_message_id": msg.get("message_id"),
                    "payload": {"reason": f"Recipient '{receiver_aid}' not connected."}
                }
                await websocket.send(json.dumps(error_response))
        else:
            # Allow other message types to pass through if needed, or handle them specifically
            # For now, we are strict: only "direct_message" (and "register" handled initially)
            print(f"Unknown or unhandled message type '{msg_type}' from '{sender_aid}'. Ignoring.")
            # Optionally send an error for unknown message types:
            # error_response = { ... "reason": f"Unknown message type '{msg_type}'" ... }
            # await websocket.send(json.dumps(error_response))


    except json.JSONDecodeError:
        print(f"Error decoding JSON from {sender_aid if sender_aid else websocket.remote_address}")
        try:
            error_msg = {"type": "error", "payload": {"reason": "Invalid JSON format."}}
            await websocket.send(json.dumps(error_msg))
        except websockets.exceptions.ConnectionClosed:
            pass
    except Exception as e:
        print(f"An error occurred processing message from {sender_aid if sender_aid else websocket.remote_address}: {e}")
        try:
            error_msg = {"type": "error", "payload": {"reason": f"Server error: {str(e)}"}}
            await websocket.send(json.dumps(error_msg))
        except websockets.exceptions.ConnectionClosed:
            pass


async def handler(websocket, path):
    """Handles incoming WebSocket connections."""
    agent_id_for_this_connection = None
    try:
        initial_message_data = await websocket.recv()
        initial_msg = json.loads(initial_message_data)

        if initial_msg.get("type") == "register":
            agent_id_to_register = initial_msg.get("payload", {}).get("aid")
            if agent_id_to_register:
                is_registered = await register_agent(websocket, agent_id_to_register)
                if not is_registered:
                    print(f"Registration failed for {websocket.remote_address}, closing connection.")
                    await websocket.close()
                    return
                agent_id_for_this_connection = agent_id_to_register # Keep track for logging/cleanup
            else:
                error_msg = {"message_id": str(uuid.uuid4()), "type": "error", "payload": {"reason": "Initial message must be 'register' type with 'aid' in payload."}}
                await websocket.send(json.dumps(error_msg))
                await websocket.close()
                print(f"Invalid initial registration (no AID) from {websocket.remote_address}, closing connection.")
                return
        else:
            error_msg = {"message_id": str(uuid.uuid4()), "type": "error", "payload": {"reason": "First message must be of type 'register'."}}
            await websocket.send(json.dumps(error_msg))
            await websocket.close()
            print(f"First message not 'register' from {websocket.remote_address}, closing connection.")
            return

        async for message_data in websocket:
            await route_message(websocket, message_data)

    except websockets.exceptions.ConnectionClosedOK:
        print(f"Connection closed normally from {agent_id_for_this_connection if agent_id_for_this_connection else websocket.remote_address}.")
    except websockets.exceptions.ConnectionClosedError as e:
        print(f"Connection closed with error from {agent_id_for_this_connection if agent_id_for_this_connection else websocket.remote_address}: {e}")
    except json.JSONDecodeError:
        # This would catch JSON errors in the *initial* registration message.
        # Errors in subsequent messages are handled in route_message.
        print(f"Invalid JSON in initial message from {websocket.remote_address}. Closing connection.")
    except Exception as e:
        print(f"An unexpected error occurred with {agent_id_for_this_connection if agent_id_for_this_connection else websocket.remote_address}: {e}")
    finally:
        # unregister_agent uses reverse_agent_lookup which is populated by register_agent
        # So, this will only do work if registration was successful.
        await unregister_agent(websocket)


async def main():
    host = "localhost"
    port = 8765
    async with websockets.serve(handler, host, port):
        print(f"MCP Server started on ws://{host}:{port}")
        await asyncio.Future()  # Run forever

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Server shutting down...")
    except Exception as e:
        print(f"Failed to start server: {e}")
