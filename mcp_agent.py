import asyncio
import json
import websockets
import uuid
import os # For environment variables
from datetime import datetime, timezone

DEFAULT_SERVER_URI = "ws://localhost:8765"

class MCPAgent:
    def __init__(self, agent_id=None, server_uri=None):
        # Configuration priority:
        # 1. Direct constructor arguments
        # 2. Environment variables
        # 3. Hardcoded defaults (for server_uri) / or raise error (for agent_id)

        self.agent_id = agent_id if agent_id is not None else os.environ.get("MCP_AGENT_ID")
        self.server_uri = server_uri if server_uri is not None else os.environ.get("MCP_SERVER_URI", DEFAULT_SERVER_URI)

        if not self.agent_id:
            raise ValueError("Agent ID must be provided either as a constructor argument or via MCP_AGENT_ID environment variable.")

        self.websocket = None
        self.is_registered = False
        self.message_handlers = {
            "direct_message": self._handle_direct_message,
            "error": self._handle_error_message,
            "register_ack": self._handle_register_ack,
            "register_nack": self._handle_register_nack,
        }
        self._listener_task = None
        self.received_messages_queue = asyncio.Queue() # For testing and external consumption


    async def _handle_direct_message(self, message):
        sender = message.get("sender_aid")
        payload = message.get("payload")
        timestamp = message.get("timestamp")
        print(f"[{self.agent_id} - Received DM from {sender} at {timestamp}]: {payload}")
        # Put the full message onto the queue for test inspection or other processing
        await self.received_messages_queue.put(message)

    async def _handle_error_message(self, message):
        reason = message.get("payload", {}).get("reason")
        original_msg_id = message.get("payload", {}).get("original_message_id")
        print(f"[{self.agent_id} - Server Error]: {reason} (Original Msg ID: {original_msg_id})")

    async def _handle_register_ack(self, message):
        print(f"[{self.agent_id}]: Successfully registered with server.")
        self.is_registered = True

    async def _handle_register_nack(self, message):
        reason = message.get("payload", {}).get("reason")
        print(f"[{self.agent_id}]: Registration failed: {reason}. Exiting or will retry if implemented.")
        self.is_registered = False
        if self.websocket and not self.websocket.closed:
            await self.websocket.close() # Close connection on registration failure

    async def connect(self):
        if self.websocket and not self.websocket.closed:
            print(f"[{self.agent_id}]: Already connected or connecting.")
            return

        try:
            self.websocket = await websockets.connect(self.server_uri)
            print(f"[{self.agent_id}]: Connected to MCP server at {self.server_uri}")

            registration_message = {
                "message_id": str(uuid.uuid4()),
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "type": "register",
                "payload": {"aid": self.agent_id}
            }
            await self.websocket.send(json.dumps(registration_message))
            print(f"[{self.agent_id}]: Sent registration request.")

            # Start listening immediately after sending registration
            # The first message received should be register_ack or register_nack
            if self._listener_task is None or self._listener_task.done():
                self._listener_task = asyncio.create_task(self.listen())

        except ConnectionRefusedError:
            print(f"[{self.agent_id}]: Connection refused. Is the MCP server running at {self.server_uri}?")
            self.websocket = None
        except websockets.exceptions.InvalidURI:
            print(f"[{self.agent_id}]: Invalid server URI: {self.server_uri}")
            self.websocket = None
        except Exception as e:
            print(f"[{self.agent_id}]: Error connecting or registering: {e}")
            self.websocket = None # Ensure websocket is None if connection failed
            if self.websocket and not self.websocket.closed: # Should be redundant
                 await self.websocket.close()


    async def listen(self):
        if not self.websocket: # Check if websocket object exists
            print(f"[{self.agent_id}]: Websocket is None, cannot listen.")
            return
        if self.websocket.closed:
            print(f"[{self.agent_id}]: Connection is closed, cannot listen.")
            return

        try:
            async for message_str in self.websocket:
                try:
                    message = json.loads(message_str)
                    msg_type = message.get("type")

                    handler = self.message_handlers.get(msg_type)
                    if handler:
                        await handler(message)
                    else:
                        print(f"[{self.agent_id}]: Received unhandled message type '{msg_type}': {message}")

                except json.JSONDecodeError:
                    print(f"[{self.agent_id}]: Error decoding JSON from server: {message_str}")
                except Exception as e:
                    print(f"[{self.agent_id}]: Error processing message: {e} - Message: {message_str}")

        except websockets.exceptions.ConnectionClosedOK:
            print(f"[{self.agent_id}]: Connection to server closed normally by server.")
        except websockets.exceptions.ConnectionClosedError as e:
            print(f"[{self.agent_id}]: Connection to server closed with error: {e}")
        except Exception as e:
            # Catch-all for other unexpected errors during listen
            print(f"[{self.agent_id}]: Unexpected error in listener: {e}")
        finally:
            self.is_registered = False
            print(f"[{self.agent_id}]: Listener stopped.")
            # Consider attempting to reconnect here or signaling the agent's main logic.


    async def send_direct_message(self, recipient_aid, content_payload):
        if not self.websocket or self.websocket.closed:
            print(f"[{self.agent_id}]: Not connected. Cannot send message.")
            return False
        if not self.is_registered:
            # It might take a moment for is_registered to be True after connect()
            # Polling for self.is_registered here can be problematic if called too soon.
            # A better approach is to ensure connect() and registration ack are fully processed.
            print(f"[{self.agent_id}]: Not registered or registration pending. Cannot send message.")
            return False

        message = {
            "message_id": str(uuid.uuid4()),
            "sender_aid": self.agent_id,
            "receiver_aid": recipient_aid,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "type": "direct_message",
            "payload": content_payload
        }
        try:
            await self.websocket.send(json.dumps(message))
            # print(f"[{self.agent_id} -> {recipient_aid}]: Sent {content_payload}") # Optional: too verbose sometimes
            return True
        except websockets.exceptions.ConnectionClosed:
            print(f"[{self.agent_id}]: Connection closed while trying to send. Marking as not registered.")
            self.is_registered = False
            return False
        except Exception as e:
            print(f"[{self.agent_id}]: Error sending message: {e}")
            return False

    async def close(self):
        if self._listener_task and not self._listener_task.done():
            self._listener_task.cancel()
            try:
                await self._listener_task
            except asyncio.CancelledError:
                print(f"[{self.agent_id}]: Listener task cancelled successfully.")
            except Exception as e: # Should not happen often
                print(f"[{self.agent_id}]: Exception while awaiting cancelled listener task: {e}")


        if self.websocket and not self.websocket.closed:
            print(f"[{self.agent_id}]: Closing connection.")
            await self.websocket.close()
        self.is_registered = False
        print(f"[{self.agent_id}]: Connection closed and agent unregistered.")

    async def wait_for_registration(self, timeout=10):
        """Waits for the agent to be registered, with a timeout."""
        if self.is_registered:
            return True

        for _ in range(int(timeout / 0.1)): # Check every 100ms
            if self.is_registered:
                return True
            if not self.websocket or self.websocket.closed: # Connection lost or registration failed
                print(f"[{self.agent_id}]: Connection closed or registration failed while waiting.")
                return False
            await asyncio.sleep(0.1)

        print(f"[{self.agent_id}]: Timeout waiting for registration.")
        return False


async def run_agent_scenario(agent_id_arg=None, server_uri_arg=None):
    """
    Runs a single agent instance.
    AGENT_ID and MCP_SERVER_URI can be set via environment variables
    or passed as arguments to this function (which take precedence).
    """
    agent = None
    try:
        # MCPAgent constructor handles env vars if args are None
        agent = MCPAgent(agent_id=agent_id_arg, server_uri=server_uri_arg)
        print(f"--- Starting Agent: {agent.agent_id} connecting to {agent.server_uri} ---")

        await agent.connect()

        if not agent.websocket: # Connection failed in connect()
            print(f"[{agent.agent_id}]: Failed to establish initial connection. Exiting.")
            return

        if not await agent.wait_for_registration(timeout=5):
            print(f"[{agent.agent_id}]: Did not register successfully. Exiting.")
            await agent.close()
            return

        # Agent specific logic
        if agent.agent_id.lower() == "alice":
            await agent.send_direct_message("Bob", {"text": "Hello Bob, this is Alice!"})
            await asyncio.sleep(0.5)
            await agent.send_direct_message("Charlie", {"text": "Hi Charlie, are you there?"}) # Charlie might not exist
            await asyncio.sleep(2) # Wait for potential error messages or replies
            print(f"[{agent.agent_id}]: Done sending initial messages.")

        elif agent.agent_id.lower() == "bob":
            # Bob just listens. If Alice sends a message, Bob's _handle_direct_message will print it.
            print(f"[{agent.agent_id}]: Listening for messages...")
            # Bob could be programmed to reply here if a message is received.
            # For now, replies would need to be added to _handle_direct_message.

        # All agents will listen for a while
        print(f"[{agent.agent_id}]: Will listen for 10 seconds then shut down.")
        await asyncio.sleep(10)

    except ValueError as e: # For agent_id not being set
        print(f"Configuration Error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred in run_agent_scenario for {agent_id_arg if agent_id_arg else 'agent'}: {e}")
    finally:
        if agent:
            print(f"--- Shutting down Agent: {agent.agent_id} ---")
            await agent.close()
        print(f"--- Agent {agent_id_arg if agent_id_arg else 'run'} finished ---")


async def main():
    # To test:
    # 1. Start mcp_server.py
    # 2. Terminal 1: MCP_AGENT_ID=Alice python mcp_agent.py
    #    or: python mcp_agent.py Alice
    # 3. Terminal 2: MCP_AGENT_ID=Bob MCP_SERVER_URI=ws://localhost:8765 python mcp_agent.py
    #    or: python mcp_agent.py Bob ws://localhost:8765

    import sys
    agent_id_from_arg = None
    server_uri_from_arg = None

    if len(sys.argv) > 1:
        agent_id_from_arg = sys.argv[1]
    if len(sys.argv) > 2:
        server_uri_from_arg = sys.argv[2]

    if not agent_id_from_arg and not os.environ.get("MCP_AGENT_ID"):
        print("Usage: python mcp_agent.py <agent_id> [server_uri]")
        print("Or set MCP_AGENT_ID environment variable.")
        print("Defaulting to run 'Alice' and 'Bob' sequentially for demo if no args/env.")

        print("\n--- Running Agent Alice (default) ---")
        await run_agent_scenario(agent_id_arg="Alice") # server_uri will use default or MCP_SERVER_URI env

        print("\n--- Running Agent Bob (default) ---")
        await run_agent_scenario(agent_id_arg="Bob")   # server_uri will use default or MCP_SERVER_URI env

        print("\n--- Sequential demo finished. ---")
        print("For live interaction, run agents in separate terminals with unique IDs.")
    else:
        # Run a single agent based on args or environment variables
        await run_agent_scenario(agent_id_arg=agent_id_from_arg, server_uri_arg=server_uri_from_arg)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("Agent execution interrupted by user.")
    finally:
        print("mcp_agent.py script finished.")
