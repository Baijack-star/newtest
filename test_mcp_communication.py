import asyncio
import unittest
import json # Not strictly needed for test logic but good for context
from mcp_agent import MCPAgent
# Assuming mcp_server can be imported if we want to run it in-process,
# but for this test, we'll assume it's running separately.
# import mcp_server # Or use subprocess to start it

# --- Configuration for the test ---
# Make sure MCP Server is running at this URI before starting tests.
# Typically, you'd run `python mcp_server.py` in a separate terminal.
TEST_SERVER_URI = "ws://localhost:8765"
TEST_TIMEOUT = 10  # seconds for async operations

class TestMCPCommunication(unittest.IsolatedAsyncioTestCase):

    async def asyncSetUp(self):
        """Called before every test via `self.IsolatedAsyncioTestCase_await_pending()`."""
        # Note: Server needs to be running independently.
        # In a more complex setup, we might start/stop the server here.
        # For now, we assume it's up.
        self.agent1 = MCPAgent(agent_id="TestAgent1", server_uri=TEST_SERVER_URI)
        self.agent2 = MCPAgent(agent_id="TestAgent2", server_uri=TEST_SERVER_URI)

        # Suppress agent's print output during tests to keep test output clean
        # This is a bit of a hack; proper logging configuration would be better.
        # Or, capture stdout/stderr if needed.
        self.agent1_print_backup = self.agent1.__class__._handle_direct_message # type: ignore
        self.agent2_print_backup = self.agent2.__class__._handle_direct_message # type: ignore

        async def quiet_handle_direct_message(agent_instance, message):
            # Call original logic if necessary, or just queue it
            # For this test, we only care about the queue
            await agent_instance.received_messages_queue.put(message)
            # print(f"[{agent_instance.agent_id} - TestQuiet]: Queued DM: {message.get('payload')}")


        self.agent1.__class__._handle_direct_message = quiet_handle_direct_message # type: ignore
        self.agent2.__class__._handle_direct_message = quiet_handle_direct_message # type: ignore


        await self.agent1.connect()
        await self.agent2.connect()

        # Wait for registration to complete for both agents
        self.assertTrue(await self.agent1.wait_for_registration(timeout=TEST_TIMEOUT / 2), "Agent1 failed to register")
        self.assertTrue(await self.agent2.wait_for_registration(timeout=TEST_TIMEOUT / 2), "Agent2 failed to register")


    async def asyncTearDown(self):
        """Called after every test."""
        await self.agent1.close()
        await self.agent2.close()

        # Restore print handlers if they were changed for specific agent instances
        # If changed on class, be careful with parallel tests (not an issue with IsolatedAsyncioTestCase)
        self.agent1.__class__._handle_direct_message = self.agent1_print_backup # type: ignore
        self.agent2.__class__._handle_direct_message = self.agent2_print_backup # type: ignore
        await asyncio.sleep(0.1) # Give connections time to close gracefully


    async def test_01_send_and_receive_direct_message(self):
        """TestAgent1 sends a message to TestAgent2."""
        message_content = {"type": "greeting", "text": "Hello TestAgent2 from TestAgent1!"}

        success = await self.agent1.send_direct_message(recipient_aid="TestAgent2", content_payload=message_content)
        self.assertTrue(success, "Agent1 failed to send message")

        try:
            received_msg = await asyncio.wait_for(self.agent2.received_messages_queue.get(), timeout=TEST_TIMEOUT)
        except asyncio.TimeoutError:
            self.fail("Agent2 did not receive message within timeout")

        self.assertIsNotNone(received_msg, "Agent2 received None message")
        self.assertEqual(received_msg.get("sender_aid"), "TestAgent1")
        self.assertEqual(received_msg.get("receiver_aid"), "TestAgent2") # Server doesn't modify this, good check
        self.assertEqual(received_msg.get("type"), "direct_message") # This is the envelope type
        self.assertEqual(received_msg.get("payload"), message_content)
        self.agent2.received_messages_queue.task_done()


    async def test_02_message_to_nonexistent_agent(self):
        """TestAgent1 sends a message to a non-existent agent and expects an error."""
        non_existent_agent_id = "GhostAgent123"
        message_content = {"text": "Hello, anyone there?"}

        # Temporarily hijack agent1's error handler to check for the specific error
        error_received_queue = asyncio.Queue()
        original_error_handler = self.agent1.message_handlers["error"]
        async def custom_error_handler(message):
            # print(f"[{self.agent1.agent_id} - TestError]: Received error: {message}")
            await error_received_queue.put(message)
            # await original_error_handler(message) # Call original if needed

        self.agent1.message_handlers["error"] = custom_error_handler

        success = await self.agent1.send_direct_message(recipient_aid=non_existent_agent_id, content_payload=message_content)
        self.assertTrue(success, "Agent1 failed to send message to non-existent agent")

        try:
            error_msg = await asyncio.wait_for(error_received_queue.get(), timeout=TEST_TIMEOUT)
        except asyncio.TimeoutError:
            self.fail("Agent1 did not receive error message for non-existent recipient within timeout")
        finally:
            # Restore original error handler
            self.agent1.message_handlers["error"] = original_error_handler


        self.assertIsNotNone(error_msg)
        self.assertEqual(error_msg.get("type"), "error")
        self.assertIn(non_existent_agent_id, error_msg.get("payload", {}).get("reason", ""))
        self.assertIn("not connected", error_msg.get("payload", {}).get("reason", "").lower())
        error_received_queue.task_done()


    async def test_03_agent_registration_failure_duplicate_id(self):
        """Test that a third agent trying to register with TestAgent1's ID fails."""
        # TestAgent1 is already registered with "TestAgent1"
        agent3 = MCPAgent(agent_id="TestAgent1", server_uri=TEST_SERVER_URI) # Same ID as agent1

        # Capture agent3's registration nack
        nack_received_queue = asyncio.Queue()
        original_nack_handler = agent3.message_handlers["register_nack"]
        async def custom_nack_handler(message):
            # print(f"[{agent3.agent_id} - TestNack]: Received nack: {message}")
            await nack_received_queue.put(message)
            # await original_nack_handler(message) # Call original if needed
        agent3.message_handlers["register_nack"] = custom_nack_handler

        await agent3.connect() # This will attempt registration and start listening

        try:
            nack_msg = await asyncio.wait_for(nack_received_queue.get(), timeout=TEST_TIMEOUT)
        except asyncio.TimeoutError:
            self.fail("Agent3 did not receive registration_nack for duplicate ID within timeout")
        finally:
            agent3.message_handlers["register_nack"] = original_nack_handler # Restore
            await agent3.close() # Ensure agent3 is closed

        self.assertIsNotNone(nack_msg)
        self.assertEqual(nack_msg.get("type"), "register_nack")
        self.assertIn("TestAgent1", nack_msg.get("payload", {}).get("reason", ""))
        self.assertIn("taken", nack_msg.get("payload", {}).get("reason", "").lower())

        self.assertFalse(agent3.is_registered, "Agent3 should not be marked as registered after nack")
        nack_received_queue.task_done()

# To run these tests:
# 1. Ensure the MCP Server is running: `python mcp_server.py`
# 2. In another terminal, run: `python -m unittest test_mcp_communication.py`
#
# Note on test names: unittest runs tests in alphabetical order by default.
# Numbering them (test_01_, test_02_) can help control order if needed,
# though tests should ideally be independent. IsolatedAsyncioTestCase helps with this.

if __name__ == '__main__':
    print("Reminder: Ensure the MCP Server (mcp_server.py) is running on " + TEST_SERVER_URI + " before starting tests.")
    print("Run tests with: python -m unittest test_mcp_communication.py")
    # unittest.main() # This would run if script is executed directly, but recommend `python -m unittest`
    # For direct execution with asyncio, it's a bit more involved to set up the event loop runner correctly.
    # The `python -m unittest` command handles this for `IsolatedAsyncioTestCase`.
    # Example of how you might run it directly if needed (less common for unittest):
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(unittest.main(argv=['first-arg-is-ignored'], exit=False))
    # loop.close()
