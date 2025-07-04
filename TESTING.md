# MCP (Multi-Agent Communication Platform) Testing Guide

## 1. Overview

This document provides instructions on how to install, deploy, and test the initial phase of the Multi-Agent Communication Platform (MCP). This version focuses on core direct messaging capabilities between distributed agents.

The system consists of:
- `mcp_server.py`: The central server that handles agent connections, registration, and message routing.
- `mcp_agent.py`: A client module that allows agents to connect to the server, send, and receive messages.
- `test_mcp_communication.py`: Automated tests for verifying core functionalities.

## 2. Prerequisites

- Python 3.8 or newer.
- `pip` (Python package installer).

## 3. Installation/Setup

1.  **Obtain the Files**:
    Ensure you have the following files in your working directory:
    - `mcp_server.py`
    - `mcp_agent.py`
    - `test_mcp_communication.py`

2.  **Install Dependencies**:
    The MCP system relies on the `websockets` library. Install it using pip:
    ```bash
    pip install websockets
    ```

## 4. Running the MCP Server

The MCP server is the central hub for agent communication.

1.  **Start the Server**:
    Open a terminal and navigate to the directory containing `mcp_server.py`. Run the following command:
    ```bash
    python mcp_server.py
    ```

2.  **Expected Output**:
    On successful startup, you should see a message similar to:
    ```
    MCP Server started on ws://localhost:8765
    ```
    The server will then listen for incoming agent connections and log activities such as registrations and message routing. Keep this terminal window open while running agents or tests.

## 5. Running MCP Agents (Manual Testing/Demonstration)

You can run multiple agent instances to test communication manually. Each agent needs a unique ID.

1.  **Configuration**:
    Agent ID and Server URI can be configured via:
    *   **Command-line arguments**: `python mcp_agent.py <AGENT_ID> [SERVER_URI]`
        *   Example: `python mcp_agent.py Alice` (uses default server URI `ws://localhost:8765`)
        *   Example: `python mcp_agent.py Bob ws://192.168.1.10:8765` (connects to a specific server)
    *   **Environment variables**:
        *   `MCP_AGENT_ID`: Sets the agent's ID.
        *   `MCP_SERVER_URI`: Sets the server URI.
        *   Example (bash/zsh):
            ```bash
            MCP_AGENT_ID=Alice python mcp_agent.py
            MCP_AGENT_ID=Bob MCP_SERVER_URI=ws://localhost:8765 python mcp_agent.py
            ```

2.  **Running Two Agents for Communication**:

    *   **Terminal 1 (Start Agent Alice)**:
        ```bash
        python mcp_agent.py Alice
        ```
        Alice will connect, register, and then attempt to send messages to "Bob" and "Charlie". She will then listen for responses or incoming messages.

    *   **Terminal 2 (Start Agent Bob)**:
        ```bash
        python mcp_agent.py Bob
        ```
        Bob will connect, register, and listen for messages. If Alice is running and sends her message to Bob, Bob's terminal should display the received message.

    Observe the server terminal output for connection and routing logs. Observe Alice's terminal for send confirmations and any error messages (e.g., if trying to message an offline agent). Observe Bob's terminal for received messages.

## 6. Running Automated Tests

Automated tests verify core functionalities programmatically.

1.  **Prerequisite**:
    **The MCP Server (`mcp_server.py`) must be running** (as described in Section 4) before you execute the automated tests. The tests connect to this live server instance.

2.  **Execute Tests**:
    Open a new terminal (while the server is running in another) and navigate to the directory containing the files. Run the following command:
    ```bash
    python -m unittest test_mcp_communication.py
    ```

3.  **Expected Output**:
    If all tests pass, you will see output similar to:
    ```
    ...
    ----------------------------------------------------------------------
    Ran 3 tests in Xs

    OK
    ```
    This indicates that the tested functionalities are working as expected. If any tests fail, the output will provide details about the failure.

## 7. Testing Objectives and Scenarios Covered

These are the primary goals this testing phase aims to achieve, largely covered by the automated tests.

### Objective 1: Successful Agent Registration
*   **Description**: Verify that agents can successfully connect to the server and register with a unique Agent ID.
*   **Scenario**: An agent attempts to connect and register.
*   **Verification**:
    *   Server logs the registration of the agent.
    *   Agent receives a `register_ack` message from the server (this is implicitly tested by the `asyncSetUp` in `test_mcp_communication.py` where `wait_for_registration` must succeed).
    *   The agent is marked as `is_registered = True`.

### Objective 2: Rejection of Duplicate Agent ID Registration
*   **Description**: Ensure the server prevents multiple agents from registering with the same Agent ID.
*   **Scenario**: Agent A registers with "ID1". Agent B then attempts to register with "ID1".
*   **Verification**:
    *   Agent B receives a `register_nack` (negative acknowledgment) message from the server, indicating the ID is taken.
    *   Agent B is not considered registered by the server.
    *   Server logs the failed registration attempt.
    *   *Covered by `test_03_agent_registration_failure_duplicate_id`.*

### Objective 3: Successful Direct Message Transmission
*   **Description**: Confirm that a registered agent can successfully send a direct message to another registered and connected agent.
*   **Scenario**: Agent A sends a message to Agent B. Both are registered and connected.
*   **Verification**:
    *   Agent B receives the message.
    *   The `sender_aid`, `receiver_aid`, and `payload` of the received message match what Agent A sent.
    *   Server logs the message routing.
    *   *Covered by `test_01_send_and_receive_direct_message`.*

### Objective 4: Server Handles Messages to Non-Connected/Non-Existent Agents
*   **Description**: Verify that the server provides appropriate feedback to a sender if the intended recipient agent is not connected or does not exist.
*   **Scenario**: Agent A sends a message to Agent C, but Agent C is not currently connected or registered.
*   **Verification**:
    *   Agent A (the sender) receives an `error` message from the server.
    *   The error message indicates that the recipient was not found or is not connected.
    *   Server logs the failed routing attempt.
    *   *Covered by `test_02_message_to_nonexistent_agent`.*

### Objective 5: Basic Agent Configuration
*   **Description**: Ensure agents can be configured with specific Agent IDs and can target specific server URIs.
*   **Scenario**:
    1. Run an agent specifying an ID via command-line argument.
    2. Run an agent specifying an ID via the `MCP_AGENT_ID` environment variable.
    3. Run an agent specifying a non-default server URI.
*   **Verification**:
    *   Manual observation: The agent uses the specified ID in its logs and when sending messages (check server logs for registration with the correct ID).
    *   Manual observation: The agent attempts to connect to the specified server URI.

## 8. Basic Troubleshooting

*   **Connection Refused (when running agent or tests)**:
    *   **Cause**: The `mcp_server.py` is likely not running, or not accessible at the URI the agent is trying to connect to (default `ws://localhost:8765`).
    *   **Solution**: Ensure the server is started and there are no firewall issues. Verify the server URI configuration for the agent.

*   **Address already in use (when starting `mcp_server.py`)**:
    *   **Cause**: Another process is already using port 8765 on localhost. This could be another instance of `mcp_server.py` or a different application.
    *   **Solution**: Stop the conflicting process or configure `mcp_server.py` to use a different port (requires code change in this version).

*   **Tests Fail**:
    *   **Cause**: Could be due to the server not running, issues in the code, or network problems.
    *   **Solution**: Check the server status first. Review the specific test failure messages for clues. Ensure the environment is set up correctly.
```
