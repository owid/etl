#!/usr/bin/env python3
"""
Simple test script for the MCP agent from apps/wizard/app_pages/expert_agent/agent.py
Tests the mcp_server_prod connection and basic functionality.
"""

import asyncio
from pydantic_ai import Agent
from pydantic_ai.mcp import MCPServerStreamableHTTP


async def test_mcp_agent():
    """Test the MCP agent with a simple query."""
    
    # Create MCP server connection (same as in agent.py)
    mcp_server_prod = MCPServerStreamableHTTP(
        url="https://mcp.owid.io/mcp",
    )
    
    # Create simple agent with MCP toolset
    agent = Agent(
        instructions="You are a helpful assistant with access to Our World in Data tools.",
        toolsets=[mcp_server_prod],
    )
    
    # Test query
    test_prompt = "What data do you have about global population?"
    
    print(f"Testing MCP agent with query: '{test_prompt}'")
    print("-" * 50)
    
    try:
        # Run the agent
        result = await agent.run(test_prompt)
        print("Response:")
        print(result.data)
        
        if hasattr(result, 'usage'):
            print(f"\nUsage: {result.usage()}")
            
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e)}")


if __name__ == "__main__":
    asyncio.run(test_mcp_agent())