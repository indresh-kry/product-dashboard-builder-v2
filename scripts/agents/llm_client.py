#!/usr/bin/env python3
"""
Unified LLM Client
Version: 2.0.0
Last Updated: 2025-10-23

Unified client for calling LLM services.
Handles API calls, error handling, and response parsing.
"""

import os
import json
import sys
from typing import Dict, Any, Optional
try:
    import httpx  # for OpenAI v1 http_client
except Exception:
    httpx = None
from datetime import datetime

try:
    import openai
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    print("⚠️ Warning: OpenAI library not available. Install with: pip install openai", file=sys.stderr)

class LLMClient:
    """Unified client for LLM API calls."""
    
    def __init__(self, api_key: Optional[str] = None, model: str = "gpt-4", proxies: Optional[Dict[str, str]] = None):
        print(f"🔑 Initializing LLM Client...", file=sys.stderr)
        self.api_key = api_key or self._get_api_key()
        self.model = model
        self.proxies = proxies
        self.client = None
        
        # Lazy import openai if it wasn't available at module import time
        global OPENAI_AVAILABLE, openai
        if not OPENAI_AVAILABLE:
            try:
                import openai as _openai
                openai = _openai  # set global reference
                OPENAI_AVAILABLE = True
            except Exception as e:
                print(f"❌ OpenAI import failed: {e}", file=sys.stderr)

        if OPENAI_AVAILABLE and self.api_key:
            # Initialize using OpenAI v1 client with optional httpx client for proxies
            # Do NOT pass 'proxies' kwarg directly to OpenAI client
            os.environ['OPENAI_API_KEY'] = self.api_key
            for proxy_var in ['HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'http_proxy', 'https_proxy', 'all_proxy']:
                # Honor explicit proxies only via httpx client; avoid env proxies
                if proxy_var in os.environ:
                    del os.environ[proxy_var]
            try:
                http_client = self._make_http_client()
                if http_client is not None:
                    self.client = openai.OpenAI(api_key=self.api_key, http_client=http_client)
                else:
                    self.client = openai.OpenAI(api_key=self.api_key)
                print(f"✅ LLM Client initialized successfully", file=sys.stderr)
            except Exception as e:
                print(f"❌ LLM Client init error: {e}", file=sys.stderr)
        else:
            print(f"❌ LLM Client initialization failed - API key: {self.api_key[:10] if self.api_key else 'None'}...", file=sys.stderr)

    def _make_http_client(self):
        """For OpenAI v1: build an httpx.Client with proxies if provided. Return None if not available."""
        if not self.proxies or not httpx:
            return None
        try:
            return httpx.Client(proxies=self.proxies, timeout=60.0)
        except Exception:
            return None
    
    def _get_api_key(self) -> str:
        """Get API key from environment or creds.json."""
        api_key = os.environ.get('OPENAI_API_KEY')
        print(f"🔍 Environment OPENAI_API_KEY: {api_key[:10] if api_key else 'None'}...", file=sys.stderr)
        
        if not api_key or api_key == 'placeholder_openai_key' or api_key.startswith('your_openai'):
            # Try to load from creds.json
            try:
                # Try multiple possible paths for creds.json
                creds_paths = [
                    'creds.json',  # Current directory
                    '../creds.json',  # Project root
                    '../../creds.json',  # Two levels up
                    '../../../creds.json',  # Three levels up
                    '/Users/indresh/GR-Repo-Local/product-dashboard-builder-AF/creds.json',  # Absolute path to AF repo root
                    '/Users/indresh/GR-Repo-Local/product-dashboard-builder-v2/creds.json'  # Legacy absolute path
                ]
                print(f"🔍 Searching for creds.json in paths: {creds_paths}", file=sys.stderr)
                for creds_path in creds_paths:
                    print(f"🔍 Checking path: {creds_path} - exists: {os.path.exists(creds_path)}", file=sys.stderr)
                    if os.path.exists(creds_path):
                        try:
                            with open(creds_path, 'r') as f:
                                creds = json.load(f)
                                # Support multiple key names
                                api_key = creds.get('openai_api_key') or creds.get('OPENAI_API_KEY')
                                print(f"🔍 Found API key in {creds_path}: {api_key[:10] if api_key else 'None'}...", file=sys.stderr)
                                if api_key and api_key != 'placeholder_openai_key' and not str(api_key).startswith('your_openai'):
                                    print(f"✅ Found OpenAI API key in {creds_path}", file=sys.stderr)
                                    break
                        except Exception as e:
                            # Skip unreadable/malformed creds files and continue searching
                            print(f"⚠️ Error loading creds.json at {creds_path}: {e}", file=sys.stderr)
            except Exception as e:
                print(f"⚠️ Error loading creds.json: {e}", file=sys.stderr)
        
        if not api_key or api_key == 'placeholder_openai_key' or api_key.startswith('your_openai'):
            raise ValueError("OpenAI API key not found. Please set OPENAI_API_KEY environment variable or add to creds.json")
        
        return api_key
    
    def call(self, prompt: str, system_prompt: str, temperature: float = 0.3, max_tokens: int = 1000) -> Dict[str, Any]:
        """Call the LLM API with the given prompt."""
        if not self.client:
            return {
                'error': 'OpenAI client not available',
                'raw_response': None,
                'parsed_response': None
            }
        
        try:
            # Use the v1+ SDK completions API
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=temperature,
                max_tokens=max_tokens
            )
            content = response.choices[0].message.content
            
            # Try to parse JSON response
            try:
                parsed_response = json.loads(content)
                return {
                    'raw_response': content,
                    'parsed_response': parsed_response,
                    'success': True
                }
            except json.JSONDecodeError:
                # Try to extract JSON from markdown code blocks
                import re
                json_match = re.search(r'```json\s*(\{.*?\})\s*```', content, re.DOTALL)
                if json_match:
                    try:
                        parsed_response = json.loads(json_match.group(1))
                        return {
                            'raw_response': content,
                            'parsed_response': parsed_response,
                            'success': True
                        }
                    except json.JSONDecodeError:
                        pass
                
                # Try to find JSON object in the text
                json_match = re.search(r'\{.*\}', content, re.DOTALL)
                if json_match:
                    try:
                        parsed_response = json.loads(json_match.group(0))
                        return {
                            'raw_response': content,
                            'parsed_response': parsed_response,
                            'success': True
                        }
                    except json.JSONDecodeError:
                        pass
                
                # If all parsing fails, return error structure
                return {
                    'raw_response': content,
                    'parsed_response': None,
                    'success': False,
                    'error': 'Response was not in expected JSON format'
                }
                
        except Exception as e:
            return {
                'raw_response': None,
                'parsed_response': None,
                'success': False,
                'error': str(e)
            }
    
    def is_available(self) -> bool:
        """Check if the LLM client is available."""
        return self.client is not None
