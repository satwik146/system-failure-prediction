#!/usr/bin/env python3
"""
test_api.py — Test IoT Failure Predictor API endpoints
"""
import requests
import json
import time

BASE_URL = "http://127.0.0.1:8000"

def test_endpoint(method, endpoint, data=None, name=None):
    """Test a single endpoint."""
    url = f"{BASE_URL}{endpoint}"
    name = name or endpoint
    try:
        if method == "GET":
            resp = requests.get(url, timeout=5)
        elif method == "POST":
            resp = requests.post(url, json=data, timeout=5)
        
        print(f"\n✅ {method} {name}")
        print(f"   Status: {resp.status_code}")
        try:
            body = resp.json()
            print(f"   Response: {json.dumps(body, indent=2)[:200]}...")
        except:
            print(f"   Response: {resp.text[:200]}")
        return resp.status_code == 200
    except Exception as e:
        print(f"\n❌ {method} {name}")
        print(f"   Error: {e}")
        return False

def main():
    print("=" * 60)
    print("IoT Failure Predictor API Test Suite")
    print("=" * 60)
    
    results = {}
    
    # Test 1: Health endpoint
    results["health"] = test_endpoint("GET", "/health", name="Health Check")
    
    # Test 2: Root endpoint
    results["root"] = test_endpoint("GET", "/", name="Root (Dashboard)")
    
    # Test 3: Status endpoint
    results["status"] = test_endpoint("GET", "/api/status", name="System Status")
    
    # Test 4: Events endpoint
    results["events"] = test_endpoint("GET", "/api/events?limit=10", name="Recent Events")
    
    # Test 5: Predictions endpoint
    results["predictions"] = test_endpoint("GET", "/api/predictions", name="Latest Predictions")
    
    # Test 6: Reports endpoint
    results["reports"] = test_endpoint("GET", "/api/reports", name="Historical Reports")
    
    # Test 7: Simulator scenarios
    results["scenarios"] = test_endpoint("GET", "/api/simulator/scenarios", name="Available Scenarios")
    
    # Test 8: Digital twin snapshot
    results["twin"] = test_endpoint("GET", "/api/twin", name="Digital Twin State")
    
    # Test 9: Inject fault
    fault_data = {
        "source_id": "pump_motor",
        "tag": "temperature",
        "fault_type": "spike",
        "magnitude": 50.0
    }
    results["inject"] = test_endpoint("POST", "/api/simulator/inject", data=fault_data, name="Inject Fault")
    
    # Summary
    print("\n" + "=" * 60)
    print("Test Summary")
    print("=" * 60)
    passed = sum(1 for v in results.values() if v)
    total = len(results)
    print(f"Passed: {passed}/{total}")
    for endpoint, passed in results.items():
        status = "✅" if passed else "❌"
        print(f"  {status} {endpoint}")

if __name__ == "__main__":
    main()
