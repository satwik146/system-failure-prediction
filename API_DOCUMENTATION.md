# IoT Failure Predictor - API Endpoints Documentation

## Base URL
```
http://127.0.0.1:8000
```

## Endpoints Overview

### Health & Status
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/health` | GET | Health check endpoint |
| `/api/status` | GET | System status including health score and predictions |

### Data & Telemetry
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/events` | GET | Retrieve recent events from ingestion system |
| `/api/predictions` | GET | Get latest failure predictions |
| `/api/reports` | GET | Retrieve historical reports |

### Simulator & Testing
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/twin` | GET | Get digital twin snapshot (current system state) |
| `/api/simulator/scenarios` | GET | List available fault scenarios |
| `/api/simulator/run/{name}` | POST | Run a named scenario |
| `/api/simulator/inject` | POST | Inject a fault into the simulator |
| `/api/simulator/cancel` | POST | Cancel all active injections |

### Real-time Updates
| Endpoint | Method | Description |
|----------|--------|-------------|
| `/ws` | WebSocket | Real-time push updates (2s interval) |

---

## Endpoint Details

### 1. Health Check
**GET** `/health`

Check if the API is responsive.

**Response:**
```json
{
  "status": "ok",
  "time": 1777637243.1331563
}
```

**Status Code:** 200

---

### 2. System Status
**GET** `/api/status`

Get comprehensive system status including:
- Event bus statistics
- System health score (0-100)
- Latest predictions from all sources
- Simulator state

**Response:**
```json
{
  "bus": {},
  "system_health": 100.0,
  "predictions": {
    "source_id_1": {
      "health_score": 95.5,
      "is_anomaly": false,
      "anomaly_reason": "",
      "current_value": 42.3,
      "forecast": [40.2, 38.9, 37.5, 36.1, 35.0],
      "minutes_to_event": null,
      "timestamp": 1777637000.0,
      "buffer_fill": 0.85
    }
  },
  "simulator": {
    "twin_sources": 1,
    "active_injections": 0,
    "scenarios_available": ["motor_overheat", "sensor_drift", "memory_leak"]
  }
}
```

**Status Code:** 200

---

### 3. Recent Events
**GET** `/api/events?source_id=pump_motor&limit=100`

Retrieve recent events from the ingestion system.

**Query Parameters:**
- `source_id` (optional): Filter by source ID
- `limit` (optional, default=100): Maximum number of events to return

**Response:**
```json
[
  {
    "timestamp": 1777637200.5,
    "source_id": "pump_motor",
    "tag": "temperature",
    "value": 65.3,
    "severity": "info"
  },
  {
    "timestamp": 1777637205.8,
    "source_id": "pump_motor",
    "tag": "vibration",
    "value": 2.1,
    "severity": "warning"
  }
]
```

**Status Code:** 200

---

### 4. Latest Predictions
**GET** `/api/predictions`

Get the latest failure predictions for all monitored sources.

**Response:**
```json
{
  "motor_1": {
    "health_score": 78.5,
    "is_anomaly": false,
    "anomaly_reason": "",
    "current_value": 45.2,
    "forecast": [44.8, 44.3, 43.9, 43.4, 42.9],
    "minutes_to_event": null,
    "timestamp": 1777637240.1
  }
}
```

**Status Code:** 200

---

### 5. Historical Reports
**GET** `/api/reports`

Retrieve up to 20 most recent historical reports.

**Response:**
```json
[
  {
    "timestamp": 1777635000.0,
    "scenario": "motor_overheat",
    "predicted_ok": true,
    "detection_time": 45.3,
    "notes": "Successfully detected temperature spike"
  }
]
```

**Status Code:** 200

---

### 6. Digital Twin Snapshot
**GET** `/api/twin`

Get the current state of the digital twin (virtual system mirror).

**Response:**
```json
{
  "pump_motor": {
    "timestamp": 1777637240.5,
    "severity": "info",
    "metrics": {
      "temperature": 62.5,
      "vibration": 2.1,
      "pressure": 100.2
    },
    "is_injected": false
  }
}
```

**Status Code:** 200

---

### 7. List Available Scenarios
**GET** `/api/simulator/scenarios`

Get a list of available fault injection scenarios.

**Response:**
```json
[
  "motor_overheat",
  "sensor_drift",
  "memory_leak"
]
```

**Status Code:** 200

---

### 8. Run Scenario
**POST** `/api/simulator/run/{scenario_name}`

Start a fault scenario simulation.

**Example:**
```
POST /api/simulator/run/motor_overheat
```

**Response:**
```json
{
  "status": "started",
  "scenario": "motor_overheat"
}
```

**Status Code:** 200

---

### 9. Inject Fault
**POST** `/api/simulator/inject`

Manually inject a fault into the digital twin for testing.

**Request Body:**
```json
{
  "source_id": "pump_motor",
  "tag": "temperature",
  "fault_type": "spike",
  "magnitude": 50.0,
  "duration": 30
}
```

**Fault Types:**
- `spike` - Instant value spike
- `drift` - Gradual linear drift
- `oscillate` - Sine-wave oscillation
- `stuck` - Freeze at current value
- `flood_errors` - Inject repeated errors

**Response:**
```json
{
  "status": "injected",
  "fault_id": "pump_motor:temperature:spike:1777637243"
}
```

**Status Code:** 200

---

### 10. Cancel Injections
**POST** `/api/simulator/cancel`

Cancel all active fault injections.

**Response:**
```json
{
  "status": "cancelled"
}
```

**Status Code:** 200

---

### 11. WebSocket (Real-time Updates)
**WebSocket** `/ws`

Establish a WebSocket connection for real-time push updates (every 2 seconds).

**Connection:**
```javascript
const ws = new WebSocket('ws://127.0.0.1:8000/ws');

ws.onmessage = (event) => {
  const data = JSON.parse(event.data);
  console.log('Update:', data);
};
```

**Update Message Format:**
```json
{
  "type": "tick",
  "timestamp": 1777637240.5,
  "bus_stats": {},
  "agent": {
    "source_id": {
      "health_score": 85.5,
      "forecast": [...]
    }
  },
  "system_health": 85.5,
  "simulator": {
    "active_injections": 0
  },
  "actions": []
}
```

---

## Error Handling

All endpoints return appropriate HTTP status codes:
- `200` - Success
- `400` - Bad request
- `404` - Not found
- `500` - Server error

Error responses include error details:
```json
{
  "detail": "Error message here"
}
```

---

## Testing the API

### Quick Test Commands

```bash
# Health check
curl http://127.0.0.1:8000/health

# System status
curl http://127.0.0.1:8000/api/status

# List events
curl http://127.0.0.1:8000/api/events?limit=10

# Get predictions
curl http://127.0.0.1:8000/api/predictions

# Inject a test fault
curl -X POST http://127.0.0.1:8000/api/simulator/inject \
  -H "Content-Type: application/json" \
  -d '{"source_id":"motor_1","tag":"temperature","fault_type":"spike","magnitude":40}'
```

### Python Client Example

```python
import requests
import json

BASE = "http://127.0.0.1:8000"

# Get status
response = requests.get(f"{BASE}/api/status")
print(json.dumps(response.json(), indent=2))

# Inject fault
fault = {
    "source_id": "pump_motor",
    "tag": "temperature",
    "fault_type": "spike",
    "magnitude": 50
}
response = requests.post(f"{BASE}/api/simulator/inject", json=fault)
print(response.json())
```

---

## Integration Notes

- The API is **async** and non-blocking
- All endpoints are **CORS-enabled** for dashboard integration
- WebSocket provides real-time updates without polling
- Fault injections are timestamped and reversible
- Event bus has configurable queue size for backpressure handling
