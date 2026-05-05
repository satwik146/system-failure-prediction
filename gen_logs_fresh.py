from datetime import datetime, timedelta
import random

lines = []
now = datetime.now()
severities = ['INFO', 'WARNING', 'ERROR', 'CRITICAL']
sources = ['pump_01', 'compressor_01', 'conveyor_01', 'app_log']

for i in range(500):
    ts = now - timedelta(seconds=i*3)
    sev = random.choice(severities)
    src = random.choice(sources)
    
    # Generate realistic sensor messages with numeric values
    msg_templates = [
        f"Temperature reading: {random.randint(20, 120)}°C",
        f"Vibration level: {random.randint(10, 80)} Hz",
        f"Pressure value: {random.uniform(3, 10):.1f} bar",
        f"Motor speed: {random.randint(800, 2000)} rpm",
        f"Current draw: {random.uniform(5, 25):.1f} A",
        f"Memory usage: {random.randint(10, 95)}%",
        f"CPU wait: {random.randint(0, 80)}%",
        f"Queue depth: {random.randint(0, 500)}",
        f"Response time: {random.uniform(0.1, 5.0):.2f}s",
        f"Sensor drift: {random.uniform(-10, 10):.2f} units",
    ]
    
    msg = random.choice(msg_templates)
    line = f"{ts.strftime('%Y-%m-%d %H:%M:%S')} [{sev}] {src}: {msg}"
    lines.append(line)

with open('./data/sample.log', 'w') as f:
    f.write('\n'.join(lines) + '\n')
print(f'Created {len(lines)} log lines with numeric values')
