# FEC IoT Project — Urban Air Quality Monitoring
## Fog and Edge Computing (H9FECC) · NCI MSc Cloud Computing · 2026

A 3-tier fog-cloud IoT architecture implementing scalable real-time
urban air quality monitoring using AWS. Sensor simulators publish
environmental data via MQTT to a fog node, which aggregates and dispatches
processed batches to a scalable AWS backend (SQS → Lambda → DynamoDB)
with a CloudWatch dashboard.

---

## Project Structure

```
fec_project/
├── sensors/                    # Sensor simulator layer
│   ├── base_sensor.py          # Abstract base class for all sensors
│   ├── temperature_sensor.py   # Ambient temperature (°C)
│   ├── humidity_sensor.py      # Relative humidity (%)
│   ├── co2_sensor.py           # CO₂ concentration (ppm)
│   ├── pm25_sensor.py          # PM2.5 fine particulates (µg/m³)
│   ├── uv_index_sensor.py      # UV Index (UVI)
│   └── run_all_sensors.py      # Launch all 5 sensors as threads
├── fog/
│   └── fog_node.py             # Fog processing node
├── backend/
│   └── lambda_functions/
│       ├── process_iot_data.py # SQS-triggered processor + CloudWatch metrics
│       ├── get_readings.py     # GET /readings API handler
│       ├── get_alerts.py       # GET /alerts API handler
│       └── get_latest.py       # GET /latest API handler
├── infra/
│   ├── setup_aws_infra.py      # Provision all AWS resources
│   ├── setup_api_gateway.py    # Create API Gateway REST API
│   └── fog-node.service        # systemd service definition
├── tests/
│   ├── test_fog_node.py        # Unit tests for fog processing logic
│   └── test_lambda.py          # Unit tests for Lambda handlers
├── .github/workflows/
│   └── deploy.yml              # CI/CD pipeline (GitHub Actions)
└── requirements.txt
```

---

## Prerequisites

- Python 3.12+
- AWS Account 
- AWS CLI configured: `aws configure`
- An EC2 t2.micro instance (for fog node deployment)
-


## Sensor Types Summary

| Sensor ID  | Type        | Unit    | Normal Range     | Alert Threshold |
|------------|-------------|---------|------------------|-----------------|
| temp_01    | temperature | °C      | 10–25°C          | > 35°C          |
| hum_01     | humidity    | %       | 40–80%           | < 20% or > 95%  |
| co2_01     | co2         | ppm     | 400–600 ppm      | > 800 ppm       |
| pm25_01    | pm25        | µg/m³   | 0–25 µg/m³       | > 35 µg/m³      |
| uv_01      | uv_index    | UVI     | 0–7 UVI          | > 6 UVI         |

