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
- AWS Account (Free Tier is sufficient)
- AWS CLI configured: `aws configure`
- An EC2 t2.micro instance (for fog node deployment)
- Git

---

## Local Development Setup

### 1. Clone the repository

```bash
git clone https://github.com/YOUR_USERNAME/fec-iot-project.git
cd fec-iot-project
```

### 2. Create a Python virtual environment

```bash
python3 -m pip install virtualenv
python3 -m virtualenv venv
source venv/bin/activate          # Linux/macOS
# or: venv\Scripts\activate       # Windows
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Install a local MQTT broker (for local testing)

```bash
# Ubuntu/Debian:
sudo apt-get install -y mosquitto mosquitto-clients
sudo systemctl start mosquitto

# macOS:
brew install mosquitto
brew services start mosquitto
```

### 5. Run sensors locally (against local broker)

```bash
cd sensors
MQTT_HOST=localhost PUBLISH_INTERVAL=3.0 python run_all_sensors.py
```

You should see all 5 sensors publishing readings. Verify with:
```bash
mosquitto_sub -h localhost -t "sensors/#" -v
```

---

## AWS Infrastructure Deployment

### Step 1: Configure AWS credentials

```bash
aws configure
# Enter: AWS Access Key ID, Secret Access Key, Region (eu-west-1), output (json)
```

### Step 2: Provision all AWS resources

This script creates DynamoDB tables, SQS queues, IAM role,
Lambda functions, SQS→Lambda trigger, and CloudWatch Dashboard.

```bash
cd infra
AWS_REGION=eu-west-1 python setup_aws_infra.py
```

Copy the SQS Queue URL printed at the end. You will need it for the fog node.

### Step 3: Set up API Gateway

```bash
AWS_REGION=eu-west-1 python setup_api_gateway.py
```

Copy the base URL printed at the end (e.g. `https://abc123.execute-api.eu-west-1.amazonaws.com/prod`).

---

## Fog Node — EC2 Deployment

### Step 1: Launch EC2 instance

In the AWS Console:
- Launch a t2.micro instance (Ubuntu 22.04 LTS)
- Attach an IAM role with SQS SendMessage permission
- Open inbound port 22 (SSH) in the Security Group
- Download the key pair (.pem file)

### Step 2: Set up AWS IoT Core certificates

In the AWS Console → IoT Core → Manage → Things:
1. Create a new Thing named `fog-node-01`
2. Create and download certificates
3. Download the Amazon Root CA 1 certificate
4. Attach a policy with `iot:Connect`, `iot:Subscribe`, `iot:Receive`
5. Copy the device data endpoint (Settings → Device data endpoint)

### Step 3: SSH into EC2 and deploy

```bash
ssh -i your-key.pem ubuntu@YOUR_EC2_PUBLIC_IP

# On the EC2 instance:
sudo apt-get update
sudo apt-get install -y python3-pip python3-venv git

git clone https://github.com/YOUR_USERNAME/fec-iot-project.git
cd fec-iot-project
pip3 install -r requirements.txt

# Create certs directory and upload your IoT Core certificates
mkdir ~/certs
# Use scp from your local machine to copy:
# scp -i your-key.pem AmazonRootCA1.pem ubuntu@IP:~/certs/
# scp -i your-key.pem device.pem.crt ubuntu@IP:~/certs/
# scp -i your-key.pem private.pem.key ubuntu@IP:~/certs/
```

### Step 4: Configure and start the fog node service

```bash
# Edit the service file with your values
sudo cp fec-iot-project/infra/fog-node.service /etc/systemd/system/

sudo nano /etc/systemd/system/fog-node.service
# Update: MQTT_HOST, SQS_QUEUE_URL with your actual values

sudo systemctl daemon-reload
sudo systemctl enable fog-node.service
sudo systemctl start fog-node.service

# Check it's running:
sudo systemctl status fog-node.service
sudo journalctl -u fog-node.service -f
```

### Step 5: Run sensors (pointing at IoT Core)

On your local machine or a separate EC2 instance:

```bash
export MQTT_HOST=YOUR_IOT_CORE_ENDPOINT   # e.g. abc123-ats.iot.eu-west-1.amazonaws.com
export MQTT_PORT=8883
export USE_TLS=true
export CA_CERT_PATH=/path/to/AmazonRootCA1.pem
export CERT_PATH=/path/to/device.pem.crt
export KEY_PATH=/path/to/private.pem.key
export PUBLISH_INTERVAL=2.0

cd sensors
python run_all_sensors.py
```

---

## CloudWatch Dashboard

1. Open AWS Console → CloudWatch → Dashboards
2. Select **FogEdgeIoT-Dashboard**
3. You will see:
   - Row 1: Single-value panels for all 5 current sensor readings
   - Row 2: Time-series charts for Temperature/Humidity and CO₂/PM2.5
   - Row 3: Anomaly count per sensor over time

The dashboard auto-refreshes. Set it to 1-minute auto-refresh for the demo.

---

## API Endpoints

All endpoints return JSON. Replace `BASE_URL` with your API Gateway URL.

```bash
# Most recent reading per sensor type
curl "BASE_URL/latest"

# Readings for past 24h (all sensors)
curl "BASE_URL/readings"

# Readings filtered by sensor type
curl "BASE_URL/readings?sensor_type=co2"

# Readings for a specific sensor and time range
curl "BASE_URL/readings?sensor_id=co2_01&from=2026-03-17T00:00:00Z"

# All anomaly alerts
curl "BASE_URL/alerts"

# High-direction alerts for CO2 only
curl "BASE_URL/alerts?sensor_type=co2&direction=high"
```

---

## Running Tests

```bash
# Install test dependencies
pip install pytest pytest-cov moto[all]

# Run all tests with coverage
pytest tests/ -v --cov=fog --cov=backend/lambda_functions --cov-report=term-missing

# Run a specific test file
pytest tests/test_fog_node.py -v
pytest tests/test_lambda.py -v
```

---

## CI/CD Pipeline (GitHub Actions)

The `.github/workflows/deploy.yml` pipeline runs on every push to `main`:

1. **Test job**: Runs all unit tests with coverage
2. **deploy-lambda job**: Zips and deploys all 4 Lambda functions via AWS CLI
3. **deploy-fog job**: Pushes updated fog node code to EC2 via AWS SSM Run Command

**Required GitHub Secrets** (Settings → Secrets → Actions):
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `FOG_NODE_INSTANCE_ID` (your EC2 instance ID, e.g. `i-0abc123def456`)

---

## Configuration Reference

| Variable             | Default      | Where Used          | Description                          |
|----------------------|--------------|---------------------|--------------------------------------|
| `MQTT_HOST`          | localhost    | Sensors, Fog node   | MQTT broker hostname                 |
| `MQTT_PORT`          | 1883         | Sensors, Fog node   | MQTT broker port (8883 for TLS)      |
| `PUBLISH_INTERVAL`   | 2.0          | Sensors             | Seconds between sensor readings      |
| `USE_TLS`            | false        | Sensors, Fog node   | Enable TLS for AWS IoT Core          |
| `CA_CERT_PATH`       | —            | Sensors, Fog node   | Path to Amazon Root CA certificate   |
| `CERT_PATH`          | —            | Sensors, Fog node   | Path to device certificate           |
| `KEY_PATH`           | —            | Sensors, Fog node   | Path to private key                  |
| `SQS_QUEUE_URL`      | —            | Fog node            | AWS SQS queue URL (required)         |
| `AWS_REGION`         | eu-west-1    | Fog node, Lambda    | AWS region                           |
| `FOG_NODE_ID`        | fog_node_01  | Fog node            | Unique fog node identifier           |
| `DISPATCH_INTERVAL`  | 30           | Fog node            | Seconds between SQS dispatches       |
| `WINDOW_SIZE`        | 20           | Fog node            | Rolling window size (readings)       |
| `ANOMALY_SIGMA`      | 2.0          | Fog node            | Sigma threshold for anomaly flagging |
| `READINGS_TABLE`     | iot_readings | Lambda              | DynamoDB readings table name         |
| `ALERTS_TABLE`       | iot_alerts   | Lambda              | DynamoDB alerts table name           |
| `CW_NAMESPACE`       | FogEdgeIoT   | Lambda              | CloudWatch custom metrics namespace  |

---

## Sensor Types Summary

| Sensor ID  | Type        | Unit    | Normal Range     | Alert Threshold |
|------------|-------------|---------|------------------|-----------------|
| temp_01    | temperature | °C      | 10–25°C          | > 35°C          |
| hum_01     | humidity    | %       | 40–80%           | < 20% or > 95%  |
| co2_01     | co2         | ppm     | 400–600 ppm      | > 800 ppm       |
| pm25_01    | pm25        | µg/m³   | 0–25 µg/m³       | > 35 µg/m³      |
| uv_01      | uv_index    | UVI     | 0–7 UVI          | > 6 UVI         |

---

## Troubleshooting

**Fog node not connecting to IoT Core:**
- Verify the IoT Core endpoint in `MQTT_HOST` (no `https://`, no trailing `/`)
- Check certificate file permissions: `chmod 400 ~/certs/*.pem ~/certs/*.key`
- Ensure your IoT Core policy allows `iot:Connect`, `iot:Publish`, `iot:Subscribe`

**Lambda not triggered from SQS:**
- Check the event source mapping is Enabled in the Lambda console
- Verify the Lambda IAM role has `sqs:ReceiveMessage`, `sqs:DeleteMessage`

**No data in CloudWatch dashboard:**
- Wait 2–3 minutes after the fog node first dispatches (CloudWatch metric data has ~1min delay)
- Check the Lambda CloudWatch Logs (`/aws/lambda/fec-iot-process`) for errors

**DynamoDB write errors:**
- Confirm table names match env vars (`iot_readings`, `iot_alerts`)
- Check Lambda IAM role has DynamoDB permissions for the correct region

---

## Academic Note

This project was developed as coursework for H9FECC (Fog and Edge Computing),
MSc Cloud Computing, NCI, Semester 2, 2026. All code is original work by the
student. No components were reused from previous projects. Do not share or
publish this code in violation of NCI academic integrity policies.
