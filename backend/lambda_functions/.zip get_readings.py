{
    "FunctionName": "fec-iot-process",
    "FunctionArn": "arn:aws:lambda:us-east-1:801564196385:function:fec-iot-process",
    "Runtime": "python3.9",
    "Role": "arn:aws:iam::801564196385:role/LabRole",
    "Handler": "process_iot_data.lambda_handler",
    "CodeSize": 2501,
    "Description": "",
    "Timeout": 60,
    "MemorySize": 128,
    "LastModified": "2026-04-10T11:17:31.692+0000",
    "CodeSha256": "mmG0uJMcicsECQaqorVodPjaeORCoqC/IYsfnd7Kbo8=",
    "Version": "$LATEST",
    "Environment": {
        "Variables": {
            "READINGS_TABLE": "iot_readings",
            "CW_NAMESPACE": "FogEdgeIoT",
            "AWS_REGION_NAME": "us-east-1",
            "ALERTS_TABLE": "iot_alerts"
        }
    },
    "TracingConfig": {
        "Mode": "PassThrough"
    },
    "RevisionId": "0b328bfb-ed51-4152-a06b-896b5c498c00",
    "State": "Pending",
    "StateReason": "The function is being created.",
    "StateReasonCode": "Creating",
    "PackageType": "Zip",
    "Architectures": [
        "x86_64"
    ],
    "EphemeralStorage": {
        "Size": 512
    },
    "SnapStart": {
        "ApplyOn": "None",
        "OptimizationStatus": "Off"
    },
    "RuntimeVersionConfig": {
        "RuntimeVersionArn": "arn:aws:lambda:us-east-1::runtime:b46f7bc0f3da8071d1b824471f2c69c8766b756b827eb0455d2118c622ae7bcf"
    },
    "LoggingConfig": {
        "LogFormat": "Text",
        "LogGroup": "/aws/lambda/fec-iot-process"
    }
}
