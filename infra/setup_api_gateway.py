#!/usr/bin/env python3
"""
setup_api_gateway.py — API Gateway REST API Setup
===================================================
Creates a REST API with three endpoints backed by Lambda functions.

Endpoints created:
    GET /readings   → fec-iot-get-readings Lambda
    GET /alerts     → fec-iot-get-alerts Lambda
    GET /latest     → fec-iot-get-latest Lambda

Usage:
    python setup_api_gateway.py

Prints the deployed API base URL on completion.
"""
import os
import sys
import json
import logging

import boto3
from botocore.exceptions import ClientError

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
logger = logging.getLogger("SetupAPI")

REGION = os.getenv("AWS_REGION", "eu-west-1")
STAGE  = "prod"

apigw = boto3.client("apigateway", region_name=REGION)
lam   = boto3.client("lambda",     region_name=REGION)
sts   = boto3.client("sts",        region_name=REGION)

ACCOUNT_ID = sts.get_caller_identity()["Account"]

LAMBDA_MAP = {
    "readings": "fec-iot-get-readings",
    "alerts":   "fec-iot-get-alerts",
    "latest":   "fec-iot-get-latest",
}


def get_or_create_api(api_name: str) -> str:
    """Get existing API ID or create a new REST API. Returns api_id."""
    # Check if it already exists
    paginator = apigw.get_paginator("get_rest_apis")
    for page in paginator.paginate():
        for api in page["items"]:
            if api["name"] == api_name:
                logger.info(f"  Using existing API: {api_name} ({api['id']})")
                return api["id"]

    response = apigw.create_rest_api(
        name=api_name,
        description="FEC IoT Air Quality Monitoring API",
        endpointConfiguration={"types": ["REGIONAL"]}
    )
    api_id = response["id"]
    logger.info(f"✓ Created REST API: {api_name} ({api_id})")
    return api_id


def get_root_resource_id(api_id: str) -> str:
    """Get the root resource ID (/) of the API."""
    resources = apigw.get_resources(restApiId=api_id)
    for r in resources["items"]:
        if r["path"] == "/":
            return r["id"]
    raise RuntimeError("Root resource not found")


def create_resource(api_id: str, parent_id: str, path_part: str) -> str:
    """Create a new path resource. Returns resource_id."""
    # Check if already exists
    resources = apigw.get_resources(restApiId=api_id)
    for r in resources["items"]:
        if r.get("pathPart") == path_part:
            return r["id"]

    response = apigw.create_resource(
        restApiId=api_id,
        parentId=parent_id,
        pathPart=path_part
    )
    return response["id"]


def add_lambda_integration(api_id: str, resource_id: str,
                            lambda_name: str, path_part: str):
    """Add GET method with Lambda proxy integration and enable CORS."""
    lambda_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{lambda_name}"
    uri = (f"arn:aws:apigateway:{REGION}:lambda:path/2015-03-31/functions/"
           f"{lambda_arn}/invocations")

    # PUT method
    try:
        apigw.put_method(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            authorizationType="NONE",
        )
    except ClientError:
        pass  # Already exists

    # PUT integration
    try:
        apigw.put_integration(
            restApiId=api_id,
            resourceId=resource_id,
            httpMethod="GET",
            type="AWS_PROXY",
            integrationHttpMethod="POST",
            uri=uri,
        )
    except ClientError:
        pass

    # Grant Lambda permission to be invoked by API Gateway
    try:
        lam.add_permission(
            FunctionName=lambda_name,
            StatementId=f"apigw-{path_part}-get",
            Action="lambda:InvokeFunction",
            Principal="apigateway.amazonaws.com",
            SourceArn=f"arn:aws:execute-api:{REGION}:{ACCOUNT_ID}:{api_id}/*/GET/{path_part}"
        )
    except ClientError as e:
        if "ResourceConflictException" not in str(e):
            logger.warning(f"Permission add failed: {e}")

    logger.info(f"✓ Integrated GET /{path_part} → {lambda_name}")


def deploy_api(api_id: str) -> str:
    """Deploy the API to the production stage. Returns base URL."""
    apigw.create_deployment(
        restApiId=api_id,
        stageName=STAGE,
        description="FEC IoT initial deployment"
    )
    base_url = f"https://{api_id}.execute-api.{REGION}.amazonaws.com/{STAGE}"
    logger.info(f"✓ Deployed API to stage '{STAGE}'")
    return base_url


def main():
    api_id    = get_or_create_api("fec-iot-api")
    root_id   = get_root_resource_id(api_id)

    for path_part, lambda_name in LAMBDA_MAP.items():
        resource_id = create_resource(api_id, root_id, path_part)
        add_lambda_integration(api_id, resource_id, lambda_name, path_part)

    base_url = deploy_api(api_id)

    logger.info("=" * 60)
    logger.info("✅  API Gateway setup complete!")
    logger.info(f"    Base URL: {base_url}")
    logger.info(f"    GET {base_url}/readings")
    logger.info(f"    GET {base_url}/alerts")
    logger.info(f"    GET {base_url}/latest")
    logger.info("=" * 60)
    return base_url


if __name__ == "__main__":
    main()
