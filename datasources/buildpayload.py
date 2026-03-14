import os
import json
from typing import Dict, List
from datasources.datasourcesProp import SOURCE_TYPES
# from datasources.datasources import COMMON_PROPERTIES
# from datasources.datasources import PARSED_FIELDS

def build_payload(source_type: str, props: Dict[str, str]) -> Dict:
    kind = SOURCE_TYPES[source_type]["kind"]
    properties = {}

    if source_type == "AdlsGen2":
        properties.update({
            "endpoint": props.get("endpoint", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })
    elif source_type == "AzureSubscription":
        properties.update({
            "subscriptionId": props.get("subscription_id", ""),
            "resourceId": props.get("resource_id", "")
        })
    elif source_type == "AzureResourceGroup":
        properties.update({
            "resourceGroup": props.get("resource_group"),
            "subscriptionId": props.get("subscription_id", ""),
            "resourceId": props.get("resource_id", "")
        })
    elif source_type == "AWS account":
        properties.update({
            "awsAccountId": props.get("awsAccount_id"),
        })
    elif source_type == "AzureSynapseAnalytics":
        properties.update({
            "dedicatedSqlEndpoint": props.get("dedicated_SqlEndpoint", ""),
            "serverlessSqlEndpoint": props.get("serverless_SqlEndpoint", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })
    elif source_type == "AmazonPostgreSql":
        properties.update({
            "serverEndpoint": props.get("server_Endpoint", ""),
            "port": props.get("Port", "")
        })
    elif source_type == "AmazonSql":
        properties.update({
            "serverEndpoint": props.get("server_Endpoint", ""),
            "port": props.get("Port", "")
        })
    elif source_type == "AmazonRedShift":
        properties.update({
            "host": props.get("Host", ""),
            "port": props.get("Port", "")
        })
    elif source_type == "AmazonS3":
        properties.update({
            "serviceUrl": props.get("service_Url", "")
        })
    elif source_type == "AzureStorage":
        properties.update({
            "endpoint": props.get("endpoint", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })

    elif source_type == "AzureSqlDatabase":
        properties.update({
            "serverEndpoint": props.get("server_endpoint", ""),
            "resourceId": props.get("resource_id", ""),
            "subscriptionId": props.get("subscription_id", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceName": props.get("resource_name", ""),
            "location": props.get("location", "")
        })

    elif source_type == "AzureCosmosDb":
        properties.update({
            "accountUri": props.get("account_uri", ""),
            "location": props.get("location", ""),
            "resourceGroup": props.get("resource_group", ""),
            "resourceId": props.get("resource_id", ""),
            "resourceName": props.get("resource_name", ""),
            "subscriptionId": props.get("subscription_id", "")
        })
    elif source_type == "Fabric":
        properties.update({
            "tenant": props.get("tenant", ""),
        })

    elif source_type == "SqlServer":
        properties.update({"serverEndpoint": props.get("server_endpoint", "")})

    elif source_type == "Oracle":
        properties.update({
            "host": props.get("host", ""),
            "port": props.get("port", ""),
            "serviceName": props.get("service_name", "")
        })

    elif source_type == "Teradata":
        properties.update({"host": props.get("host", "")})

    elif source_type == "SapS4Hana":
        properties.update({
            "applicationServer": props.get("application_server", ""),
            "systemNumber": props.get("system_number", "")
        })

    properties["collection"] = {
        "type": "CollectionReference",
        "referenceName": props.get("collection_name", "")
    }

    return {"name": props.get("ds_name", ""), "kind": kind, "properties": properties}
