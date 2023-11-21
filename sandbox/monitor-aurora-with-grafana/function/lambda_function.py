"""
Purpose
Shows how to use lambda function to get Performance Insight metrics from Amazon Aurora and publish to CloudWatch
@mundabra
@sharajag
https://docs.amazonaws.cn/en_us/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights.UsingDashboard.AnalyzeDBLoad.AdditionalMetrics.PostgreSQL.html
"""

from base64 import encode
import time
import boto3
import os
import logging
import json
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

pi_client = boto3.client('pi')
rds_client = boto3.client('rds')
cw_client = boto3.client('cloudwatch')
targetMetricNamespace = os.environ.get('TargetMetricNamespace')

dbSliceGroup = { 
    "db.sql_tokenized", 
    "db.wait_event", 
    "db.user", 
    "db.session_type", 
    "db.host", 
    "db", 
    "db.application" 
}


def lambda_handler(event, context):
    # Get DB instances for which Performance Insights have been enabled
    pi_instances = get_pi_instances()
    logger.info('## PI Instances --> ')
    for [instance, rds_id] in pi_instances:
        pi_response = get_db_resource_metrics(instance)

        if pi_response:
            send_cloudwatch_data(pi_response, rds_id)

    return {
        'statusCode': 200,
        'body': 'ok'
    }

# Get DB instances for which Performance Insights have been enabled
def get_pi_instances():
    dbInstancesResponse = rds_client.describe_db_instances()

    if dbInstancesResponse:
        response = filter(lambda _: _.get('PerformanceInsightsEnabled', False), dbInstancesResponse['DBInstances'])
        
        if response:
            dbInstanceList = [[item['DbiResourceId'],item['DBInstanceIdentifier']] for item in response]
            logger.info("rds instances with pi enabled: {}".format([item for item in response]))
            return dbInstanceList
    return None


def get_db_resource_metrics(instance):
    # Build metric query list
    metric_queries = []
    for group in dbSliceGroup:
        # The example specifies the metric of db.load.avg and a GroupBy of the top seven wait events
        # https://docs.aws.amazon.com/AmazonRDS/latest/AuroraUserGuide/USER_PerfInsights.API.html#USER_PerfInsights.API.Examples.CounterMetrics
        
        logger.info('## metrics for group --> %s ', group)
        metric_queries.append(
        {
            "Metric": "db.load.avg", 
            "GroupBy":
                
                { 
                    "Group": group
                }
        }
        )
        
    if not metric_queries:
        return None

    try:
        response = pi_client.get_resource_metrics(
                    ServiceType='RDS',
                    Identifier=instance,
                    StartTime= time.time() - 900,   #15 mins
                    EndTime= time.time(),
                    PeriodInSeconds=60,
                    MetricQueries=metric_queries
                )
        logger.info("pi client response: {}".format(response))
    except pi_client.exceptions.InvalidArgumentException as error:
        logger.error('## Invalid Argument Exception --> %s ', error)
        return None

    return response

def str_encode(string):
    encoded_str = string.encode("ascii","ignore")
    return remove_non_ascii(encoded_str.decode())

def remove_non_ascii(string):
    non_ascii = ascii(string)
    return non_ascii

def send_cloudwatch_data(pi_response, rds_id):
    
    metric_data = []
    db_identifier = pi_response.get('Identifier')
    
    for metric_response in pi_response['MetricList']: #dataoints and key
        metric_dict = metric_response['Key']  #db.load.avg
        metric_name = metric_dict['Metric']
     
        is_metric_dimensions = False
        formatted_dims = []
        if metric_dict.get('Dimensions'):
            metric_dimensions = metric_response['Key']['Dimensions']  # return a dictionary
            pid_identifier = str_encode(db_identifier).replace('\'','')
            formatted_dims.append(dict(Name='identifier', Value=pid_identifier))
            formatted_dims.append(dict(Name='rds_identifier', Value=rds_id))

            for key in metric_dimensions:
                metric_name = key.split(".")[1]
                formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key]).replace('\'','')))
                """ if key == "db.sql_tokenized.statement":
                    formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key])))
                else:
                    formatted_dims.append(dict(Name=key, Value=str_encode(metric_dimensions[key]))) """

            is_metric_dimensions = True
        else:
            metric_name = metric_name.replace("avg","")


        for datapoint in metric_response['DataPoints']:
            # We don't always have values from an instance
            value = datapoint.get('Value', None)

            if value:
                if is_metric_dimensions:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Dimensions': formatted_dims,
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    })
                else:
                    metric_data.append({
                        'MetricName': metric_name,
                        'Timestamp': datapoint['Timestamp'],
                        'Value': round(datapoint['Value'], 2)
                    }) 
    
    result = []
    max_elements = 500
    for i in range(0, len(metric_data), max_elements):
        result.append(metric_data[i:i + max_elements])
    
    if metric_data:
        for entry in result:
            logger.info('## sending data to cloduwatch: {}'.format(entry[:20]))
            try:
                cw_client.put_metric_data(
                Namespace= targetMetricNamespace,
                MetricData= entry)
            except ClientError as error:
                raise ValueError('The parameters you provided are incorrect: {}'.format(error))
    else:
        logger.info('## NO Metric Data ##')



