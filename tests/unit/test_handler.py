import io
import json
import toml
import boto3
import pytest

from resize_image import app

with open('samconfig.toml', 'r') as f:
    config = toml.load(f)
    s3_bucket = config['default']['deploy']['parameters']['s3_bucket']
    s3_region = config['default']['deploy']['parameters']['region']
    s3_bucket_in = config['default']['deploy']['parameters']['s3_bucket_in']
    s3_bucket_out = config['default']['deploy']['parameters']['s3_bucket_out']


s3 = boto3.client('s3')


def get_s3_match_json(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(response['Body'].read())


# def build_lambda_input(bucket, infile_json):

#     return {
#         "statusCode": 200,
#         "body": {
#             "message": "hello world",
#             "bucket": bucket,
#             "orig": "raw/mn-sherburne-county/batch3/R3Part2/Abstract 88291.jpg",
#             "json": infile_json,
#             # "txt": "ocr/txt/mn-sherburne-county/batch3/R3Part2/Abstract 88291.txt",
#             # "stats": "ocr/stats/mn-sherburne-county/batch3/R3Part2/Abstract 88291__69727524d8d04bfc99ee0f0bf22584e0.json",
#             "uuid": "69727524d8d04bfc99ee0f0bf22584e0",
#             # "handwriting_pct": 0.01
#         }
#     }


def build_ocr_step_output(remainder, uuid, in_bucket=None, out_bucket=None, orig_key=None):
    """ Generates API GW Event based on output from OCR step Lambda"""

    orig = orig_key if orig_key else f"raw/{remainder}.jpg"

    event_json = {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bucket": None if in_bucket else s3_bucket,
            "orig": orig,
            "json": f"ocr/json/{remainder}.json",
            "txt": f"ocr/txt/{remainder}.txt",
            "stats": f"ocr/stats/{remainder}__{uuid}.json",
            "uuid": uuid,
            "handwriting_pct": 0.01
        }
    }

    if out_bucket:
        event_json['body']['in_bucket'] = in_bucket
        event_json['body']['out_bucket'] = out_bucket

    return event_json


@pytest.fixture()
def basic_ocr_step_output():
    return build_ocr_step_output("mn-sherburne-county/batch3/R3Part2/Abstract 88291", "69727524d8d04bfc99ee0f0bf22584e0")

@pytest.fixture()
def ocr_step_output_2_buckets():
    return build_ocr_step_output("mn-stearns-county/mn_stearns-usmnstr-dee-346-000-0000-000_00006-000", "69727524d8d04bfc99ee0f0bf22584e0", s3_bucket_in, s3_bucket_out, 'test/mn-stearns-county/mn_stearns-usmnstr-dee-346-000-0000-000_00006-000.jpg')


def test_input_output_results(basic_ocr_step_output):
    ''' Does this run appropriately with output of mp-covenants-ocr-page Lambda?'''
    fixture = basic_ocr_step_output

    ret = app.lambda_handler(fixture, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "hello world"

    assert "uuid" in data
    assert "orig_img" in data
    assert "ocr_json" in data

    assert data["uuid"] == fixture['body']['uuid']
    assert data["orig_img"] == fixture['body']['orig']
    assert data["ocr_json"] == fixture['body']['json']

def test_input_output_results_2_buckets(ocr_step_output_2_buckets):
    ''' Does this run appropriately with output of mp-covenants-ocr-page Lambda?'''
    fixture = ocr_step_output_2_buckets

    assert fixture['body']['in_bucket'] == s3_bucket_in

    ret = app.lambda_handler(fixture, "")
    data = ret["body"]
    print(data)

    assert ret["statusCode"] == 200
    assert data["message"] == "hello world"

    assert "uuid" in data
    assert "orig_img" in data
    assert "ocr_json" in data

    # assert data["uuid"] == fixture['body']['uuid']
    # assert data["orig_img"] == fixture['body']['orig']
    # assert data["ocr_json"] == fixture['body']['json']
    assert data["bucket"] == fixture['body']['out_bucket']
