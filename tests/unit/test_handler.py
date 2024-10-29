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


@pytest.fixture()
def basic_ocr_step_output():
    """ Generates API GW Event based on output from OCR step Lambda"""

    return {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bucket": s3_bucket,
            "orig": "raw/mn-sherburne-county/batch3/R3Part2/Abstract 88291.jpg",
            "json": "ocr/json/mn-sherburne-county/batch3/R3Part2/Abstract 88291.json",
            "txt": "ocr/txt/mn-sherburne-county/batch3/R3Part2/Abstract 88291.txt",
            "stats": "ocr/stats/mn-sherburne-county/batch3/R3Part2/Abstract 88291__69727524d8d04bfc99ee0f0bf22584e0.json",
            "uuid": "69727524d8d04bfc99ee0f0bf22584e0",
            "handwriting_pct": 0.01
        }
    }


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

    # assert 'up' == 'down'


    # # Check if image in correct mode
    # im = open_s3_image(data['bucket'], data['highlighted_img'])
    # assert im.mode == 'RGB'