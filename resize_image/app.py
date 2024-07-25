import io
import re
import sys
import json
import math
import uuid
import urllib.parse
import boto3
from pathlib import PurePath
from PIL import Image, ImageFont, ImageDraw


s3 = boto3.client('s3')
Image.MAX_IMAGE_PIXELS = 1000000000


def save_jpeg_to_target_size(key: str, in_tiff_body: str, target_bytes: int, watermark=True, resize=False, resize_max=2000) -> str:
    """Save the image as JPEG with the given name at best quality that makes less than "target" bytes
    Source: https://stackoverflow.com/questions/52259476/how-to-reduce-a-jpeg-size-to-a-desired-size
    Args:
        in_tiff_body: The full path to the input raw or output TIF to convert
        target_bytes: The maximum file size. The output JPEG will be smaller than this value.
    Returns:
        out_jpg_path if save successful, or False
        watermark: Boolean -- whether or not to add a watermark
        resize: Boolean -- whether or not to resize the image to resize_max before optimizing
        resize_max: The maxium pixel width or height used to resize. The function will choose
            the largest dimension (width vs height) and resize that to the resize_max,
            and proportionally resize the other dimension
    """
    try:
        im = Image.open(in_tiff_body)
    except Image.UnidentifiedImageError as err:
        return None

    print(f'Opened {key} successfully')

    if im.mode != 'RGB':
        im = im.convert('RGB')

    if watermark:
        im = add_watermark(im)

    if resize:
        print('Trying with resize...')
        if im.size[0] >= im.size[1]:
            new_width = resize_max
            new_height = int(
                float(im.size[1]) * float(new_width/float(im.size[0])))
        else:
            new_height = resize_max
            new_width = int(float(im.size[0])
                            * float(new_height/float(im.size[1])))

        im = im.resize((new_width, new_height), Image.Resampling.LANCZOS)

    # Min and Max quality
    Qmin, Qmax = 12, 96
    # Highest acceptable quality found
    Qacc = -1
    while Qmin <= Qmax:
        m = math.floor((Qmin + Qmax) / 2)

        # Encode into memory and get size
        buffer = io.BytesIO()
        im.save(buffer, format="JPEG", quality=m)
        s = buffer.getbuffer().nbytes

        if s <= target_bytes:
            Qacc = m
            Qmin = m + 1
        elif s > target_bytes:
            Qmax = m - 1

    # Write to disk at the defined quality
    if Qacc > -1:
        buffer.seek(0)
        print(f'Successfully resized {key}.')
        return buffer
    else:
        print("ERROR: No acceptable quality factor found", file=sys.stderr)

    return False


def add_watermark(img, font_ratio=1.5, diagonal_percent=0.5,
                  opacity_scalar=0.15):
    """ Adds a translucent gray 'UNOFFICIAL DOCUMENT' watermark over the image."""

    img = img.convert('RGBA')

    width, height = img.size

    watermark_text = 'UNOFFICIAL DOCUMENT'
    watermark_length = len(watermark_text)

    diagonal_length = int(math.sqrt((width * width)
                                    + (height * height))) * diagonal_percent

    font_size = int(diagonal_length / (watermark_length / font_ratio))
    font = ImageFont.truetype('fonts/Arial.ttf', font_size)

    opacity = int(256 * opacity_scalar)

    # See details on issue with v 10.0.0 of Pillow: https://github.com/tensorflow/models/commit/0aa039f16361f14ee587a9b2d99a16be01d9dcf9
    if hasattr(font, 'getsize'):
        mark_width, mark_height = font.getsize(watermark_text)
    else:
        text_bbox = font.getbbox(watermark_text)
        mark_width = text_bbox[2]
        mark_height = text_bbox[3]

    watermark = Image.new('RGBA', (mark_width, mark_height), (0, 0, 0, 0))

    draw = ImageDraw.Draw(watermark)
    draw.text((0, 0), text=watermark_text, font=font, fill=(0, 0, 0, opacity))
    angle = math.degrees(math.atan(height / width))
    watermark = watermark.rotate(angle, expand=1)

    wx, wy = watermark.size
    px = int((width - wx)/2)
    py = int((height - wy)/2)
    img.paste(watermark, (px, py, px + wx, py + wy), watermark)
    img = img.convert('RGB')
    return img


def lambda_handler(event, context):
    """ This function receives a TIF or JPEG file and creates a scaled-down, web-friendly JPEG with a watermark on it for public transcription using the Pillow library. The output filename includes a randomized UUID suffix to deter scraping, since this image's permissions will be set to publicly viewable."""

    if 'Records' in event:
        # Get the object from a more standard put event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = urllib.parse.unquote_plus(
            event['Records'][0]['s3']['object']['key'], encoding='utf-8')
        public_uuid = 'standalone-' + uuid.uuid4().hex
    else:
        # Coming from step function
        bucket = event['body']['bucket']
        key = event['body']['orig']
        public_uuid = event['body']['uuid']
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])

        print(key, re.sub('\.tif', '.jpg', key, flags=re.IGNORECASE))

        out_jpg_buffer = save_jpeg_to_target_size(
            key, response['Body'], 1000000, True, True)

        if out_jpg_buffer:
            out_key = re.sub('\.tif', '.jpg', key, flags=re.IGNORECASE).replace('raw', 'web')

            # Change final part of key to uuid, keeping other "folders"
            randomized_out_key = str(PurePath(out_key).with_name(public_uuid + '.jpg'))

            # Upload resized image to destination bucket
            s3.put_object(
                Body=out_jpg_buffer,
                Bucket=bucket,
                Key=randomized_out_key,
                StorageClass='GLACIER_IR',
                ContentType='image/jpeg',
                ACL='public-read'
            )

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    return {
        "statusCode": 200,
        "body": {
            "message": "hello world",
            "bucket": bucket,
            "orig_img": key,
            "web_img": randomized_out_key,
            "uuid": public_uuid
            # "location": ip.text.replace("\n", "")
        }
    }
