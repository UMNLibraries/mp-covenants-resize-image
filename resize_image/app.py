import io
import sys
import json
import math
import urllib.parse
import boto3
from PIL import Image, ImageFont, ImageDraw

print('Loading function')

s3 = boto3.client('s3')


def save_jpeg_to_target_size(key: str, in_tiff_body: str, target_bytes: int, resize=False, resize_max=2000) -> str:
    """Save the image as JPEG with the given name at best quality that makes less than "target" bytes
    Source: https://stackoverflow.com/questions/52259476/how-to-reduce-a-jpeg-size-to-a-desired-size
    Args:
        in_tiff_body: The full path to the input raw or output TIF to convert
        target_bytes: The maximum file size. The output JPEG will be smaller than this value.
    Returns:
        out_jpg_path if save successful, or False
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

        im = im.resize((new_width, new_height), Image.ANTIALIAS)

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

    img = img.convert('RGBA')

    width, height = img.size

    watermark_text = 'UNOFFICIAL DOCUMENT'
    watermark_length = len(watermark_text)

    diagonal_length = int(math.sqrt((width * width)
                                    + (height * height))) * diagonal_percent

    font_size = int(diagonal_length / (watermark_length / font_ratio))
    font = ImageFont.truetype('fonts/Arial.ttf', font_size)

    opacity = int(256 * opacity_scalar)
    mark_width, mark_height = font.getsize(watermark_text)

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
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(
        event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        print("CONTENT TYPE: " + response['ContentType'])
        # return response['ContentType']

        print(key, key.replace('.tif', '.jpg'))

        out_jpg_buffer = save_jpeg_to_target_size(
            key, response['Body'], 1000000)
        if not out_jpg_buffer:
            # Try with resize
            out_jpg_buffer = save_jpeg_to_target_size(
                key, response['Body'], 1000000, True)

        if out_jpg_buffer:
            out_key = key.replace('.tif', '.jpg').replace('raw', 'web')

            # Upload resized image to destination bucket
            s3.put_object(Body=out_jpg_buffer, Bucket=bucket, Key=out_key)

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "hello world",
            # "location": ip.text.replace("\n", "")
        }),
    }