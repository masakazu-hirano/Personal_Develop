import boto3
import io
import re

from botocore.config import Config
from pydub import AudioSegment

def authenticate_s3():
    return boto3.client(
        service_name = 's3',
        region_name = 'Region',
        aws_access_key_id = 'Access Key ID',
        aws_secret_access_key = 'Secret Access Key',
        config = Config(signature_version = 'v4')
    )

def authenticate_cloudflare():
    return boto3.client(
        service_name = 's3',
        endpoint_url = 'https://***.r2.cloudflarestorage.com',
        aws_access_key_id = 'Access Key ID',
        aws_secret_access_key = 'Secret Access Key',
        config = Config(signature_version = 'v4')
    )

if __name__ == '__main__':
    aws_s3_client = authenticate_s3()
    s3_object_list = aws_s3_client.list_objects_v2(
        Bucket = 'バケット名',
        Prefix = '',
        MaxKeys = 1000
    )['Contents']

    for object in s3_object_list:
        if object['Size'] != 0:
            response_s3_object = aws_s3_client.get_object(
                Bucket = 'バケット名',
                Key = object['Key'],
                IfMatch = object['ETag'],
                ResponseContentLanguage = 'ja',
                ResponseContentType = '',
                ResponseCacheControl = 'no-store',
            )['Body']

            import_audio_data = io.BytesIO(response_s3_object.read())
            audio_data_segment = AudioSegment.from_file(
                import_audio_data,
                format = '',
                start_second = None,
                channels = 2,
                frame_rate = 44100
            )

            export_audio_data = audio_data_segment.export(
                io.BytesIO(),
                format = '',
                tags = {'Artist': '', 'Album': ''}
            )

            cloudflare_client = authenticate_cloudflare()
            object_file_name = object['Key'].split('/')[1]
            file_name = re.sub(r'\D', '', object_file_name.split('.')[0])
            cloudflare_client.upload_fileobj(
                export_audio_data,
                'バケット名',
                f''
            )

            print(f'ファイルアップロード（Cloudflare）成功:')

            aws_s3_client.delete_object(
                Bucket = 'バケット名',
                Key = object['Key']
            )

            print(f"ファイル削除（S3）:")
            print('──────────')
    print('処理が正常終了しました。')
