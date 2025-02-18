from config.logging_config import logger
from datetime import datetime
import boto3
import os

class S3Uploader:
    def __init__(self, bucket_name: str, aws_region: str = 'us-east-1'):
        '''
        Inicializa a classe S3Uploader com o nome do bucket e a região da AWS.
        '''
        self.s3_client = boto3.client('s3', region_name=aws_region)
        self.bucket_name = bucket_name

    def upload_file(self, file_path: str, s3_prefix: str = '', new_file_name: str = None):
        """
        Faz o upload de um arquivo para o S3, organizando em pastas de ano/mês/dia.
        """
        now = datetime.now()
        timestamp = now.strftime('%Y%m%d_%H%M%S')
        date_prefix = f'{now.year}/{now.strftime("%m")}/{now.strftime("%d")}/'

        file_name = os.path.basename(file_path) if new_file_name is None else new_file_name
        s3_key = f'{s3_prefix}{date_prefix}{timestamp}_{file_name}'.replace('\\', '/')

        try:
            self.s3_client.upload_file(file_path, self.bucket_name, s3_key)
            logger.info(f'Upload concluído: {file_path} → s3://{self.bucket_name}/{s3_key}')
        except Exception as e:
            logger.error(f'Erro no upload: {e}')
