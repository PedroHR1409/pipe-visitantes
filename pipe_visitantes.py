import os
from datetime import date, datetime
from dotenv import load_dotenv
from pathlib import Path
from config.logging_config import logger
from pipe_visitantes.extraction import WebScraper
from pipe_visitantes.transform import ManipulateData
from pipe_visitantes.load import S3Uploader

load_dotenv()

def extract_and_upload(scraper, uploader, data, members_path, followups_path):
    try:
        # Extrai dados e faz o upload dos arquivos brutos para o S3
        scraper.extract_members(data, members_path)
        scraper.extract_followups(data, followups_path)
        uploader.upload_file(members_path, 'pipe-visitantes-bronze/', 'membros-raw')
        uploader.upload_file(followups_path, 'pipe-visitantes-bronze/', 'followups-raw')
        logger.info('Extração e upload concluídos com sucesso.')
    except Exception as e:
        logger.error(f'Erro durante extração ou upload: {e}')

def upload_transformed_data(uploader, file_path):
    try:
        # Realiza o upload do arquivo transformado para o S3
        uploader.upload_file(file_path, 'pipe-visitantes-silver/', 'visitantes-silver.csv')
        logger.info('Arquivo transformado enviado para o S3 com sucesso.')
    except Exception as e:
        logger.error(f'Erro ao fazer upload do arquivo transformado: {e}')

def process_gold_data(df):
    try:
        # Adiciona a coluna 'data_cadastro' com a data atual
        df['data_cadastro'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        # Salva o arquivo transformado como CSV
        gold_file_path = 'files/visitantes_gold.csv'
        df.to_csv(gold_file_path, index=False)  # Salvando como CSV agora
        logger.info(f'Arquivo Gold salvo como {gold_file_path}')
        return gold_file_path
    except Exception as e:
        logger.error(f'Erro ao processar dados da camada Gold: {e}')
        return None

def upload_gold_data(uploader, file_path):
    try:
        if file_path:  # Verifica se o caminho do arquivo existe
            uploader.upload_file(file_path, 'pipe-visitantes-gold/', 'visitantes-gold.csv')
            logger.info('Arquivo Gold enviado para o S3 com sucesso.')
        else:
            logger.error('Arquivo Gold não foi processado corretamente.')
    except Exception as e:
        logger.error(f'Erro ao fazer upload do arquivo Gold: {e}')


if __name__ == "__main__":
    try:
        # Carregar variáveis de ambiente
        URL = os.getenv('URL')
        EMAIL = os.getenv('EMAIL')
        SENHA = os.getenv('SENHA')
        FILE_PATH = os.getenv('FILE_PATH')

        # Valida se todas variáveis de ambiente foram carregadas
        if not all([URL, EMAIL, SENHA, FILE_PATH]):
            logger.error('Alguma variável de ambiente não foi carregada.')
            raise ValueError('Faltando variáveis de ambiente')

        # Data atual formatada
        data_atual = date.today()
        data_formatada = data_atual.strftime('%d/%m/%Y')

        # Caminho do projeto
        project_path = Path.cwd() / FILE_PATH
        project_path.mkdir(parents=True, exist_ok=True)

        # Caminho dos arquivos
        file_path_members = project_path / 'RelatorioClientes.xlsx'
        file_path_followups = project_path / 'RelatorioFollowups.xlsx'
        
        # Inicializa classes
        scraper = WebScraper(URL, EMAIL, SENHA)
        data_manipulator = ManipulateData(file_path_members, file_path_followups)
        uploader_raw = S3Uploader('pipe-visitantes', 'us-east-1')
        uploader_transformed = S3Uploader('pipe-visitantes', 'us-east-1')

        # Extrai e carrega os dados brutos
        if scraper.login():
            extract_and_upload(scraper, uploader_raw, data_formatada, file_path_members, file_path_followups)

        scraper.close_page()

        # Transformar e salvar o dataframe
        df = data_manipulator.transform_to_df()
        data_manipulator.save_merged_df()

        # Agora que o arquivo foi salvo localmente, faz o upload para a camada Silver
        upload_transformed_data(uploader_transformed, 'files/visitantes.csv')

        # Processa e faz o upload para a camada Gold
        gold_file_path = process_gold_data(df)
        upload_gold_data(uploader_transformed, gold_file_path)

        logger.info('Script executado com sucesso')
        logger.info('='*31)

    except TimeoutError as e:
        logger.error(f'Timeout na execução do script: {e}')
    except FileNotFoundError as e:
        logger.error(f'Arquivo não encontrado: {e}')
    except Exception as e:
        logger.error(f'Erro inesperado: {e}')
