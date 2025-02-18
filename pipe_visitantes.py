import os

from datetime import date
from dotenv import load_dotenv
from pathlib import Path

from config.logging_config import logger
from pipe_visitantes.extraction import WebScraper
from pipe_visitantes.transform import ManipulateData
from pipe_visitantes.load import S3Uploader

load_dotenv()

def extract_and_upload(scraper, uploader, data, members_path, followups_path):
    try:
        scraper.extract_members(data, members_path)
        scraper.extract_followups(data, followups_path)
        uploader.upload_file(members_path, 'pipe-visitantes-bronze/', 'membros-raw')
        uploader.upload_file(followups_path, 'pipe-visitantes-bronze/', 'followups-raw')
        logger.info('Extração e upload concluídos com sucesso.')
    except Exception as e:
        logger.error(f'Erro durante extração ou upload: {e}')

if __name__ == "__main__":
    try:
        URL = os.getenv('URL')
        EMAIL = os.getenv('EMAIL')
        SENHA = os.getenv('SENHA')
        FILE_PATH = os.getenv('FILE_PATH')

        # Valida se todas variaveis de ambiente foram carregadas
        if not all([URL, EMAIL, SENHA, FILE_PATH]):
            logger.error('Alguma variavel de ambiente nao foi carregada.')
            raise ValueError('Faltando variaveis de ambiente')

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

        # Extrai e carrega as bases
        if scraper.login():
            extract_and_upload(scraper, uploader_raw, '15/02/2025', file_path_members, file_path_followups)

        scraper.close_page()

        # Transformar e salvar o dataframe
        df = data_manipulator.transform_to_df()
        data_manipulator.save_merged_df()

        logger.info('Script executado com sucesso')
        logger.info('='*31)
    
    except TimeoutError as e:
        logger.error(f'Timeout na execução do script: {e}')
    except FileNotFoundError as e:
        logger.error(f'Arquivo não encontrado: {e}')
    except Exception as e:
        logger.error(f'Erro inesperado: {e}')
