import pandas as pd
from typing import Optional
from config.logging_config import logger
import os

class ManipulateData:

    def __init__(self, file_path_members: str, file_path_followups: str) -> None:
        self.file_path_members = file_path_members
        self.file_path_followups = file_path_followups
        self.df: Optional[pd.DataFrame] = None

    def transform_to_df(self) -> Optional[pd.DataFrame]:
        """Lê os dois arquivos e faz o merge em um único DataFrame"""
        try:
            # Verificar se os arquivos existem
            if not os.path.exists(self.file_path_members):
                raise FileNotFoundError(f'O arquivo {self.file_path_members} não foi encontrado.')
            if not os.path.exists(self.file_path_followups):
                raise FileNotFoundError(f'O arquivo {self.file_path_followups} não foi encontrado.')

            # Lê os dois arquivos Excel
            df1 = pd.read_excel(self.file_path_members, usecols=['Nome', 'Telefone', 'Celula'], engine='openpyxl')
            df2 = pd.read_excel(self.file_path_followups, usecols=['Cliente', 'Tipo'], engine='openpyxl')

            # Log de sucesso no carregamento
            logger.info(f'Arquivos {self.file_path_members} e {self.file_path_followups} carregados com sucesso!')

            # Tratar os dados da coluna Celular
            df1['Celular'] = df1.apply(lambda row: row['Celula'] if pd.notna(row['Celula']) else row['Telefone'], axis=1)
            df1['Celular'] = df1['Celular'].str.replace(r'[^\d]', '', regex=True)  # Remove qualquer caractere não numérico

            # Renomear as colunas
            df2 = df2.rename(columns={'Cliente': 'Nome'})

            # Mescla os dois DataFrames
            self.df = pd.merge(df1, df2, how='inner', on='Nome')

            # Filtra as visitas e seleciona as colunas desejadas
            self.df = self.df[self.df['Tipo'] == 'Visita']
            self.df = self.df[['Nome', 'Celular', 'Tipo']]
            self.df = self.df.drop_duplicates()

            # Log de sucesso no merge
            logger.info('Merge realizado com sucesso!')
            return self.df

        except FileNotFoundError as fnf_error:
            logger.error(f'Erro: {fnf_error}')
            return None
        except Exception as e:
            logger.error(f'Erro ao tentar carregar ou realizar o merge: {e}')
            return None

    def save_merged_df(self) -> None:
        """Salva o DataFrame resultante em um arquivo Excel"""
        if self.df is not None:
            try:
                # Verificar se a pasta de destino existe, se não, cria a pasta
                output_dir = 'files/'
                os.makedirs(output_dir, exist_ok=True)  # Cria a pasta se não existir

                # Salva o DataFrame em um arquivo Excel
                file_path = os.path.join(output_dir, 'visitantes.xlsx')
                self.df.to_csv(file_path, index=False, sep=',')
                logger.info(f'Arquivo {file_path} salvo com sucesso!')

            except Exception as e:
                logger.error(f'Erro ao salvar o arquivo: {e}')
        else:
            logger.error('Nenhum DataFrame para salvar.')
