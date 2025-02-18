import pandas as pd
from typing import Optional
from config.logging_config import logger

class ManipulateData:

    def __init__(self, file_path_members: str, file_path_followups: str) -> None:
        self.file_path_members = file_path_members
        self.file_path_followups = file_path_followups
        self.df: Optional[pd.DataFrame] = None

    def transform_to_df(self) -> Optional[pd.DataFrame]:
        """Lê os dois arquivos e faz o merge em um único DataFrame"""
        try:
            # Lê os dois arquivos Excel
            df1 = pd.read_excel(self.file_path_members, usecols=['Nome', 'Telefone', 'Celula'],engine='openpyxl')
            df2 = pd.read_excel(self.file_path_followups, usecols=['Cliente', 'Tipo'], engine='openpyxl')

            # Log de sucesso no carregamento
            logger.info(f'Arquivos {self.file_path_members} e {self.file_path_followups} carregados com sucesso!')

            df1['Celular'] = df1.apply(lambda row: row['Celula'] if pd.notna(row['Celula']) else row['Telefone'], axis=1)
            df2 = df2.rename(columns={ 'Cliente': 'Nome'})

            self.df = pd.merge(df1, df2, how='inner', on='Nome')

            self.df = self.df[self.df['Tipo'] == 'Visita']
            self.df = self.df[['Nome', 'Celular', 'Tipo']]
            self.df = self.df.rename(columns={'Celula': 'Celular'})

            logger.info('Merge realizado com sucesso!')
            return self.df

        except Exception as e:
            logger.error(f'Erro ao tentar carregar ou realizar o merge: {e}')
            return None

    def save_merged_df(self) -> None:
        """Salva o DataFrame resultante em um arquivo Excel"""
        if self.df is not None:
            try:
                # Salva o DataFrame em um arquivo Excel
                self.df.to_excel('files/visitantes.xlsx', index=False)
                logger.info('Arquivo visitantes.xlsx salvo com sucesso!')

            except Exception as e:
                logger.error(f'Erro ao salvar o arquivo: {e}')
        else:
            logger.error('Nenhum DataFrame para salvar.')