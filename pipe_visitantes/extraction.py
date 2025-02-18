from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import pandas as pd
import time

from config.logging_config import logger

class WebScraper:
    
    def __init__(self, url, email, senha):
        self.url = url
        self.email = email
        self.senha = senha

        options = Options()
        options.add_argument('--no-sandbox')
        options.add_argument('--disable-dev-shm-usage')
        options.add_argument('--disable-gpu')

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=options)
        self.driver.maximize_window()

        logger.info("Inicializando o WebScraper...")

    def login(self):
        try:
            logger.info('Acessando a página de login...')
            self.driver.get(self.url)
            time.sleep(2)

            logger.debug(f'URL carregada: {self.url}')

            logger.info('Preenchendo credenciais...')
            self.driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_txtEmail').send_keys(self.email)
            self.driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_txtPassword').send_keys(self.senha)
            self.driver.find_element(By.ID, 'ctl00_ContentPlaceHolder1_txtPassword').send_keys(Keys.RETURN)

            time.sleep(5)

            # Verifica se o login foi bem-sucedido
            if 'login.aspx' in self.driver.current_url:
                logger.error(f"Erro: Credenciais inválidas ou bloqueio de login. URL: {self.driver.current_url}")
                return False

            logger.info('Login realizado com sucesso!')
            return True

        except Exception as e:
            logger.error(f'Erro durante login: {e}', exc_info=True)
            return False

    def extract_members(self, data, folder_to_save):
        try:
            logger.info(f'Aguardando carregamento da página...')
            wait = WebDriverWait(self.driver, 10)

            # Espera o botão "Relatórios" estar visível e clica nele
            relatorio_field = wait.until(EC.element_to_be_clickable((By.ID, 'ctl00_liRelat')))
            relatorio_field.click()
            logger.info('Cliquei no menu "Relatórios"!')

            # Espera os campos de data estarem visíveis antes de interagir
            date_field1 = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_datac1')))
            date_field2 = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_datac2')))

            logger.debug(f'Datas encontradas: {date_field1}, {date_field2}')
            logger.info('Preenchendo as datas...')
            date_field1.clear()
            date_field1.send_keys(data)

            date_field2.clear()
            date_field2.send_keys(data)

            gerar_btn = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_btnGerar')))
            gerar_btn.click()

            logger.info('Relatório gerado com sucesso!')

            # Aguardar a tabela ser carregada
            table = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_gvExcel')))  # Atualizado para o novo ID

            # Extraindo os dados da tabela
            rows = table.find_elements(By.TAG_NAME, 'tr')  # Encontra todas as linhas da tabela
            data = []
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, 'td')  # Encontra todas as colunas da linha
                cols_text = [col.text for col in cols]  # Extrai o texto das colunas
                if cols_text:  # Evita adicionar linhas vazias
                    data.append(cols_text)

            logger.info(f'Tabela extraída com sucesso! {len(data)} linhas encontradas.')

            # Aqui você pode salvar os dados extraídos ou retorná-los
            # Exemplo: salvar os dados em um DataFrame do pandas
            df = pd.DataFrame(data, columns=['Nome', 'Telefone', 'Celula', 'Email','Endereço','Bairro', 'Cidade', 'Estado'])  # Substitua pelos nomes reais das colunas
            df.to_excel(folder_to_save, index=False)
            logger.info(f'Dados salvos em {folder_to_save}!')

        except Exception as e:
            logger.error(f'Erro ao extrair membros: {e}', exc_info=True)

    def extract_followups(self, data, folder_to_save):
        try:
            logger.info(f'Aguardando carregamento para geração da base de Follow-ups...')
            wait = WebDriverWait(self.driver, 15)

            # Preenche os campos de datas
            date_field1 = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_dataa1')))
            date_field2 = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_dataa2')))

            logger.info('Preenchendo as datas...')
            date_field1.clear()
            date_field1.send_keys(data)

            date_field2.clear()
            date_field2.send_keys(data)

            gerar_btn = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_btnGerarAtiv')))
            gerar_btn.click()

            logger.info('Relatório gerado com sucesso!')

            table = wait.until(EC.presence_of_element_located((By.ID, 'ctl00_ContentPlaceHolder1_gvExcel')))

            # Extraindo os dados da tabela
            rows = table.find_elements(By.TAG_NAME, 'tr')
            data = []
            for row in rows:
                cols = row.find_elements(By.TAG_NAME, 'td')
                cols_text = [col.text for col in cols]
                if cols_text: 
                    data.append(cols_text)

            logger.info(f'Tabela extraída com sucesso! {len(data)} linhas encontradas.')

            df = pd.DataFrame(data, columns=['Cliente', 'Data', 'Hora', 'Descrição','Tipo'])
            df.to_excel(folder_to_save, index=False)
            logger.info(f'Dados salvos em {folder_to_save}!')

        except Exception as e:
            logger.error(f'Erro ao extrair follow-ups: {e}', exc_info=True)


    def close_page(self):
        logger.info('Fechando o navegador...')
        self.driver.quit()
