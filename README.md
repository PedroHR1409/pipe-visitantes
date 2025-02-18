# Pipe Visitantes

Este repositório contém um pipeline de dados para extrair, transformar e carregar (ETL) informações de visitantes. O projeto coleta dados de relatórios de clientes e follow-ups de uma página web e os processa para análise.

## Objetivo

O objetivo do projeto é automatizar a extração de dados de visitantes e follow-ups, realizar transformações nos dados e fazer o upload dos arquivos para um bucket no AWS S3. Além disso, é possível realizar uma análise combinando esses dados em um único DataFrame.

## Estrutura do Projeto

O pipeline é dividido nas seguintes etapas:

1. **Extração**: Usando o Selenium, os dados são extraídos de uma página web de relatórios. 
2. **Transformação**: Os dados extraídos são transformados em um formato útil, com a limpeza e união de informações.
3. **Carregamento**: Os dados transformados são enviados para um bucket S3 da AWS.


## Requisitos

Antes de rodar o projeto, instale as dependências utilizando o pip:

bash
pip install -r requirements.txt


As principais dependências são:

* **Selenium:** Para automação do navegador e extração dos dados da web.
* **Pandas:** Para manipulação e transformação dos dados.
* **Boto3:** Para interagir com a AWS S3.
* **Python-dotenv:** Para carregar variáveis de ambiente do arquivo .env.

## Variáveis de Ambiente

Este projeto requer um arquivo .env para armazenar variáveis de ambiente necessárias para a execução:

.env
URL='https://www.cadastrodeclientesonline.com.br/login.aspx'
EMAIL='seu_email@example.com'
SENHA='sua_senha'
FILE_PATH='files'


## Como Rodar o Projeto

1. **Configuração Inicial:** Certifique-se de que o arquivo .env esteja configurado com as variáveis de ambiente corretas.

2. **Executando o Pipeline:** O pipeline pode ser executado com o seguinte comando:

bash
python pipe-visitantes.py


Esse comando vai:

* Logar na página de relatórios.
* Extrair os dados de clientes e follow-ups.
* Realizar a transformação dos dados.
* Fazer o upload dos dados para o AWS S3.

3. **Visualizando os Resultados:** Após a execução, o arquivo visitantes.xlsx será gerado com os dados transformados e salvo na pasta files/.