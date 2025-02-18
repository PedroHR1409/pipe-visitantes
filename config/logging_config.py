import logging
import os

# Diretório onde os logs serão armazenados
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)  # Garante que a pasta de logs exista

# Configuração do logging
logging.basicConfig(
    filename=os.path.join(LOG_DIR, "log.app"),
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding="utf-8"
)

# Exemplo de como usar o logger
logger = logging.getLogger(__name__)
