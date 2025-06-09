import datetime
import logging
from logging.handlers import RotatingFileHandler

from .utils import LOGS_DIR


class OnlyInfoFilter(logging.Filter):
    def filter(self, record):
        return record.levelno == logging.INFO


class MyLogger:

    def __init__(self, name: str, with_console: bool = False, with_file: bool = True):
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        # Gestionnaire pour l'affichage console
        if with_console:
            console_handler = logging.StreamHandler()
            console_handler.setFormatter(formatter)
            self.logger.addHandler(console_handler)
        if not with_console:
            self.logger.propagate = False
        # Gestionnaire pour l'enregistrement des logs dans un fichier
        if with_file:
            log_date = datetime.datetime.now().strftime("%Y_%m_%d")
            info_handler = RotatingFileHandler(
                f'{LOGS_DIR}/{name}_info_{log_date}.log',
                maxBytes=500 * 1024 * 1024,  # 500 Mo
                backupCount=5  # Nombre maximum de fichiers de sauvegarde
            )
            info_handler.setLevel(logging.INFO)
            info_handler.setFormatter(formatter)
            info_handler.addFilter(OnlyInfoFilter())
            self.logger.addHandler(info_handler)
            error_handler = RotatingFileHandler(
                f'{LOGS_DIR}/{name}_error_{log_date}.log',
                maxBytes=500 * 1024 * 1024,  # 500 Mo
                backupCount=5  # Nombre maximum de fichiers de sauvegarde
            )
            error_handler.setLevel(logging.ERROR)
            error_handler.setFormatter(formatter)
            self.logger.addHandler(error_handler)

    def info(self, message: str):
        self.logger.info(message)

    def error(self, message: str):
        self.logger.error(message)

    def warning(self, message: str):
        self.logger.warning(message)

    def debug(self, message: str):
        self.logger.debug(message)
