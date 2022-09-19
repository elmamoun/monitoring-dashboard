# encoding=utf8
from datetime import datetime
import logging
import os
import sys

def chargerFileHandler(filePathName):
  fileHandler = logging.FileHandler(filePathName, 'a')
  formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s',datefmt='%a, %d %b %Y %H:%M:%S')
  fileHandler.setFormatter(formatter)
  return fileHandler


def chargerLogger(fileHandler,level="info"):
  logger = logging.getLogger()  # root logger
  logger.setLevel(logging.DEBUG) if level.lower() == "debug" else logger.setLevel(logging.INFO) if level.lower() == "info" else logger.setLevel(logging.ERROR)
  for hdlr in logger.handlers[:]:  # remove all old handlers
    print(hdlr.baseFilename)
    logger.removeHandler(hdlr)
  logger.addHandler(fileHandler)      # set the new handler
  return logger


def get_or_create_logger(log_path = "/home/cdsw/logs/unedic_data_pipeline" ,log_file_name_prefix = "fonctions_executees_sans_logger"):
  if not os.path.exists(log_path):
    os.makedirs(log_path, exist_ok=True)
  localLogFileName = datetime.now().strftime(log_path+'/'+log_file_name_prefix+'_%Y%m%d%H%M%S.log') if log_path != "/home/cdsw/logs/unedic_data_pipeline" \
                         else datetime.now().strftime(log_path+'/'+log_file_name_prefix+'_%Y%m%d.log')
  fileh = chargerFileHandler(localLogFileName)
  logger = chargerLogger(fileh)
  if os.stat(localLogFileName).st_size == 0 :
    logger.info(": ======================= "+log_file_name_prefix+" =======================")
    logger.info(": ================================ DEBUT =================================")
  return logger