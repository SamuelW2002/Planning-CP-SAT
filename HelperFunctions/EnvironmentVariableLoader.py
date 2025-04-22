import os

def get_filemaker_auth_token_from_env_reg():
    return os.environ.get('FILEMAKER_AUTH_TOKEN_REG')

def get_filemaker_username_from_env():
    return os.environ.get('ML_USER_FILEMAKER_USERNAME')

def get_filemaker_password_from_env():
    return os.environ.get('ML_USER_FILEMAKER_PASSWORD')

def get_mongodb_uri_from_env():
    return os.environ.get('MONGODB_URI')

def get_filemaker_auth_token_from_env_erp():
    return os.environ.get('FILEMAKER_AUTH_TOKEN_ERP')