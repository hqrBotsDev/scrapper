from dotenv import dotenv_values

class Config():
    def __init__(self, ):
        self.config = dotenv_values(".env")

    def postgres(self):
        pg_dict = {
                'host': self.config['PG_HOST'],
                'port': self.config['PG_PORT'],
                'user': self.config['PG_USER'],
                'password': self.config['PG_PASS'],
                'dbname': self.config['PG_DATABASE']
                }
        return pg_dict

    def mongo(self):
        mongo_dict = {
                'host': self.config['MONGO_HOST'],
                'port': int(self.config['MONGO_PORT']),
                'user': self.config['MONGO_USER'],
                'password': self.config['MONGO_PASS'],
                'dbname': self.config['MONGO_DATABASE']
                }
        return mongo_dict

    def opensea_api(self):
        os_dict = {
                'OPENSEA_API_BASE_URL': self.config['OPENSEA_API_BASE_URL'],
                'OPENSEA_API_VERSION': self.config['OPENSEA_API_VERSION'],
                'OPENSEA_API_KEY': self.config['OPENSEA_API_KEY']
                }
        return os_dict

    def app(self):
        pg_dict = {
                'host': self.config['APP_HOST'],
                'port': self.config['APP_PORT'],
                'name': self.config['APP_NAME']
                }
        return pg_dict

    def etherscan(self):
        return self.config['ETHERSCAN_API_KEY']

    def app_host(self):
        return self.config['APP_HOST']

    def app_port(self):
        return self.config['APP_PORT']

    def app_name(self):
        return self.config['APP_NAME']

    def app_log_file(self):
        return self.config['APP_LOG_FILE']


