import json 
from pathlib import Path

default_path="setup_ini.json" #il percorso del file json di inizilizzazione dei path

class Setup:
    _config = {}

    def __init__(self, json_path: str=default_path):
        self.json_path = Path(__file__).parent / Path(default_path)  #uso il path di questo modulo altrimenti usa quello del caller
        self.load(self.json_path)

    def load(self, file_path):
        """Carica il file JSON e converte i dizionari in oggetti accessibili con dot notation."""
        with open(file_path, "r") as file:
            self.__class__._config = self._convert(json.load(file))

    @classmethod
    def _convert(cls, data):
        """Converte dizionari annidati in oggetti accessibili con dot notation."""
        if isinstance(data, dict):
            return type("ConfigObject", (object,), {k: cls._convert(v) for k, v in data.items()})
        elif isinstance(data, list):
            return [cls._convert(item) for item in data]
        return data

    def __getattr__(self, name):
        """Permette di accedere ai valori usando Setup.NomeChiave."""
        return getattr(self.__class__._config, name, None)



""" 
# Inizializzazione della configurazione
setup = Setup()
# Accesso ai valori
print(setup.Credentials.username)  # myuser
print(setup.Credentials.api_key)   # 123456789abcdef
print(setup.Paths.file_path)       # /home/user/myfile.txt
print(setup.debug)                 # True


{
    "servers": {
        "s1": {"host": "server1.com", "port": 8080},
        "s2": {"host": "server2.com", "port": 9090}
    }
}

server_num = "s3"  # Non esiste nel JSON

server = getattr(setup.servers, server_num, None)

if server:
    print(f"Connettendosi a {server.host} sulla porta {server.port}")
else:
    print(f"Errore: il server '{server_num}' non esiste nella configurazione!")


Elenchi misti con liste

{
    "Credentials": {
        "username": "myuser",
        "password": "mypassword"
    },
    "Paths": {
        "log_path": "/var/logs/app.log",
        "backup_paths": [
            "/backup/first.bak",
            "/backup/second.bak"
        ]
    }
}


setup.Credentials.username  # "myuser"
setup.Paths.log_path        # "/var/logs/app.log"
setup.Paths.backup_paths[0] # "/backup/first.bak"
setup.Paths.backup_paths[1] # "/backup/second.bak" 
"""
