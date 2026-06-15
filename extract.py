import requests
from typing import Dict, Any

class Extract:

    def __init__(self) -> None:
        self.urls = {
            "forca_trabalho": "https://servicodados.ibge.gov.br/api/v3/agregados/4093/periodos/-6/variaveis/4096?localidades=N1[all]|N3[all]&classificacao=2[all]",
            "desocupacao": "https://servicodados.ibge.gov.br/api/v3/agregados/4093/periodos/-6/variaveis/4099?localidades=N1[all]|N3[all]&classificacao=2[all]",
            "informalidade": "https://servicodados.ibge.gov.br/api/v3/agregados/4093/periodos/-6/variaveis/12466?localidades=N1[all]|N3[all]&classificacao=2[all]"
        }
        self.session = requests.Session()

    def extract_desocupacao(self) -> Dict[str, Any]:
        resultado = {}
        
        for nome_indicador, url in self.urls.items():
            response = self.session.get(url)
            response.raise_for_status() 
            resultado[nome_indicador] = response.json()
            
        return resultado