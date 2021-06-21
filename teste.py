import requests
from lxml import html

headers = {
    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36'
}

base = 'http://bvmf.bmfbovespa.com.br/pt-br/mercados/acoes/empresas/ExecutaAcaoConsultaInfoEmp.asp?CodCVM=25283'
# base_url = f'http://bvmf.bmfbovespa.com.br/cias-listadas/empresas-listadas/ResumoEmpresaPrincipal.aspx?codigoCvm=25283&idioma=pt-br'

page = requests.get(base, headers=headers)
tree = page.content
tree = html.fromstring(tree)

teste = tree.xpath('//div[contains(@id, "div1")]//h3/text()')[0]
print(teste)