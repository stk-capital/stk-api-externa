import requests
import json
import os
import webbrowser
from urllib.parse import urljoin

def test_openapi():
    """
    Testa se a documentação OpenAPI está disponível.
    """
    # Definir a URL base (mudar para a URL da Vercel em produção)
    base_url = "http://localhost:8000"
    
    # Caminhos dos endpoints da documentação
    openapi_path = "/openapi.json"
    docs_path = "/docs"
    redoc_path = "/redoc"
    
    # Testar o endpoint OpenAPI
    try:
        response = requests.get(urljoin(base_url, openapi_path))
        if response.status_code == 200:
            print(f"✅ OpenAPI disponível em {urljoin(base_url, openapi_path)}")
            # Salvar o JSON para inspeção
            with open("openapi_response.json", "w") as f:
                json.dump(response.json(), f, indent=2)
            print("   Resposta salva em openapi_response.json")
        else:
            print(f"❌ OpenAPI não disponível: {response.status_code}")
    except Exception as e:
        print(f"❌ Erro ao acessar OpenAPI: {str(e)}")
    
    # Tentar abrir a documentação no navegador
    try:
        webbrowser.open(urljoin(base_url, docs_path))
        print(f"🌐 Abrindo a documentação Swagger UI em {urljoin(base_url, docs_path)}")
    except:
        print("❌ Não foi possível abrir o navegador")
    
    print("\nDicas para resolver problemas de documentação:")
    print("1. Verifique se a API está em execução na URL correta")
    print("2. Verifique as configurações CORS")
    print("3. Garanta que docs_url, redoc_url e openapi_url estão configurados corretamente")
    print("4. Na Vercel, verifique as variáveis de ambiente e a configuração vercel.json")

if __name__ == "__main__":
    test_openapi() 