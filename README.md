# Safra Duplicate Insights

Aplicação web moderna construída com Flask para identificar e visualizar duplicidades em resultados de consultas SQL ou tabelas.

## Recursos

- Interface responsiva com Bootstrap 5 inspirada na identidade visual do Banco Safra.
- Entrada flexível: informe apenas o nome da tabela (`flights`) ou uma consulta SQL iniciada em `SELECT`/`WITH`.
- Suporte a múltiplas colunas-chave para detecção de duplicidades.
- Visualização das linhas duplicadas ordenadas pelas chaves.
- Resumo visual das colunas com divergências entre os registros duplicados.
- Banco de dados SQLite com 100 registros de voos de exemplo.
- Estrutura preparada para futura integração com DataFrames Spark (PySpark).

## Estrutura do projeto

```
├── app.py
├── data
│   └── flights.db (gerado automaticamente na primeira execução)
├── requirements.txt
├── templates
│   ├── base.html
│   └── index.html
└── static
    └── css
        └── styles.css
```

## Pré-requisitos

- Python 3.10 ou superior
- Ambiente virtual recomendado (venv, conda, etc.)

## Instalação

```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\\Scripts\\activate
pip install -r requirements.txt
```

## Execução

```bash
export FLASK_APP=app.py
flask run --host 127.0.0.1 --port 5000
```

A base de dados `data/flights.db` será criada automaticamente com os 100 registros de exemplo quando a aplicação for executada pela primeira vez.

Acesse `http://127.0.0.1:5000` para utilizar a interface.

## Utilização

1. Informe o nome da tabela ou a consulta SQL no campo principal.
2. Defina uma ou mais colunas-chave separadas por vírgulas (ex.: `airline, flight_number`).
3. Clique em **Executar análise** para visualizar as duplicidades e o resumo das diferenças.

## Próximos passos

- Criar camada de serviços para aceitar DataFrames Spark (PySpark) mantendo a mesma lógica de análise.
- Exportar resultados em CSV ou Excel diretamente da interface.
- Adicionar autenticação e histórico de análises.

## Licença

Distribuído para fins educacionais. Ajuste conforme necessário para uso em produção.
