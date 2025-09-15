# Desafio MLOps: Pipeline de Inteligência de Mercado

Pipeline automatizado para enriquecimento e consolidação de dados de empresas, com orquestração via Airflow, processamento PySpark e Feature Store em Redis. Projeto containerizado, testável e com CI/CD.

## Sumário

- [Visão Geral](#visão-geral)
- [Estrutura do Projeto](#estrutura-do-projeto)
- [Serviços e Tecnologias](#serviços-e-tecnologias)
- [Como Executar](#como-executar)
- [Testes](#testes)
- [Verificação no Redis](#verificação-no-redis)
- [Customização e Parâmetros](#customização-e-parâmetros)
- [CI/CD](#cicd)

---

## Visão Geral

O pipeline lê arquivos CSV de empresas, gera features agregadas por cidade usando PySpark e salva no Redis. A DAG do Airflow garante automação, monitoramento e robustez. O ambiente é totalmente containerizado.

## Estrutura do Projeto

```
├── dags/                # DAG do Airflow
├── src/features/        # Lógica de feature engineering
├── data/                # Dados de entrada (CSV)
├── tests/               # Testes unitários
├── config/              # Scripts de setup e entrypoint
├── Dockerfile.airflow   # Imagem do Airflow
├── Dockerfile.test      # Imagem para testes
├── docker-compose.yml   # Orquestração dos serviços
├── requirements.txt     # Dependências Python
```

## Serviços e Tecnologias

- **Airflow**: Orquestração do pipeline (DAG, parametrização, healthcheck do Redis)
- **PySpark**: Processamento escalável dos dados
- **Redis**: Feature Store simplificada (cidade como chave, features como hash)
- **RedisInsight**: UI para inspeção dos dados no Redis
- **Docker Compose**: Setup automatizado dos serviços
- **Testes**: Cobertura da lógica de features, sem dependência externa
- **CI/CD**: Pipeline GitHub Actions para linting e testes

## Como Executar

1. **Clone o repositório:**
   ```sh
   git clone https://github.com/cayoesn/Desafio_Neoway_MLOps.git
   cd Desafio_Neoway_MLOps
   ```
2. **Suba o ambiente:**

   ```sh
   docker-compose up -d
   ```

   Serviços iniciados:

   - Airflow: http://localhost:8080
   - Redis: porta 6379
   - RedisInsight: http://localhost:5540

3. **Acesse o Airflow:**

   - Usuário: `cayoesn`
   - Senha: `123456789`

4. **Execute a DAG:**
   - Ative e execute a DAG `pipeline_inteligencia_mercado` na interface do Airflow.

## Testes

Testes unitários estão em `tests/` e rodam automaticamente via Docker Compose. Para rodar manualmente:

```sh
docker-compose run --rm tests
```

Cobrem agregações, logging e escrita no Redis (mockado).

## Verificação no Redis

Após rodar a DAG, as features por cidade ficam salvas no Redis. Inspecione via RedisInsight (http://localhost:5540) ou CLI:

```sh
docker exec -it redis redis-cli
keys *
hgetall "sao_paulo"
```

Saída esperada:

```
1) "capital_social_total"
2) "195000.0"
3) "quantidade_empresas"
4) "3"
5) "capital_social_medio"
6) "65000.0"
```

## Customização e Parâmetros

Para processar outro CSV, passe o parâmetro na execução manual da DAG:

```
{
	"input_csv": "/opt/airflow/data/novas_empresas_2.csv"
}
```

Ou defina a variável `input_csv` no Airflow ou a env `INPUT_CSV`.

## CI/CD

Pipeline de integração contínua via GitHub Actions:

- Linting com flake8
- Testes unitários em container Docker
