# Desafio MLOps: Pipeline de Inteligência de Mercado na Neoway

Este repositório implementa um pipeline automatizado para enriquecer e consolidar dados brutos de novas empresas, gerando features agregadas por cidade e armazenando-as em uma Feature Store simplificada (Redis). O projeto é totalmente containerizado, testável e automatizado, conforme o desafio proposto pela Neoway.

## Sumário
- [Descrição da Solução](#descrição-da-solução)
- [Decisões de Design](#decisões-de-design)
- [Como Executar o Projeto](#como-executar-o-projeto)
- [Testes](#testes)
- [Verificando o Pipeline no Redis](#verificando-o-pipeline-no-redis)
- [Parâmetros e Customização](#parâmetros-e-customização)
- [CI/CD](#cicd)

---

## Descrição da Solução
O pipeline lê arquivos CSV de empresas recém-abertas, calcula features agregadas por cidade usando PySpark e armazena os resultados em um Redis. O processo é orquestrado por uma DAG do Airflow, garantindo automação, monitoramento e robustez.

## Decisões de Design
- **PySpark**: Usado para garantir escalabilidade e aderência ao ecossistema Big Data.
- **Redis**: Feature Store simplificada, com cada cidade como chave e features como hash.
- **Airflow**: Orquestração do pipeline, com health check do Redis, parametrização do arquivo de entrada e retries.
- **Docker Compose**: Facilita o setup do ambiente completo (Airflow, Redis, RedisInsight, testes).
- **Testes**: Cobrem toda a lógica de feature engineering, sem dependência de serviços externos.
- **CI/CD**: Pipeline no GitHub Actions para linting e testes automatizados.

## Como Executar o Projeto
1. **Clone o repositório:**
	```sh
	git clone https://github.com/cayoesn/Desafio_Neoway_MLOps.git
	cd Desafio_Neoway_MLOps
	```
2. **Suba o ambiente completo:**
	```sh
	docker-compose up --build
	```
	Isso irá iniciar:
	- Airflow (web UI em http://localhost:8080)
	- Redis (porta 6379)
	- RedisInsight (UI em http://localhost:5540)

3. **Acesse o Airflow:**
	- Usuário: `cayoesn`
	- Senha: `123456789`

4. **Execute a DAG**
	- No Airflow, ative e execute a DAG `pipeline_inteligencia_mercado`.

## Testes

Os testes unitários estão localizados na pasta `tests/` e são executados juntos no Docker Compose, mas caso queira rodar isoladamente, siga os passos abaixo.
```sh
docker-compose run --rm tests
```
Os testes cobrem toda a lógica de feature engineering, incluindo agregações, logging e escrita no Redis (mockado).

## Verificando o Pipeline no Redis
Após a execução da DAG, as features agregadas por cidade estarão salvas no Redis. Você pode inspecionar usando o RedisInsight (http://localhost:5540) ou via CLI:

```sh
docker exec -it redis redis-cli
keys *
hgetall "sao_paulo"
```
Exemplo de saída esperada:
```
1) "capital_social_total"
2) "195000.0"
3) "quantidade_empresas"
4) "3"
5) "capital_social_medio"
6) "65000.0"
```

## Parâmetros e Customização
Para processar outro arquivo CSV, basta passar o parâmetro na execução manual da DAG:
```
{
  "input_csv": "/opt/airflow/data/novas_empresas_2.csv"
}
```
Ou defina a variável `input_csv` no Airflow, ou a env `INPUT_CSV`.

## CI/CD
O projeto possui pipeline de integração contínua via GitHub Actions:
- Linting com flake8
- Testes unitários em container Docker