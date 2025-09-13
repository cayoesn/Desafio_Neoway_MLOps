# Desafio_Neoway_MLOps
Repositório do desafio técnico da Neoway


comandos

rodar os testes na pasta raiz com o comando: docker-compose run --rm tests

Nos logs do pod airflow mostrara as credenciais do usuário admin, algo como:

standalone | Airflow is ready
standalone | Login with username: admin  password: nAFsy2aTvvGGR3sg


parametros para o airflow rodar com outros arquivos

{ "input_csv": "/opt/airflow/data/novas_empresas_2.csv" }