# Poder de compra da população Brasileira
Esse projeto tem como intuito estimar o poder de compra da população da região Sudeste do Brasil desde a implantação do Plano Real (1994) até o fechamento do ano de 2023. O estudo se dá através do cruzamento de informações do salário mínimo e do custo da cesta básica na região.

Abaixo temos um diagrama que representa o fluxo de ingestão, curadoria e refinamento dos dados.

```mermaid
flowchart LR
    subgraph ingestion
        subgraph "landing (s3)"
            landing_preco_cesta_basica[preco_cesta_basica.csv]
            landing_salario_minimo[salario_minimo.csv]
        end

        subgraph "raw"
            raw_tb_preco_cesta_basica[tb_preco_cesta_basica]
            raw_tb_salario_minimo[tb_salario_minimo]
        end
    end
    
    subgraph "trusted"
        trusted_tb_preco_cesta_basica[tb_preco_cesta_basica]
        trusted_tb_salario_minimo[tb_salario_minimo]
    end

    subgraph "refined"
        refined_tb_poder_compra[tb_poder_compra]
    end

    landing_preco_cesta_basica --> raw_tb_preco_cesta_basica
    landing_salario_minimo --> raw_tb_salario_minimo
    
    raw_tb_preco_cesta_basica --> trusted_tb_preco_cesta_basica
    raw_tb_salario_minimo --> trusted_tb_salario_minimo
    
    trusted_tb_preco_cesta_basica --> refined_tb_poder_compra
    trusted_tb_salario_minimo --> refined_tb_poder_compra
```

## Ferramentas e serviços
Abaixo estão listados as ferramentas e serviços utilizados no projeto.

- Docker
- PySpark
- Pytest

## Getting Started
Para executar os testes unitários, utilize o comando abaixo.

```bash
sh ./workspace/project/test.sh [pytest options]
```

Para mais informações sobre o pytest, confira a [documentação oficial](https://docs.pytest.org/en/8.0.x/).

Sinta-se à vontade para modificar o Dockerfile ou qualquer outro arquivo do projeto.