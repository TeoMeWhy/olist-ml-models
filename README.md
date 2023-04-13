# olist-ml-models

<img src="https://github.com/TeoMeWhy/olist-ml-models/blob/main/img/photo_2023-03-19_21-01-54.jpg?raw=true" width=450>

Projeto de Machine Learning do início ao fim no contexto de um e-commerce.

Este projeto é resultado de uma parceria entre o canal [Téo Me Why](https://www.twitch.tv/teomewhy) e o [Instituto Aaron Swartz](https://institutoasw.org/).

Se inscreva [aqui](https://forms.gle/bXEdjsjYWt3K9euC7) para receber o certificado de partipação: [Formulário Docs](https://forms.gle/bXEdjsjYWt3K9euC7)

## +12 horas de conteúdo gratuito sobre Machine Learning

Nosso objetivo será, a priori, criar um modelo de Machine Learning para ajudar o negócio da empresa Olist. Dentre as possibilidades temos:

1. Predição de Churn dos vendedores
2. Predição de ativação dos vendedores
3. Predição de atraso no pedido
4. Clustering de vendedores

## Índice
- [Como vamos nos organizar](#como-vamos-nos-organizar)
  - [Cronograma](#cronograma)
  - [Ementa](#ementa)
  - [Pré requisitos](#pre-requisitos)
- [Sober o Instituto Aaron Swartz](#sobre-o-instituto-aaron-swartz)
- [Sobre o instrutor](#sobre-o-instrutor)

## Como vamos nos organizar?

O projeto será 100% ao vivo na Twitch, canal [Téo Me Why](https://www.twitch.tv/teomewhy) de forma gratuita. Todo o desenvolvimento será realizado no Databricks, onde as pessoas `assinantes` do canal terão acesso a este Datalake para realizar seus próprios experimentos.

Passaremos por todas etapas do ciclo analítico, desde ETL das fontes de dados, criação de `feature store`, criação da `ABT` (_Analytical Base Table_), treinamento dos algoritmos, implementação do algoritmo campeão para novas predições. Utilizaremos ainda o `MLFlow` para gestão de nossos modelos.

### Cronograma

| Dia | Data/Hora | Tema | Link |
| :---: | :---: | --- | :---: |
| 1 | 03/04/23 20hrs BR | Introdução à ML + Definição do problema | [:link:](https://www.twitch.tv/videos/1784362772) |
| 2 | 04/04/23 20hrs BR | Brainstorm de variáveis + Criação Feature Store Pt. 1 | [:link:](https://www.twitch.tv/videos/1784988522) |
| 3 | 05/04/23 20hrs BR | Criação Feature Store Pt. 2 | [:link:](https://www.twitch.tv/videos/1785891887) |
| 4 | 06/04/23 20hrs BR | Criação das Safras | [:link:](https://www.twitch.tv/videos/1787450542) |
| 5 | 07/04/23 20hrs BR | Criação da ABT | [:link:](https://www.twitch.tv/videos/1789854320) |
| 6 | 10/04/23 20hrs BR | Teoria dos Algoritmos (Árvore e Regressão Linear e Logística) | [:link:](https://www.twitch.tv/videos/1790448831) |
| 7 | 11/04/23 20hrs BR | Métricas de ajuste | [:link:](https://www.twitch.tv/videos/1792052442) |
| 8 | 12/04/23 20hrs BR | Deploy com MLFlow | [:link:](https://www.twitch.tv/videos/1792476782) |

### Ementa

Todo material de apresentação está [disponível aqui](https://docs.google.com/presentation/d/1-1KM4gamVv7TBJ6DP6ZYOZRBmk1MfNUkIngBGc2JDBA/edit?usp=sharing).

#### Dia 1 - Introdução à ML + Definição do problema

No primeiro dia de curso, conheceremos o ciclo básico de desenvolvimento de um modelo (aplicação) de Machine Learning. Além disso, juntos, de forma colaborativa, definiremos qual será o problema de negócio que gostaríamos de resolver utilizando técnicas preditivas.

#### Dia 2 - Brainstorm de variáveis + Criação Feature Store Pt. 1

Com o problema bem definido, podemos discutir quais são as variáveis (características, atributos, etc) que ajudarão a prever o evento de interesse. isto é, qual conjunto de informações podemos criar para ajudar na solução de nosso problema. Ainda neste momento, as primeiras variáveis serão criadas em suas tabelas de `Feature Stores`.

#### Dia 3 - Criação Feature Store Pt. 2

Continuação da criação das variáveis relevantes para nosso estudo. É importante que ao final deste dia, todas as variáveis estejam devidamente construídas e disponíveis para consulta.

#### Dia 4 - Criação da ABT

Com todas as nossas variáveis criadas e disponíveis, temos condições de processar a nossa tabela definitiva para treinamento de uma algoritmo de Machine Learning. O nome desta tabela é `ABT - *Analytical Base Table*`, onde possui todas informações necessária para solução de nosso problema de negócios, i.e. features (variáveis, características, etc.) e target (variáveis resposta, alvo).

#### Dia 5 - Treinando algoritmos com MLflow

Chegou o momento de treinar nossos primeiros algoritmos de Machine Learning. Utilizaremos a biblioteca MLFlow para realizar a gestão do ciclo de vida de nossos modelos. Desta forma, conseguimos identificar a performance, métricas, parâmetros e variáveis de cada modelo, facilitando assim a tomada de decisão do modelo campeão.

#### Dia 6 - Escolhendo melhor algoritmo + Deploy

Ao definirmos o modelo campeão, podemos realizar novas predições e criar um novos script para fazer este processo de forma automática. Isto é, usar o nosso modelo para ajudar o negócio com novas possibilidades.

### Pre requisitos

Utilizaremos bastante SQL e Python. O nível básico de conhecimento nessas linguagens deve ser suficiente para acompanhar o curso. Durante as lives faremos questão de explicar a lógica do desenvolvimento e algumas sintaxes mais avançadas.

## Sobre o Instituto Aaron Swartz

Fazemos parte de um esforço global em que nossa estratégia de impacto social está diretamente alinhada com os Objetivos do Desenvolvimento Sustentável da ONU, contribuindo para o desenvolvimento sustentável reduzindo o gap gênero em TICs, fornecendo acesso à formação e tecnologias à pessoas de baixa renda e potencializando o trabalho coletivo em comunidades periféricas promovendo a cidadania ativa.

Dentre nossos objetivos, temos:
- Promover oportunidades de aprendizado sobre programação e o acesso à tecnologia e à informação;
- Elaborar, criar, implantar, executar projetos e programas voltados para educação, cultura do conhecimento e qualificação profissional;
- Fomentar o desenvolvimento de uma comunidade de interessados em inovação, ciência, cultura, tecnologia, criatividade, artes e disseminação do conhecimento;
- Promover e dar apoio ao uso de tecnologias e padrões que permitam seu livre uso, estudo, adaptação e compartilhamento, respeitando a autonomia individual e coletiva e incentivando a colaboração;
- Promover os ideais da ética hacker perante a comunidade e o poder público, esclarecendo desentendimentos acerca do termo;
- Promover o incentivo ao uso de tecnologia e inovação para a igualdade de gênero – ou seja, aumentar o uso de tecnologias de base, em particular as tecnologias de informação e comunicação, para promover o empoderamento das mulheres e segurança;

Conheça mais em: [institutoasw.org](https://institutoasw.org/)

## Sobre o instrutor

[Téo](https://www.linkedin.com/in/teocalvo/) é bacharel em Estatística pela FCT-UNESP e tem pós graduação em Big Data & Data Science pela UFPR. Hoje, é Sr. Head of Data na Gamers Club, tendo passado por diferentes empresas e indústrias sempre trabalhando com dados e `Data Science` desde 2014.

No cenário da educação, é criador do canal Téo Me Why na Twitch para divulgação de conteúdo de qualidade na área de dados e tecnologia de forma gratuita. Além disso, é professor na ASN.Rocks ministrando aulas de SQL, Python e Machine Learning (classificadores e regressores). Também é parceiro da [LinuxTips](https://www.linuxtips.io/) na criação de cursos. Recentemente tornou-se membro do Instituto Aaron Swartz como instrutor de tecnologia, onde firmou parceria junto ao seu canal, visando maior impacto social de seu conteúdo.

Nos apoie: [Apoia.se/teomewhy](http://apoia.se/teomewhy)
