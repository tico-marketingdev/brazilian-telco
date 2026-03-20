# 📡 Brazilian Telco — Anatel Analytics

Pipeline ETL de dados abertos do setor de telecomunicações brasileiro.
Extrai dados da **Anatel**, transforma e carrega no **Supabase**, com análise em **R**.

## Estrutura
```
├── .github/workflows/etl_anatel.yml  ← Agendamento mensal automático
├── pipeline/anatel_etl.py            ← Script ETL (Extract → Transform → Load)
├── sql/anatel_schema.sql             ← Schema do banco (rodar uma vez no Supabase)
├── analysis/exploratory.Rmd          ← Notebook R de análise exploratória
├── requirements.txt
└── .env.example
```

## Setup rápido
```bash
pip install -r requirements.txt
cp .env.example .env   # preencher com credenciais do Supabase
```

## Executar
```bash
python pipeline/anatel_etl.py --step municipios   # 1. dimensões
python pipeline/anatel_etl.py --step movel        # 2. telefonia móvel
python pipeline/anatel_etl.py --step banda_larga  # 3. banda larga fixa
```

## GitHub Actions
Configure os Secrets no repositório:
- `SUPABASE_URL`
- `SUPABASE_KEY`

O pipeline roda automaticamente todo dia 1 do mês às 06:00 BRT.
