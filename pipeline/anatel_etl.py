"""
ETL — Anatel → Supabase
Serviços: SMP (Telefonia Móvel) + SCM (Banda Larga Fixa)
Janela:   2020–2025

Uso:
    python pipeline/anatel_etl.py --step all
    python pipeline/anatel_etl.py --step municipios
    python pipeline/anatel_etl.py --step movel
    python pipeline/anatel_etl.py --step banda_larga
"""

import os, sys, argparse, logging, requests, zipfile, io, psycopg2
import psycopg2.extras
from datetime import datetime, date
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client, Client
from tqdm import tqdm

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler("etl.log", encoding="utf-8")],
)
log = logging.getLogger(__name__)

ANO_INICIO = int(os.getenv("ANO_INICIO_OVERRIDE") or 2020)
ANO_FIM    = int(os.getenv("ANO_FIM_OVERRIDE")    or 2025)

BASE_URL   = "https://www.anatel.gov.br/dadosabertos/PDA/Acessos"
URL_SMP = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_telefonia_movel.zip"
URL_SCM = "https://www.anatel.gov.br/dadosabertos/paineis_de_dados/acessos/acessos_banda_larga_fixa.zip"
URL_IBGE   = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios?orderBy=nome"

MAPA_OPERADORAS = {
    "CLARO": "Claro", "TELEFONICA": "Vivo", "TELEFÔNICA": "Vivo", "VIVO": "Vivo",
    "TIM": "TIM", "OI": "Oi", "SKY": "SKY",
}

def normalizar_operadora(nome):
    if not isinstance(nome, str):
        return "Outras"
    n = nome.strip().upper()
    for k, v in MAPA_OPERADORAS.items():
        if k in n:
            return v
    return "Outras"

def get_supabase():
    url = os.getenv("SUPABASE_URL")
    key = os.getenv("SUPABASE_KEY")
    if not url or not key:
        raise EnvironmentError("Defina SUPABASE_URL e SUPABASE_KEY no .env")
    return create_client(url, key)

def baixar_zip(fonte, chunksize=100000):
    log.info(f"Lendo: {fonte}")
    if fonte.startswith("http"):
        r = requests.get(fonte, timeout=300)
        r.raise_for_status()
        conteudo = io.BytesIO(r.content)
    else:
        conteudo = open(fonte, "rb")

    dfs = []
    with zipfile.ZipFile(conteudo) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        log.info(f"  {len(csvs)} CSV(s) encontrado(s)")
        for nome in csvs:
            # Pula arquivos claramente fora da janela temporal pelo nome
            anos_no_nome = [str(a) for a in range(ANO_INICIO, ANO_FIM + 1)]
            if not any(a in nome for a in anos_no_nome) and any(
                str(a) in nome for a in range(2005, ANO_INICIO)
            ):
                log.info(f"    Pulando {nome} (fora da janela)")
                continue
            with z.open(nome) as f:
                try:
                    chunks = []
                    for chunk in pd.read_csv(
                        f, sep=";", encoding="utf-8-sig",
                        dtype=str, low_memory=False,
                        chunksize=chunksize
                    ):
                        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]
                        if "ano" in chunk.columns:
                            chunk = chunk[pd.to_numeric(chunk["ano"], errors="coerce").between(ANO_INICIO, ANO_FIM)]
                        if not chunk.empty:
                            chunks.append(chunk)
                    if chunks:
                        df = pd.concat(chunks, ignore_index=True)
                        dfs.append(df)
                        log.info(f"    {nome}: {len(df):,} linhas")
                except Exception as e:
                    log.warning(f"    Erro {nome}: {e}")
    return dfs

# ── NOVA FUNÇÃO: gerador que processa o ZIP chunk por chunk sem acumular na memória ──
def iterar_zip(fonte, chunksize=50000):
    """
    Em vez de carregar o CSV inteiro na memória (como o baixar_zip fazia),
    esta função GERA um chunk de cada vez — processa 50k linhas, libera,
    processa as próximas 50k, e assim por diante.
    """
    # Abre o arquivo local ou baixa da URL
    conteudo = open(fonte, "rb") if not fonte.startswith("http") else io.BytesIO(
        requests.get(fonte, timeout=300).content
    )

    with zipfile.ZipFile(conteudo) as z:
        csvs = [n for n in z.namelist() if n.lower().endswith(".csv")]
        log.info(f"  {len(csvs)} CSV(s) encontrado(s)")

        for nome in csvs:
            # Usa apenas arquivos _Colunas que têm código IBGE
            if "_colunas" not in nome.lower():
                log.info(f"    Pulando {nome} (sem código IBGE)")
                continue

            # Pula arquivos fora da janela temporal pelo nome
            anos_validos = [str(a) for a in range(ANO_INICIO, ANO_FIM + 1)]
            if not any(a in nome for a in anos_validos) and any(
                str(a) in nome for a in range(2005, ANO_INICIO)
            ):
                log.info(f"    Pulando {nome} (fora da janela)")
                continue

            log.info(f"    Processando {nome}...")
            with z.open(nome) as f:
                try:
                    for chunk in pd.read_csv(
                        f, sep=";", encoding="utf-8-sig",
                        dtype=str, low_memory=False,
                        chunksize=chunksize      # <-- lê 50k linhas por vez
                    ):
                        # Padroniza nomes das colunas
                        chunk.columns = [c.strip().lower().replace(" ", "_") for c in chunk.columns]

                        # Filtra pela janela temporal dentro do chunk
                        if "ano" in chunk.columns:
                            chunk = chunk[
                                pd.to_numeric(chunk["ano"], errors="coerce").between(ANO_INICIO, ANO_FIM)
                            ]

                        # Só gera o chunk se não estiver vazio após o filtro
                        if not chunk.empty:
                            yield nome, chunk   # <-- retorna o chunk para quem chamou

                except Exception as e:
                    log.warning(f"    Erro {nome}: {e}")

def registrar_log(sb, tabela, arquivo, data_ref, status, ok=0, err=0, msg=None):
    import psycopg2
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO anatel.etl_log 
        (tabela_destino, arquivo_origem, data_referencia, status, linhas_inseridas, linhas_erro, mensagem_erro)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (tabela, arquivo, data_ref, status, ok, err, msg))
    conn.commit()
    cur.close()
    conn.close()

def upsert_lotes(sb, tabela, registros, lote=500):
    import psycopg2.extras
    ok = err = 0
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()
    for i in tqdm(range(0, len(registros), lote), desc=f"  upsert {tabela}"):
        bloco = registros[i:i+lote]
        try:
            cols = bloco[0].keys()
            vals = [[r[c] for c in cols] for r in bloco]
            col_str = ", ".join(cols)
            placeholders = ", ".join(["%s"] * len(cols))
            conflict_col = list(cols)[0]
            query = f"""
                INSERT INTO anatel.{tabela} ({col_str})
                VALUES ({placeholders})
                ON CONFLICT DO NOTHING
            """
            psycopg2.extras.execute_batch(cur, query, vals)
            conn.commit()
            ok += len(bloco)
        except Exception as e:
            conn.rollback()
            log.error(f"  Lote {i}: {e}")
            err += len(bloco)
    cur.close()
    conn.close()
    return ok, err

# ── Steps ─────────────────────────────────────────────────────────────────────

def etl_municipios(sb):
    log.info("=" * 60)
    log.info("STEP: dim_municipios")
    dados = requests.get(URL_IBGE, timeout=60).json()
    CAPITAIS = {3550308,3304557,3106200,2927408,2304400,1302603,4106902,2611606,
                5300108,4314902,1501402,2800308,5208707,2111300,3205309,4205407,
                1200401,1600303,2900702,2408102,2211001,1721000,1100205,1400100,
                5002704,5103403,2507507}

    # ← função auxiliar declarada DENTRO de etl_municipios
    def extrair_uf(m):
        try:
            return m["microrregiao"]["mesorregiao"]["UF"]["sigla"]
        except (KeyError, TypeError):
            try:
                return m["regiao-imediata"]["regiao-intermediaria"]["UF"]["sigla"]
            except (KeyError, TypeError):
                return None

    registros = [{
        "cod_ibge":       int(m["id"]),
        "nome_municipio": m["nome"],
        "sigla_uf":       extrair_uf(m),
        "capital":        int(m["id"]) in CAPITAIS,
    } for m in dados]

    # Remove municípios sem UF resolvida
    registros = [r for r in registros if r["sigla_uf"] is not None]

    log.info(f"  {len(registros):,} municípios")
    ok, err = upsert_lotes(sb, "dim_municipios", registros)
    registrar_log(sb, "dim_municipios", URL_IBGE, None, "sucesso" if err==0 else "parcial", ok, err)
    log.info(f"  ✓ {ok:,} inseridos | ✗ {err:,} erros")
    ok, err = upsert_lotes(sb, "dim_municipios", registros)
    registrar_log(sb, "dim_municipios", URL_IBGE, None, "sucesso" if err==0 else "parcial", ok, err)
    log.info(f"  ✓ {ok:,} inseridos | ✗ {err:,} erros")

COLS_SMP = {
    "ano":"ano","mes":"mes","cod_municipio_ibge":"cod_ibge",
    "codigo_municipio_ibge":"cod_ibge","empresa":"_emp","prestadora":"_emp",
    "tecnologia":"_tec","natureza":"_nat","acessos":"acessos_total","quantidade":"acessos_total",
}

# ── TRANSFORMAÇÃO: normaliza o chunk do SMP para o schema do banco ──
def transformar_smp(df, ano):
    COLS = {
        "ano": "ano",
        "mês": "mes",
        "mes": "mes",
        "município": "_municipio_raw",
        "municipio": "_municipio_raw",
        "código_ibge_município": "cod_ibge",    # com acento
        "codigo_ibge_municipio": "cod_ibge",    # sem acento
        "cod_municipio_ibge": "cod_ibge",
        "empresa": "_emp",
        "grupo_econômico": "_grupo",            # com acento
        "grupo_economico": "_grupo",            # sem acento
        "tecnologia_geração": "_tec",           # com acento
        "tecnologia_geracao": "_tec",           # sem acento
        "tecnologia": "_tec",
        "modalidade_de_cobrança": "_nat",       # com acento
        "modalidade_de_cobranca": "_nat",       # sem acento
        "acessos": "acessos_total",
    }
    df = df.rename(columns={c: COLS.get(c, c) for c in df.columns})

    meses_cols = [c for c in df.columns if len(c) == 7 and c[4] == "-" and c[:4].isdigit()]
    if meses_cols and "cod_ibge" in df.columns:
        id_cols = list(df.columns[~df.columns.isin(meses_cols)])
        df = df.melt(
            id_vars=id_cols,
            value_vars=list(meses_cols),
            var_name="_ano_mes",
            value_name="acessos_total"
        )
        df["ano"] = df["_ano_mes"].str[:4].astype(int)
        df["mes"] = df["_ano_mes"].str[5:].astype(int)

    if "cod_ibge" not in df.columns:
        return pd.DataFrame()

    df["ano"] = pd.to_numeric(df.get("ano", ano), errors="coerce").fillna(ano).astype(int)
    df["mes"] = pd.to_numeric(df.get("mes", 1), errors="coerce").astype("Int64")
    df["cod_ibge"] = pd.to_numeric(df["cod_ibge"], errors="coerce").astype("Int64")
    df["acessos_total"] = pd.to_numeric(df.get("acessos_total", 0), errors="coerce").fillna(0).astype(int)
    df = df[df["ano"].between(ANO_INICIO, ANO_FIM)].dropna(subset=["cod_ibge", "mes"])

    if df.empty:
        return pd.DataFrame()

    df["data_referencia"] = pd.to_datetime(
        df["ano"].astype(str) + "-" + df["mes"].astype(str).str.zfill(2) + "-01", errors="coerce"
    ).dt.date

    df["_op"] = df["_emp"].apply(normalizar_operadora) if "_emp" in df.columns else "Outras"

    for col in ["acessos_2g", "acessos_3g", "acessos_4g", "acessos_5g", "acessos_prepago", "acessos_pospago"]:
        if col not in df.columns:
            df[col] = None

    if "_tec" in df.columns:
        t = df["_tec"].str.upper().fillna("")
        for g, c in [("2G", "acessos_2g"), ("3G", "acessos_3g"), ("4G", "acessos_4g"), ("5G", "acessos_5g")]:
            df.loc[t.str.contains(g), c] = df["acessos_total"]

    if "_nat" in df.columns:
        n = df["_nat"].str.upper().fillna("")
        df.loc[n.str.contains("PRÉ|PRE"), "acessos_prepago"] = df["acessos_total"]
        df.loc[n.str.contains("PÓS|POS"), "acessos_pospago"] = df["acessos_total"]

    return df

# ── FUNÇÃO ATUALIZADA: etl_movel agora processa chunk por chunk via iterar_zip ──
def etl_movel(sb):
    log.info("=" * 60)
    log.info("STEP: fato_movel (SMP)")

    # Busca o mapa de operadoras do banco
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()
    cur.execute("SELECT nome_operadora, id_operadora FROM anatel.dim_operadoras")
    mapa_op = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    conn.close()

    total_ok = total_err = 0

    # Define a fonte — arquivo local se existir, senão baixa da URL
    fonte_smp = "/tmp/smp.zip" if os.path.exists("/tmp/smp.zip") else URL_SMP
    log.info(f"Fonte: {fonte_smp}")

    try:
        # Itera chunk por chunk — nunca carrega tudo na memória
        for nome_csv, chunk in iterar_zip(fonte_smp):

            # Transforma o chunk (normaliza colunas, tipos, operadora, etc.)
            df = transformar_smp(chunk, ANO_INICIO)
            if df.empty:
                continue

            # Resolve FK da operadora
            df["id_operadora"] = df["_op"].map(mapa_op)
            df = df.dropna(subset=["id_operadora"])
            if df.empty:
                continue

            # Monta lista de dicts para upsert
            registros = [{
                "data_referencia":  row["data_referencia"].isoformat(),
                "ano":              int(row["ano"]),
                "mes":              int(row["mes"]),
                "id_operadora":     int(row["id_operadora"]),
                "cod_ibge":         int(row["cod_ibge"]),
                "acessos_total":    int(row["acessos_total"])   if pd.notna(row.get("acessos_total"))   else None,
                "acessos_prepago":  int(row["acessos_prepago"]) if pd.notna(row.get("acessos_prepago")) else None,
                "acessos_pospago":  int(row["acessos_pospago"]) if pd.notna(row.get("acessos_pospago")) else None,
                "acessos_2g":       int(row["acessos_2g"])      if pd.notna(row.get("acessos_2g"))      else None,
                "acessos_3g":       int(row["acessos_3g"])      if pd.notna(row.get("acessos_3g"))      else None,
                "acessos_4g":       int(row["acessos_4g"])      if pd.notna(row.get("acessos_4g"))      else None,
                "acessos_5g":       int(row["acessos_5g"])      if pd.notna(row.get("acessos_5g"))      else None,
                "fonte_arquivo":    nome_csv,
            } for _, row in df.iterrows()]

            # Upsert no Supabase
            ok, err = upsert_lotes(sb, "fato_movel", registros)
            total_ok += ok
            total_err += err

    except Exception as e:
        log.error(f"Erro no ETL móvel: {e}")
        registrar_log(sb, "fato_movel", fonte_smp, None, "erro", msg=str(e))

    # Registra resultado final
    registrar_log(sb, "fato_movel", fonte_smp, None,
                  "sucesso" if total_err == 0 else "parcial", total_ok, total_err)
    log.info(f"\n✓ fato_movel: {total_ok:,} inseridos | {total_err:,} erros")

COLS_SCM = {
    "ano":"ano","mes":"mes","cod_municipio_ibge":"cod_ibge","codigo_municipio_ibge":"cod_ibge",
    "empresa":"_emp","prestadora":"_emp","tecnologia":"tecnologia","meio_acesso":"tecnologia",
    "velocidade":"velocidade_contratada","faixa_velocidade":"velocidade_contratada",
    "acessos":"acessos_total","quantidade":"acessos_total",
}

def etl_banda_larga(sb):
    log.info("=" * 60)
    log.info("STEP: fato_banda_larga (SCM)")
    conn = psycopg2.connect(os.getenv("DB_URL"))
    cur = conn.cursor()
    cur.execute("SELECT nome_operadora, id_operadora FROM anatel.dim_operadoras")
    mapa_op = {row[0]: row[1] for row in cur.fetchall()}
    cur.close()
    conn.close()
    total_ok = total_err = 0

    log.info("Baixando SCM...")
    try:
        dfs = baixar_zip(URL_SCM)
    except requests.HTTPError as e:
        log.warning(f"  Erro ao baixar SCM: {e}")
        registrar_log(sb, "fato_banda_larga", URL_SCM, None, "erro", msg=str(e))
        return

    for df_raw in dfs:
        # ... resto do processamento permanece igual
            df = df.rename(columns={c: COLS_SCM.get(c, c) for c in df.columns})
            if "cod_ibge" not in df.columns:
                continue
            df["ano"] = pd.to_numeric(df.get("ano", ano), errors="coerce").fillna(ano).astype(int)
            df["mes"] = pd.to_numeric(df.get("mes", 1),   errors="coerce").astype("Int64")
            df["cod_ibge"] = pd.to_numeric(df["cod_ibge"], errors="coerce").astype("Int64")
            df["acessos_total"] = pd.to_numeric(df.get("acessos_total", 0), errors="coerce").fillna(0).astype(int)
            df = df[df["ano"].between(ANO_INICIO, ANO_FIM)].dropna(subset=["cod_ibge","mes"])
            df["data_referencia"] = pd.to_datetime(
                df["ano"].astype(str)+"-"+df["mes"].astype(str).str.zfill(2)+"-01", errors="coerce").dt.date
            df["_op"] = df.get("_emp", pd.Series(dtype=str)).apply(normalizar_operadora)
            if "tecnologia" in df.columns:
                t = df["tecnologia"].str.upper().fillna("")
                df["acessos_fibra"]    = df["acessos_total"].where(t.str.contains("FIBRA|FTT"), 0)
                df["acessos_cabo"]     = df["acessos_total"].where(t.str.contains("CABO|HFC"),  0)
                df["acessos_xdsl"]     = df["acessos_total"].where(t.str.contains("DSL"),       0)
                df["acessos_radio"]    = df["acessos_total"].where(t.str.contains("RÁDIO|RADIO|WI-FI|WIMAX"), 0)
                df["acessos_satelite"] = df["acessos_total"].where(t.str.contains("SATÉL|SATEL"), 0)
                df["tecnologia"] = df["tecnologia"].str.strip().str.title()
            else:
                for col in ["acessos_fibra","acessos_cabo","acessos_xdsl","acessos_radio","acessos_satelite"]:
                    df[col] = None
            if "velocidade_contratada" not in df.columns:
                df["velocidade_contratada"] = None
            df = df.groupby(
                ["data_referencia","ano","mes","_op","cod_ibge","tecnologia","velocidade_contratada"],
                as_index=False, dropna=False
            ).agg({"acessos_total":"sum","acessos_fibra":"sum","acessos_cabo":"sum",
                   "acessos_xdsl":"sum","acessos_radio":"sum","acessos_satelite":"sum"})
            df["id_operadora"] = df["_op"].map(mapa_op)
            df = df.dropna(subset=["id_operadora"])
            registros = [{
                "data_referencia": row["data_referencia"].isoformat(),
                "ano": int(row["ano"]), "mes": int(row["mes"]),
                "id_operadora": int(row["id_operadora"]), "cod_ibge": int(row["cod_ibge"]),
                "tecnologia": str(row["tecnologia"]) if pd.notna(row.get("tecnologia")) else None,
                "velocidade_contratada": str(row["velocidade_contratada"]) if pd.notna(row.get("velocidade_contratada")) else None,
                "acessos_total":    int(row["acessos_total"])    if pd.notna(row["acessos_total"])    else None,
                "acessos_fibra":    int(row["acessos_fibra"])    if pd.notna(row.get("acessos_fibra"))    else None,
                "acessos_cabo":     int(row["acessos_cabo"])     if pd.notna(row.get("acessos_cabo"))     else None,
                "acessos_xdsl":     int(row["acessos_xdsl"])     if pd.notna(row.get("acessos_xdsl"))     else None,
                "acessos_radio":    int(row["acessos_radio"])    if pd.notna(row.get("acessos_radio"))    else None,
                "acessos_satelite": int(row["acessos_satelite"]) if pd.notna(row.get("acessos_satelite")) else None,
                "fonte_arquivo": url,
            } for _, row in df.iterrows()]
            ok, err = upsert_lotes(sb, "fato_banda_larga", registros)
            total_ok += ok; total_err += err
            registrar_log(sb, "fato_banda_larga", url, df["data_referencia"].min() if not df.empty else None,
                          "sucesso" if err==0 else "parcial", ok, err)
    log.info(f"\n✓ fato_banda_larga: {total_ok:,} inseridos | {total_err:,} erros")

# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--step", choices=["municipios","movel","banda_larga","all"], default="all")
    args = parser.parse_args()
    sb = get_supabase()
    log.info(f"Conectado ao Supabase ✓ | step={args.step} | {ANO_INICIO}–{ANO_FIM}")
    inicio = datetime.now()
    if args.step in ("municipios","all"): etl_municipios(sb)
    if args.step in ("movel","all"):      etl_movel(sb)
    if args.step in ("banda_larga","all"):etl_banda_larga(sb)
    log.info(f"ETL finalizado em {datetime.now() - inicio}")

if __name__ == "__main__":
    main()
