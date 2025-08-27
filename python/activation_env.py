# IMPORTANDO MÓDULOS E PACOTES
import pandas as pd
import numpy as np
import datetime as dt
import logging
import numpy as np
import os
import awswrangler as awr

xlsx_ontem = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\placas_movimentacoes_ontem.xlsx"
xlsx = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\placas_movimentacoes.xlsx"

df_ontem = pd.read_excel(xlsx_ontem, engine='openpyxl', sheet_name= 'ATIVAÇÕES')
df = pd.read_excel(xlsx, engine='openpyxl')
status_filter_list = ['CANCELADO', 'CANCELADA', 'FINALIZADO', 'FINALIZADA', 'NAO RENOVADO']

path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes"
dir_query = os.path.join(path,'sql', 'listagem_mestra.sql')

with open(dir_query, 'r') as file:
    query = file.read()

df_conferencia = awr.athena.read_sql_query(query, database='silver')

class ChassiProcessor:
    @staticmethod
    def process(df, df_ontem, df_conf):
        # Normalizar chassi em ambos os dataframes
        for d in (df, df_ontem):
            if 'chassi' in d.columns:
                d['chassi'] = d['chassi'].astype(str).str.strip().str.upper()

        # Conjuntos para comparação
        set_hoje = set(df['chassi'].dropna().unique()) if 'chassi' in df.columns else set()
        set_ontem = set(df_ontem['chassi'].dropna().unique()) if 'chassi' in df_ontem.columns else set()

        # Classificar status_beneficio em df
        if 'chassi' in df.columns:
            df['status_beneficio'] = np.where(df['chassi'].isin(set_ontem), 'ATIVO', 'NOVO')
        if 'migration_from' not in df.columns:
            df['migration_from'] = 'NULL'

        # Construir df_cancelados: existiam ontem e não existem hoje
        if 'chassi' in df_ontem.columns:
            cancelados_mask = ~df_ontem['chassi'].isin(set_hoje)
            df_cancelados = df_ontem.loc[cancelados_mask].copy()
        else:
            df_cancelados = pd.DataFrame(columns=df_ontem.columns)

        # Refinar NOVO usando df_conferencia (historico por chassi+beneficio)
        try:

            # Normalização de chaves
            for d in (df_conf):
                if 'chassi' in d.columns:
                    d['chassi'] = d['chassi'].astype(str).str.strip().str.upper()
                if 'beneficio' in d.columns:
                    d['beneficio'] = d['beneficio'].astype(str).str.strip().str.upper()
                if 'empresa' in d.columns:
                    d['empresa'] = d['empresa'].astype(str).str.strip()
            # Garantir datetime
            if 'data_ativacao_beneficio' in df_conf.columns:
                df_conf['data_ativacao_beneficio'] = pd.to_datetime(df_conf['data_ativacao_beneficio'], errors='coerce')

            # Aplicar apenas nas linhas marcadas como NOVO e que tenham chassi+beneficio
            mask_novo = df['status_beneficio'].eq('NOVO')
            if mask_novo.any() and {'chassi','beneficio'}.issubset(df.columns) and not df_conf.empty and {'chassi','beneficio','data_ativacao_beneficio','status_beneficio','empresa'}.issubset(df_conf.columns):
                df_novos = df.loc[mask_novo, ['chassi','beneficio','empresa']].copy()
                df_novos['beneficio'] = df_novos['beneficio'].astype(str).str.strip().str.upper()
                df_novos['empresa'] = df_novos['empresa'].astype(str).str.strip()

                # Contagem de registros por par
                hist_counts = (df_conf
                    .groupby(['chassi','beneficio'], as_index=False)
                    .size()
                    .rename(columns={'size':'hist_count'}))

                # Penúltimo registro por par: ordenar desc e pegar a 2a linha (index 1)
                df_conf_sorted = df_conf.sort_values('data_ativacao_beneficio', ascending=False)
                penult = (df_conf_sorted
                    .groupby(['chassi','beneficio'], as_index=False)
                    .nth(1)
                    .reset_index(drop=False))
                # Selecionar colunas relevantes
                penult = penult[['chassi','beneficio','status_beneficio','empresa']].rename(columns={
                    'status_beneficio':'status_penultimo',
                    'empresa':'empresa_penultima'
                })

                # Merge info de histórico e penúltimo nas linhas NOVO
                df_novos = df_novos.merge(hist_counts, on=['chassi','beneficio'], how='left')
                df_novos = df_novos.merge(penult, on=['chassi','beneficio'], how='left')

                # Regras:
                # - hist_count <= 1 -> permanece NOVO
                # - hist_count > 1:
                #   - status_penultimo in status_filter_list -> REATIVAÇÃO
                #   - else: se empresa_penultima != empresa_atual -> MIGRAÇÃO (migration_from = empresa_penultima)
                #           senão -> RENOVAÇÃO
                cond_hist_many = (df_novos['hist_count'].fillna(0) > 1)
                cond_penult_cancel = df_novos['status_penultimo'].isin(status_filter_list)
                cond_mudou_empresa = df_novos['empresa_penultima'].notna() & df_novos['empresa'].notna() & (df_novos['empresa_penultima'] != df_novos['empresa'])

                # Inicial
                df_novos['novo_status'] = 'NOVO'
                df_novos['novo_migration_from'] = 'NULL'

                # REATIVAÇÃO
                reativ_mask = cond_hist_many & cond_penult_cancel
                df_novos.loc[reativ_mask, 'novo_status'] = 'REATIVAÇÃO'
                df_novos.loc[reativ_mask, 'novo_migration_from'] = 'NULL'

                # MIGRAÇÃO
                migr_mask = cond_hist_many & ~cond_penult_cancel & cond_mudou_empresa
                df_novos.loc[migr_mask, 'novo_status'] = 'MIGRAÇÃO'
                df_novos.loc[migr_mask, 'novo_migration_from'] = df_novos.loc[migr_mask, 'empresa_penultima']

                # RENOVAÇÃO
                renov_mask = cond_hist_many & ~cond_penult_cancel & ~cond_mudou_empresa
                df_novos.loc[renov_mask, 'novo_status'] = 'RENOVAÇÃO'
                df_novos.loc[renov_mask, 'novo_migration_from'] = 'NULL'

                # Aplicar de volta no df
                df.loc[mask_novo, 'status_beneficio'] = df_novos['novo_status'].values
                df.loc[mask_novo, 'migration_from'] = df_novos['novo_migration_from'].values
        except Exception as e:
            logging.info(f"Falha ao refinar NOVO com histórico: {e}")

        return df, df_ontem, df_cancelados


ChassiProcessor.process(df, df_ontem, df_conferencia)

xlsx_path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\df_tratado.xlsx"
df_ativacoes_tratado = df.to_excel(xlsx_path, engine='openpyxl', index=False)

xlsx_cancel_path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\df_canceladas.xlsx"
df_ativacoes_tratado = df_cancelados.to_excel(xlsx_path, engine='openpyxl', index=False)
