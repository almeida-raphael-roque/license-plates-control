
import pandas as pd
import numpy as np
import datetime as dt
import logging
import os
import awswrangler as awr

# --------------------------------------------------------- EXTRACT

class ETL:

    def __init__(self) -> None:
        self.df = None
        self.df_ontem = None
        self.df_conferencia = None

    def extract(self):
        xlsx_ontem = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\placas_movimentacoes_ontem.xlsx"
        xlsx = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\placas_movimentacoes.xlsx"

        self.df_ontem = pd.read_excel(xlsx_ontem, engine='openpyxl', sheet_name='ATIVAÇÕES')
        self.df = pd.read_excel(xlsx, engine='openpyxl', sheet_name='ATIVAÇÕES')

        path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes"
        dir_query = os.path.join(path, 'sql', 'listagem_mestra.sql')

        with open(dir_query, 'r') as file:
            query = file.read()

        self.df_conferencia = awr.athena.read_sql_query(query, database='silver')

        return self.df, self.df_ontem

    def loading_deactived(self):
        # Construir df_desativados: existiam ontem e não existem hoje
        set_hoje = set(self.df['chassi'].dropna().unique()) if 'chassi' in self.df.columns else set()

        if 'chassi' in self.df_ontem.columns:
            desativados_mask = ~self.df_ontem['chassi'].isin(set_hoje)
            df_desativados = self.df_ontem.loc[desativados_mask].copy()
        else:
            df_desativados = pd.DataFrame(columns=self.df_ontem.columns)

        xlsx_cancel_path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\df_desativados.xlsx"
        df_desativados.to_excel(xlsx_cancel_path, engine='openpyxl', index=False)

    def transform_process(self):
        # Normalizar chassi em ambos os dataframes
        for d in (self.df, self.df_ontem):
            if 'chassi' in d.columns:
                d['chassi'] = d['chassi'].astype(str).str.strip().str.upper()

        # Conjuntos para comparação
        set_ontem = set(self.df_ontem['chassi'].dropna().unique()) if 'chassi' in self.df_ontem.columns else set()

        # Classificar status_beneficio em df
        if 'chassi' in self.df.columns:
            self.df['status_beneficio'] = np.where(self.df['chassi'].isin(set_ontem), 'ATIVO', 'NOVO')
        if 'migration_from' not in self.df.columns:
            self.df['migration_from'] = 'NULL'

        return self.df, self.df_ontem

    def transform_movement(self):
        df = self.df
        df_conf = self.df_conferencia

        # Normalização de chaves
        if df_conf is not None and not df_conf.empty:
            for col in ['chassi', 'beneficio', 'empresa']:
                if col in df_conf.columns:
                    if col == 'empresa':
                        df_conf[col] = df_conf[col].astype(str).str.strip()
                    else:
                        df_conf[col] = df_conf[col].astype(str).str.strip().str.upper()
            # Garantir datetime
            if 'data_ativacao_beneficio' in df_conf.columns:
                df_conf['data_ativacao_beneficio'] = pd.to_datetime(df_conf['data_ativacao_beneficio'], errors='coerce')

        # Aplicar apenas nas linhas marcadas como NOVO e que tenham chassi+beneficio
        mask_novo = df['status_beneficio'].eq('NOVO')
        if (
            mask_novo.any()
            and {'chassi', 'beneficio'}.issubset(df.columns)
            and df_conf is not None
            and not df_conf.empty
            and {'chassi', 'beneficio', 'data_ativacao_beneficio', 'status_beneficio', 'empresa'}.issubset(df_conf.columns)
        ):
            df_novos = df.loc[mask_novo, ['chassi', 'beneficio', 'empresa']].copy()
            df_novos['beneficio'] = df_novos['beneficio'].astype(str).str.strip().str.upper()
            df_novos['empresa'] = df_novos['empresa'].astype(str).str.strip()

            # Contagem de registros por par
            hist_counts = (
                df_conf
                .groupby(['chassi', 'beneficio'], as_index=False)
                .size()
                .rename(columns={'size': 'hist_count'})
            )

            # Penúltimo registro por par: ordenar desc e pegar a 2a linha (index 1)
            df_conf_sorted = df_conf.sort_values('data_ativacao_beneficio', ascending=False)
            penult = (
                df_conf_sorted
                .groupby(['chassi', 'beneficio'], as_index=False)
                .nth(1)
                .reset_index(drop=False)
            )
            # Selecionar colunas relevantes
            penult = penult[['chassi', 'beneficio', 'status_beneficio', 'empresa']].rename(columns={
                'status_beneficio': 'status_penultimo',
                'empresa': 'empresa_penultima'
            })

            # Merge info de histórico e penúltimo nas linhas NOVO
            df_novos = df_novos.merge(hist_counts, on=['chassi', 'beneficio'], how='left')
            df_novos = df_novos.merge(penult, on=['chassi', 'beneficio'], how='left')

            # Regras:
            # - hist_count <= 1 -> permanece NOVO
            # - hist_count > 1:
            #   - status_penultimo in status_filter_list -> REATIVAÇÃO
            #   - else: se empresa_penultima != empresa_atual -> MIGRAÇÃO (migration_from = empresa_penultima)
            #           senão -> RENOVAÇÃO
            status_filter_list = ['CANCELADO', 'CANCELADA', 'FINALIZADO', 'FINALIZADA', 'NAO RENOVADO']
            cond_hist_many = (df_novos['hist_count'].fillna(0) > 1)
            cond_penult_cancel = df_novos['status_penultimo'].isin(status_filter_list)
            cond_mudou_empresa = (
                df_novos['empresa_penultima'].notna()
                & df_novos['empresa'].notna()
                & (df_novos['empresa_penultima'] != df_novos['empresa'])
            )

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

        self.df = df
        return self.df

    def load(self):
        xlsx_path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\df_tratado.xlsx"
        self.df.to_excel(xlsx_path, engine='openpyxl', index=False)

if __name__ == "__main__":
    etl = ETL()
    etl.extract()
    etl.transform_process()
    etl.transform_movement()
    etl.loading_deactived()
    etl.load()

