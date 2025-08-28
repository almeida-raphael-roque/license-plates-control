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
    def process(df, df_ontem):
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

        # Construir df_cancelados: existiam ontem e não existem hoje
        if 'chassi' in df_ontem.columns:
            cancelados_mask = ~df_ontem['chassi'].isin(set_hoje)
            df_cancelados = df_ontem.loc[cancelados_mask].copy()
        else:
            df_cancelados = pd.DataFrame(columns=df_ontem.columns)
        
        return df, df_ontem, df_cancelados