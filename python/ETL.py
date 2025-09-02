
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
        self.df_cancelamentos = None
        self.today = pd.Timestamp.today().date()

        if self.today.weekday() == 0:
            self.yesterday = self.today - pd.Timedelta(days=3)
        else:
            self.yesterday = self.today - pd.Timedelta(days=1)

        if self.today.weekday() == 0:
            self.dbf_yesterday = self.today - pd.Timedelta(days=4)
        else:
            self.dbf_yesterday = self.today - pd.Timedelta(days=2)
    
    def extract(self):
        try:
            xlsx_ontem = rf"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\bkp_activation\placas_movimentacoes_{self.yesterday}.xlsx"
            xlsx = rf"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\bkp_activation\placas_movimentacoes_{self.today}.xlsx"
            

            self.df_ontem = pd.read_excel(xlsx_ontem, engine='openpyxl', sheet_name='ATIVAÇÕES')
            self.df = pd.read_excel(xlsx, engine='openpyxl', sheet_name='ATIVAÇÕES')

            path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes"
            dir_query = os.path.join(path, 'sql', 'listagem_mestra.sql')

            with open(dir_query, 'r') as file:
                query = file.read()

            self.df_conferencia = awr.athena.read_sql_query(query, database='silver')

            dir_query = os.path.join(path, 'sql', 'all_boards_CANCELADOS.sql')

            with open(dir_query, 'r') as file:
                query = file.read()

            self.df_cancelamentos = awr.athena.read_sql_query(query, database='silver')
            
            print("Extração concluída com sucesso!")
            return self.df, self.df_ontem
        except Exception as e:
            print(f"Erro na extração: {e}")
            return None, None

    def loading_deactived(self):
        try:
            set_hoje = set(self.df['chassi'].dropna().unique()) if 'chassi' in self.df.columns else set()

            if 'chassi' in self.df_ontem.columns:
                desativados_mask = ~self.df_ontem['chassi'].isin(set_hoje)
                df_desativados = self.df_ontem.loc[desativados_mask].copy()
            else:
                df_desativados = pd.DataFrame(columns=self.df_ontem.columns)

            xlsx_cancel_path = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Ativações Placas\placas_desativadas.xlsx"
            df_desativados.to_excel(xlsx_cancel_path, engine='openpyxl', index=False)
            print("Arquivo de placas desativadas gerado com sucesso!")
        except Exception as e:
            print(f"Erro ao gerar arquivo de placas desativadas: {e}")

    def transform_process(self):
        try:
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

            print("Transformação de processo concluída com sucesso!")
            return self.df, self.df_ontem
        except Exception as e:
            print(f"Erro na transformação de processo: {e}")
            return None, None

    def transform_movement(self):
        try:
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
            print("Transformação de movimentação concluída com sucesso!")
            return self.df
        except Exception as e:
            print(f"Erro na transformação de movimentação: {e}")
            return None

    def load(self):
        try:
            # Executar todo o pipeline se ainda não foi executado
            if self.df is None:
                print("Iniciando pipeline completo...")
                self.extract()
                self.loading_deactived()
                self.transform_process()
                self.transform_movement()
            
            # Criar diretórios se não existirem
            destination_dir = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Ativações Placas"
            destination_dir2 = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\bkp_activation"
            
            os.makedirs(destination_dir, exist_ok=True)
            os.makedirs(destination_dir2, exist_ok=True)

            file_path = rf"C:\Users\raphael.almeida\Documents\Processos\placas_acompanhamento\template\placas_movimentacoes_{self.today}.xlsx"

            destination_path = os.path.join(destination_dir, os.path.basename(file_path))
            destination_path2 = os.path.join(destination_dir2, os.path.basename(file_path))

            with pd.ExcelWriter(destination_path, engine='openpyxl') as writer:
                self.df.to_excel(writer, index=False, sheet_name='ATIVAÇÕES')
                self.df_cancelamentos.to_excel(writer, index=False, sheet_name='CANCELAMENTOS')

            with pd.ExcelWriter(destination_path2, engine='openpyxl') as writer:
                self.df.to_excel(writer, index=False, sheet_name='ATIVAÇÕES')
                self.df_cancelamentos.to_excel(writer, index=False, sheet_name='CANCELAMENTOS')

            xlsx_rm_path = rf"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Ativações Placas\placas_movimentacoes_{self.dbf_yesterday}.xlsx"
            if os.path.exists(xlsx_rm_path):
                os.remove(xlsx_rm_path)

            print("Pipeline completo executado com sucesso!")
        except Exception as e:
            print(f"Erro no carregamento: {e}")

if __name__ == "__main__":
    etl = ETL()
    etl.load()

