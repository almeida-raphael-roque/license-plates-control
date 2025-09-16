# EXTRACT 
import awswrangler as awr
import logging
import pandas as pd
import os
import numpy as np
import datetime as dt
import openpyxl

logging.basicConfig(
    level=logging.INFO,  # Exibe mensagens a partir de INFO
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.StreamHandler()  # Garante logs no console
    ]
)

logging.info('\n ----------------------------------------------------------------------------------')
logging.info('\n Executando Rotina: Movimentação de Placas')

class ETL_boards:

#EXTRACT
    def __init__(self):
        self.path = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes"
        self.today = pd.Timestamp.today().date()
        self.df_ativacoes = None
        self.df_cancelamentos = None
        self.df_conferencia = None
        self.df_final_ativacoes = None
        self.df_final_cancelamentos = None
        if self.today.weekday() == 0:  # 
            self.yesterday = self.today - pd.Timedelta(days=3)
        else:
            self.yesterday = self.today - pd.Timedelta(days=1)
       

    def extract_all_ativacoes(self):

        try:
            dir_query = os.path.join(self.path, 'sql', 'all_boards_ATIVOS.sql')
            with open(dir_query, 'r') as file:
                query = file.read()
            self.df_ativacoes = awr.athena.read_sql_query(query, database='silver')
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Consulta de ativações extraída com sucesso!')
            return self.df_ativacoes

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha ao extrair a consulta de ativações: {e}')
            return None

    def extract_all_cancelamentos(self):

        try:
            dir_query = os.path.join(self.path, 'sql', 'all_boards_CANCELADOS.sql')
            with open(dir_query, 'r') as file:
                query = file.read()
            self.df_cancelamentos = awr.athena.read_sql_query(query, database='silver')
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Consulta de cancelamentos extraída com sucesso!')
            return self.df_cancelamentos

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha ao extrair a consulta de cancelamentos: {e}')
            return None
    
    def extract_conf_boards(self):

        try:

            dir_query = os.path.join(self.path,'sql', 'listagem_mestra.sql')

            with open(dir_query, 'r') as file:
                query = file.read()

            self.df_conferencia = awr.athena.read_sql_query(query, database='silver')
        
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Relatorio conferência  - Dados Extraidos com sucesso!')

            return self.df_conferencia

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha ao Extrair relatorio conferência: {e}')

# TRANSFORM
    def board_status_treatment(self, df, df_conf, status_filter_list):

        try:
            if not df.empty:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(df.shape)
                row_count = 0
                for idx, row in df.iterrows():
                    row_count += 1
                    df_verification = df_conf[
                        (df_conf['chassi'] == row['chassi']) & (df_conf['beneficio'] == row['beneficio'])
                    ].sort_values(by='data_ativacao', ascending=True)

                    if not df_verification.empty and len(df_verification['empresa'].values) > 1:
                        hist_datas_ativacao = sorted(df_verification['data_ativacao_beneficio'].dropna().drop_duplicates().unique())

                        if len(hist_datas_ativacao) > 1:
                            penultimo_registro_data = hist_datas_ativacao[-2]
                            verification_penultima_row = df_verification.loc[df_verification['data_ativacao_beneficio'] == penultimo_registro_data]
                            
                            if verification_penultima_row['status_beneficio'].values[0] not in status_filter_list:
                                if verification_penultima_row['empresa'].values[0] != row['empresa']:
                                    df.at[idx, 'status_beneficio'] = 'MIGRAÇÃO'
                                    df.at[idx, 'migration_from'] = verification_penultima_row['empresa'].values[0]
                                else:
                                    df.at[idx, 'status_beneficio'] = 'RENOVAÇÃO'
                                    df.at[idx, 'migration_from'] = 'NULL'
                            else:
                                # today = dt.datetime.today()
                                # hist_datas_atualizacao = sorted(df_verification['data_atualizacao'].dropna().drop_duplicates().unique())
                                # penultimo_registro_data_atualizacao = hist_datas_atualizacao[-2]
                                # if today - penultimo_registro_data_atualizacao > dt.timedelta(days=30):
                                    df.at[idx, 'status_beneficio'] = 'REATIVAÇÃO'
                                    df.at[idx, 'migration_from'] = 'NULL'
                                    
                                # else:
                                #     if verification_penultima_row ['empresa'].values[0] != row['empresa']:
                                #         df.at[idx, 'status_beneficio'] = 'MIGRAÇÃO'
                                #         df.at[idx, 'migration_from'] = verification_penultima_row['empresa'].values[0]
                                #     else:
                                #         df.at[idx, 'status_beneficio'] = 'RENOVAÇÃO'
                                #         df.at[idx, 'migration_from'] = 'NULL'
                                    
                        else:
                            df.at[idx, 'status_beneficio'] = 'NOVO'
                            df.at[idx, 'migration_from'] = 'NULL'
                    else:
                        df.at[idx, 'status_beneficio'] = 'NOVO'
                        df.at[idx, 'migration_from'] = 'NULL'

                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Total de linhas processadas: {row_count}')

            else:
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info('Nnehum registro de ativações para tratamento de dados. Dataframe vazio!')


        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha no tratamento de status das placas ativadas. Revise o código: {e}')

        return df

    def transforming_files(self):

        #DEFININDO DATAFRAMES VAZIOS 
        try:

            df_final_ativacoes = pd.DataFrame()
            df_final_cancelamentos = pd.DataFrame()

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('Falha ao definir dataframes.')

        #TRANSFORMANDO DF_ATIV E SEGMENTANDO POR EMPRESA
        try:

            if self.df_cancelamentos is None or self.df_ativacoes is None:
                raise ValueError('DataFrames de extração não encontrados. Execute a extração antes da transformação.')

            df_final_cancelamentos = self.df_cancelamentos
            df_ativ_all_boards = self.df_ativacoes

            df_ativ_all_boards['data_ativacao_beneficio'] = pd.to_datetime(df_ativ_all_boards['data_ativacao_beneficio']).dt.date
                        
            df_ativ_all_boards['beneficio'] = df_ativ_all_boards['beneficio'].replace('REPARAÇÃO OU REPOSIÇÃO DO VEÍCULO', 'CASCO (VEÍCULO)').replace('REPARAÇÃO OU REPOSIÇÃO DO (SEMI)REBOQUE', 'CASCO (R/SR)').replace('REPARAÇÃO OU REPOSIÇÃO DO COMPLEMENTO', 'CASCO (COMPLEMENTO)')
            
            df_ativ_viavante = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Viavante']
            df_ativ_stcoop = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Stcoop']
            df_ativ_segtruck = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Segtruck']
            df_ativ_tag = df_ativ_all_boards[df_ativ_all_boards['empresa'] == 'Tag']

            df_final_cancelamentos = df_final_cancelamentos

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')  
            logging.info(f'Falha ao realizar a segmentação dos dataframes: {e}')

        # SELECIONANDO APENAS AS ATIVAÇÕES CORRESPONDENTES AOS BENEFICIOS 'CASCO' / 'TERCEIRO' POR UM REGEX PADRÃO
        try:
            ids_beneficios_segtruck = [2, 3, 4, 7, 24, 25, 26, 29]
            ids_beneficios_stcoop = [24, 25, 26, 29]
            ids_beneficios_viavante = [40, 41, 42, 45]
            ids_beneficios_tag = [2, 3, 4, 7, 24, 25, 26, 29, 34, 35, 36, 37, 38, 39]

            df_ativ_viavante = df_ativ_viavante.loc[df_ativ_viavante['id_beneficio'].isin(ids_beneficios_viavante)]
            df_ativ_stcoop = df_ativ_stcoop[df_ativ_stcoop['id_beneficio'].isin(ids_beneficios_stcoop)]
            df_ativ_segtruck = df_ativ_segtruck.loc[df_ativ_segtruck['id_beneficio'].isin(ids_beneficios_segtruck)]
            df_ativ_tag = df_ativ_tag.loc[df_ativ_tag['id_beneficio'].isin(ids_beneficios_tag)]

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')  
            logging.info(f'Falha ao padronizar nomenclaturas referente aos beneficios pré-estabelecidos: {e}')

        # CONCATENANDO E CRIANDO COLUNA DE MIGRAÇÃO (MIGRATION_FROM) 
        try:

            df_final_ativacoes = pd.concat([df_ativ_viavante, df_ativ_stcoop, df_ativ_segtruck, df_ativ_tag])

            if not df_final_ativacoes.empty:
                df_final_ativacoes['migration_from'] = np.nan

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha na criação da coluna de migração e concatenação de dataframes: {e}')

        # CRIANDO LISTA DE VERIFICAÇÃOST DE PLACAS MIGRADAS (STATUS)
        status_filter_list = ['CANCELADO', 'CANCELADA', 'FINALIZADO', 'FINALIZADA', 'NAO RENOVADO']
        
        try:
            # PEGANDO DADOS DE ATIVAÇÃO DO DIA ANTERIOR
            if not df_final_ativacoes.empty:
                df_ativos_sem_hoje_ontem = df_final_ativacoes[
                    ~df_final_ativacoes['data_ativacao_beneficio'].isin([self.today, self.yesterday])
                ]
                df_ativos_dia_anterior = df_final_ativacoes[df_final_ativacoes['data_ativacao_beneficio'] == self.yesterday]
                df_ativacoes_dia_anterior_ranking_tratado = self.board_status_treatment(df_ativos_dia_anterior, self.df_conferencia, status_filter_list)
                df_final_ativacoes = pd.concat([df_ativos_sem_hoje_ontem, df_ativacoes_dia_anterior_ranking_tratado])
                
                logging.info('\n ----------------------------------------------------------------------------------')
                logging.info(f'Número de registros ativos na carteira tratado com os dados do dia anterior.')

        except Exception as e:

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao incluir registros ativos referente ao dia anterior na contagem . Revise o código: {e}')


        # DEFININDO COLUNAS QUE SERÃO UTILIZADAS NO DATAFRAME FINAL
        try:
           
            df_final_ativacoes = df_final_ativacoes[[
                'placa', 'chassi', 'id_placa', 'id_veiculo', 'id_carroceria', 'matricula', 'conjunto', 'unidade', 'consultor', 'status_beneficio', 
                'cliente', 'data_registro', 'data_ativacao_beneficio', 'suporte', 'data_filtro', 'empresa', 'migration_from'
            ]]

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Processo de seleção de colunas realizado com sucesso!')

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao definir colunas: {e}')

        # RETIRANDO DUPLICATAS
        try:
            df_final_ativacoes = df_final_ativacoes.drop_duplicates(subset=['chassi'])

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Duplicatas retiradas com sucesso.')

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao retirar duplicatas. Revise o código: {e}')

        # TRATANDO DADOS NULOS NOS DATAFRAMES
        try:
            df_final_ativacoes['placa'] = df_final_ativacoes['placa'].fillna('SEM-PLACA')
            df_final_ativacoes['chassi'] = df_final_ativacoes['chassi'].fillna('NULL')
            df_final_ativacoes['id_placa'] = df_final_ativacoes['id_placa'].fillna(0)
            df_final_ativacoes['id_veiculo'] = df_final_ativacoes['id_veiculo'].fillna(0)
            df_final_ativacoes['id_carroceria'] = df_final_ativacoes['id_carroceria'].fillna(0)
            df_final_ativacoes['matricula'] = df_final_ativacoes['matricula'].fillna(0)
            df_final_ativacoes['conjunto'] = df_final_ativacoes['conjunto'].fillna(0)
            df_final_ativacoes['unidade'] = df_final_ativacoes['unidade'].fillna('NULL')
            df_final_ativacoes['consultor'] = df_final_ativacoes['consultor'].fillna('NULL')
            df_final_ativacoes['status_beneficio'] = df_final_ativacoes['status_beneficio'].fillna('NULL')
            df_final_ativacoes['cliente'] = df_final_ativacoes['cliente'].fillna('NULL')
            df_final_ativacoes['data_registro'] = df_final_ativacoes['data_registro'].fillna(pd.Timestamp('1900-01-01'))
            df_final_ativacoes['data_ativacao_beneficio'] = df_final_ativacoes['data_ativacao_beneficio'].fillna(pd.Timestamp('1900-01-01'))
            df_final_ativacoes['suporte'] = df_final_ativacoes['suporte'].fillna('NULL')
            df_final_ativacoes['data_filtro'] = df_final_ativacoes['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
            df_final_ativacoes['empresa'] = df_final_ativacoes['empresa'].fillna('NULL')
            df_final_ativacoes['migration_from'] = df_final_ativacoes['migration_from'].fillna('NULL')

            df_final_cancelamentos['placa'] = df_final_cancelamentos['placa'].fillna('SEM-PLACA')
            df_final_cancelamentos['chassi'] = df_final_cancelamentos['chassi'].fillna('NULL')
            df_final_cancelamentos['id_placa'] = df_final_cancelamentos['id_placa'].fillna(0)
            df_final_cancelamentos['id_veiculo'] = df_final_cancelamentos['id_veiculo'].fillna(0)
            df_final_cancelamentos['id_carroceria'] = df_final_cancelamentos['id_carroceria'].fillna(0)
            df_final_cancelamentos['matricula'] = df_final_cancelamentos['matricula'].fillna(0)
            df_final_cancelamentos['conjunto'] = df_final_cancelamentos['conjunto'].fillna(0)
            df_final_cancelamentos['unidade'] = df_final_cancelamentos['unidade'].fillna('NULL')
            df_final_cancelamentos['status'] = df_final_cancelamentos['status'].fillna('NULL')
            df_final_cancelamentos['cliente'] = df_final_cancelamentos['cliente'].fillna('NULL')
            df_final_cancelamentos['data'] = df_final_cancelamentos['data'].fillna(pd.Timestamp('1900-01-01'))
            df_final_cancelamentos['data_cancelamento'] = df_final_cancelamentos['data_cancelamento'].fillna(pd.Timestamp('1900-01-01'))
            df_final_cancelamentos['usuario_cancelamento'] = df_final_cancelamentos['usuario_cancelamento'].fillna('NULL')
            df_final_cancelamentos['data_filtro'] = df_final_cancelamentos['data_filtro'].fillna(pd.Timestamp('1900-01-01'))
            df_final_cancelamentos['empresa'] = df_final_cancelamentos['empresa'].fillna('NULL')
            df_final_cancelamentos['migracao'] = df_final_cancelamentos['migracao'].fillna('NULL')
            df_final_cancelamentos['renegociacao'] = df_final_cancelamentos['renegociacao'].fillna('NULL')
            df_final_cancelamentos['data_substituicao'] = df_final_cancelamentos['data_substituicao'].fillna(pd.Timestamp('1900-01-01'))

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Processo de Transformacao de Dados concluido com sucesso!')

        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'Falha ao realizar tratamento de dados: {e}')
     
        # salvar no estado do objeto para uso no LOAD
        self.df_final_ativacoes = df_final_ativacoes
        self.df_final_cancelamentos = df_final_cancelamentos

        return df_final_ativacoes, df_final_cancelamentos
    
# LOAD
    def loading_files(self):

        try:
            # garantir dados transformados no estado
            if self.df_final_ativacoes is None or self.df_final_cancelamentos is None:
                if self.df_ativacoes is None:
                    self.extract_all_ativacoes()
                if self.df_cancelamentos is None:
                    self.extract_all_cancelamentos()
                if self.df_conferencia is None:
                    self.extract_conf_boards()
                self.transforming_files()

            file_path = rf"C:\Users\raphael.almeida\Documents\Processos\placas_acompanhamento\template\placas_movimentacoes_{self.today}.xlsx"

            destination_dir = r"C:\Users\raphael.almeida\OneDrive - Grupo Unus\analise de dados - Arquivos em excel\Relatório de Ativações Placas"
            destination_path = os.path.join(destination_dir, os.path.basename(file_path))

            destination_dir2 = r"C:\Users\raphael.almeida\Documents\Processos\placas_movimentacoes\bkp_activation"
            destination_path2 = os.path.join(destination_dir2, os.path.basename(file_path))

            os.makedirs(destination_dir, exist_ok=True)
            os.makedirs(destination_dir2, exist_ok=True)

            with pd.ExcelWriter(destination_path, engine='openpyxl') as writer:
                self.df_final_ativacoes.to_excel(writer, index=False, sheet_name='ATIVAÇÕES')
                self.df_final_cancelamentos.to_excel(writer, index=False, sheet_name='CANCELAMENTOS')

            with pd.ExcelWriter(destination_path2, engine='openpyxl') as writer:
                self.df_final_ativacoes.to_excel(writer, index=False, sheet_name='ATIVAÇÕES')
                self.df_final_cancelamentos.to_excel(writer, index=False, sheet_name='CANCELAMENTOS')

            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info('\n Processo de Carregamento de Dados concluido com sucesso!')
        except Exception as e:
            logging.info('\n ----------------------------------------------------------------------------------')
            logging.info(f'\n Falha no processo de carregamento: {e}')

if __name__ == '__main__':
    etl = ETL_boards()
    etl.loading_files()


    
