import os
import glob
import pandas as pd
import json
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials
import shutil

from numpy.f2py.crackfortran import skipemptyends


def carregar_historico(caminho="historico.json"):
    """Carrega o histórico dos últimos IDs lançados."""
    if os.path.exists(caminho):
        with open(caminho, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def salvar_historico(historico, caminho="historico.json"):
    """Salva o histórico atualizado."""
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(historico, f, indent=4, ensure_ascii=False)

def processar_relatorios(pasta_relatorios, pasta_saida_shopee, pasta_saida_shein):
    """
    Processa os arquivos das plataformas Shopee (shp) e Shein (shein),
    salvando os resultados em diretórios diferentes.
    """
    # Lista os arquivos .xlsx no diretório
    arquivos_xlsx = [
        f for f in glob.glob(os.path.join(pasta_relatorios, "*.xlsx"))
        if not os.path.basename(f).startswith("~$") and "todos_lancamentos.xlsx" not in f
    ]

    if not arquivos_xlsx:
        print("Nenhum arquivo XLSX encontrado.")
        return

    df_shopee = pd.DataFrame()  #  armazenar os dados processados de Shopee
    df_shein = pd.DataFrame()  #  armazenar os dados mesclados de Shein
    historico = carregar_historico()

    # Colunas esperadas shp
    colunas_esperadas = ["ID do pedido", "Status do pedido", "Hora do pagamento do pedido"]

    for arquivo in arquivos_xlsx:
        # Dividir o nome do arquivo em partes para ver a plataforma
        nome_arquivo = os.path.basename(arquivo).split()
        if len(nome_arquivo) < 2:
            print(f"Aviso: O arquivo '{os.path.basename(arquivo)}' não possui o formato esperado. Pulando o arquivo.")
            continue

        nome_conta = nome_arquivo[0]
        plataforma = nome_arquivo[1].lower()
        print(f"\nProcessando arquivo: {arquivo} (Conta: {nome_conta}, Plataforma: {plataforma})")

        if plataforma == 'shp':
            df = pd.read_excel(arquivo)
            print(f"Total de linhas no arquivo: {len(df)}")

            colunas_disponiveis = list(df.columns)
            colunas_faltando = [col for col in colunas_esperadas if col not in colunas_disponiveis]

            if colunas_faltando:
                print(f"Colunas necessárias não encontradas no arquivo {arquivo}. Faltando: {colunas_faltando}")
                print(f"Colunas disponíveis: {colunas_disponiveis}")
                continue

            # Processar:
            df['ID do pedido'] = df['ID do pedido'].astype(str)
            df['Status do pedido'] = df['Status do pedido'].str.strip().str.lower()
            df = df[~df['Status do pedido'].isin(['não pago', 'cancelado'])]
            print(f"Linhas após remover não pagos e cancelados: {len(df)}")

            df = df[df['Hora do pagamento do pedido'].notna() & (df['Hora do pagamento do pedido'] != '-')]
            print(f"Linhas após remover valores vazios ou '-': {len(df)}")

            df['Hora do pagamento do pedido'] = pd.to_datetime(
                df['Hora do pagamento do pedido'], errors='coerce'
            )

            df = df.dropna(subset=['Hora do pagamento do pedido'])
            print(f"Linhas após remover valores inválidos de data: {len(df)}")

            df = df.sort_values(by=['Hora do pagamento do pedido'])
            print("\nExemplo de dados após ordenação:")
            print(df[['ID do pedido', 'Hora do pagamento do pedido']].head())

            ultimo_id = historico.get(nome_conta)
            if ultimo_id:
                ultima_data = df.loc[df['ID do pedido'] == ultimo_id, 'Hora do pagamento do pedido'].max()
                if pd.notna(ultima_data):
                    df = df[df['Hora do pagamento do pedido'] > ultima_data]
                else:
                    print(f"Último ID registrado ({ultimo_id}) não encontrado no arquivo. Pulando filtragem.")
            print(f"Linhas após filtrar IDs mais recentes que {ultimo_id}: {len(df)}")

            if not df.empty:
                ultimo_id_novo = df['ID do pedido'].iloc[-1]
                historico[nome_conta] = ultimo_id_novo
                print(f"Último ID processado para {nome_conta}: {ultimo_id_novo}")

            if 'Conta' not in df.columns:
                df.insert(0, 'Conta', nome_conta)

            df_shopee = pd.concat([df_shopee, df], ignore_index=True)

        elif plataforma == 'shein':  # Arquivos da Shein
            print(f"Adicionando arquivo Shein para mesclagem: {arquivo}")
            df_shein_tmp = pd.read_excel(arquivo, skiprows=1)
            nome_conta = os.path.basename(arquivo).split(' ')[0]
            df_shein_tmp.insert(0, "Conta", nome_conta)
            df_shein = pd.concat([df_shein, df_shein_tmp], ignore_index=True)

    if not df_shopee.empty:
        print(f"\nSalvando dados processados da Shopee em: {pasta_saida_shopee}")
        df_shopee.to_excel(pasta_saida_shopee, index=False)
        salvar_historico(historico)
    else:
        print("Nenhum dado válido para Shopee foi encontrado.")

    if not df_shein.empty:
        print(f"\nSalvando dados mesclados da Shein em: {pasta_saida_shein}")
        df_shein.to_excel(pasta_saida_shein, index=False)
    else:
        print("Nenhum dado válido para Shein foi encontrado.")

    print("Processamento concluído!")
    return df_shopee, df_shein


def encontrar_proxima_posicao(aba):
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets().values()

    RANGE = f'{aba}!A:A'
    result = sheet.get(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
    valores = result.get('values', [])

    coluna_A = [row[0] if row else '' for row in valores]  

    for i in range(len(coluna_A) - 2):
        if coluna_A[i] != '' and coluna_A[i + 1] == '' and coluna_A[i + 2] == '':
            return i + 2  # Retorna a primeira linha vazia onde tem duas linhas vazias

    return len(coluna_A) + 1  # Se não encontrou, vai pra próxima linha disponível

def enviar_para_google_sheets(aba, excel_file):
    try:
        linha_inicio = encontrar_proxima_posicao(aba)

        creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
        service = build('sheets', 'v4', credentials=creds)
        sheet = service.spreadsheets()

        available_sheets = pd.ExcelFile(excel_file).sheet_names
        if EXCEL_SHEET not in available_sheets:
            raise ValueError(f"A aba '{EXCEL_SHEET}' não foi encontrada no arquivo Excel. "
                             f"Abas disponíveis: {available_sheets}")

        df = pd.read_excel(excel_file, sheet_name=EXCEL_SHEET, engine='openpyxl')
        df = df.astype(str)
        data = df.values.tolist()

        RANGE = f'{aba}!A{linha_inicio}'
        body = {'values': data}
        sheet.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()

        print(f'Dados enviados para a aba "{aba}" com sucesso na linha {linha_inicio}!')

    except FileNotFoundError:
        print(f"Erro: O arquivo '{excel_file}' não foi encontrado no diretório.")
    except ValueError as e:
        print(f"Erro: {e}")
    except Exception as e:
        print(f"Ocorreu um erro inesperado: {e}")

def processar_abas():
    arquivos = {
        'SHOPEE': r"C:\Users\Thiag\Downloads\todos_lancamentos_shopee.xlsx",
        'SHEIN': r"C:\Users\Thiag\Downloads\todos_lancamentos_shein.xlsx"
    }

    for aba, arquivo in arquivos.items():
        enviar_para_google_sheets(aba, arquivo)


SERVICE_ACCOUNT_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1VdKtyxP-pj92DcCwaXItfeoV-_6VJF8hiy9pEb54RfA'
EXCEL_SHEET = 'Sheet1'
while True:
    opcao = str(input('Aperte Enter para Lançar Vendas'))

    pasta_relatorios = r"C:\Users\Thiag\Downloads"
    shopee_xlsx = r"C:\Users\Thiag\Downloads\todos_lancamentos_shopee.xlsx"
    shein_xlsx = r"C:\Users\Thiag\Downloads\todos_lancamentos_shein.xlsx"
    processar_relatorios(pasta_relatorios, shopee_xlsx, shein_xlsx)
    processar_abas()
