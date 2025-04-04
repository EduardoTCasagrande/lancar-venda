import pandas as pd
from googleapiclient.discovery import build
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT_FILE = 'credentials.json'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
SPREADSHEET_ID = '1EqgiL0cm52Yy10d7DPMHi1bz8olUePn6NlhUuv3ygE4'
EXCEL_SHEET = 'Sheet1'

def encontrar_proxima_posicao(aba):
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets().values()

    RANGE = f'{aba}!A:A'
    result = sheet.get(spreadsheetId=SPREADSHEET_ID, range=RANGE).execute()
    valores = result.get('values', [])

    coluna_A = [row[0] if row else '' for row in valores]  # Garantir que todas as linhas sejam consideradas

    for i in range(len(coluna_A) - 2):
        if coluna_A[i] != '' and coluna_A[i + 1] == '' and coluna_A[i + 2] == '':
            return i + 2  # Retorna a primeira linha vazia onde há duas linhas vazias abaixo

    return len(coluna_A) + 1  # Se não encontrou, retorna a próxima linha disponível

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

if __name__ == "__main__":
    processar_abas()
    