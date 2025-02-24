import pandas as pd
from datetime import datetime
import requests
import json

# Função para converter HH:MM:SS em segundos
def time_to_seconds(time_str):
    hh, mm, ss = map(int, time_str.split(':'))
    return hh * 3600 + mm * 60 + ss

# Função para converter segundos de volta para HH:MM:SS
def seconds_to_time(seconds):
    seconds = int(seconds)  # Converter para inteiro para evitar floats
    hh = seconds // 3600
    mm = (seconds % 3600) // 60
    ss = seconds % 60
    return f"{hh:02d}:{mm:02d}:{ss:02d}"

# Função para calcular dias desde a última atualização
def dias_desde_ultima_atualizacao(data_str):
    try:
        data_ultima_atualizacao = datetime.strptime(data_str, '%Y-%m-%dT%H:%M:%S.%fZ')
        data_atual = datetime.now()
        diferenca = data_atual - data_ultima_atualizacao
        return diferenca.days
    except ValueError as e:
        print(f"Erro ao processar a data: {data_str}. Erro: {e}")
        return None

# Função para carregar e pré-processar os dados da API
def carregar_e_preprocessar_api(url_api):
    try:
        response = requests.get(url_api)
        response.raise_for_status()
        print(f"Resposta da API {url_api}: {response.status_code} - {response.text[:200]}")
        
        dados = response.json()
        if "watchtimes" not in dados:
            print("Erro: A chave 'watchtimes' não foi encontrada no retorno da API.")
            return None
        df = pd.DataFrame(dados["watchtimes"])
        
        colunas_mapeadas = {
            'user_email': 'Email',
            'user_full_name': 'Nome Completo',
            'course_name': 'Curso',
            'until_completed_duration': 'Tempo Assistido',
            'updated_at': 'Última atualização'
        }
        
        colunas_necessarias = ['user_email', 'user_full_name', 'course_name', 'until_completed_duration', 'updated_at']
        for col in colunas_necessarias:
            if col not in df.columns:
                print(f"Erro: A coluna '{col}' não foi encontrada nos dados da API.")
                return None
        
        df = df.rename(columns=colunas_mapeadas)
        df['Tempo Assistido'] = df['Tempo Assistido'].apply(lambda x: x / 1000)  # Milissegundos para segundos
        df['dias_sem_acesso'] = df['Última atualização'].apply(dias_desde_ultima_atualizacao)
        df['dias_sem_acesso'] = df['dias_sem_acesso'].apply(lambda x: min(x, 120) if x is not None else None)
        
        return df
    
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter dados da API {url_api}: {e}")
        return None

# Função para processar tempo por aluno e curso
def processar_tempo_por_aluno_e_curso(df):
    df_grouped = df.groupby(['Nome Completo', 'Curso', 'Email'], as_index=False).agg({
        'Tempo Assistido': 'sum',
        'dias_sem_acesso': 'min',
        'Última atualização': 'max'  # Manter o valor mais recente de updated_at
    })
    df_grouped['tempo_total_formatado'] = df_grouped['Tempo Assistido'].apply(seconds_to_time)
    df_grouped = df_grouped.drop(columns=['Tempo Assistido'])
    return df_grouped

# Função para processar tempo por aluno
def processar_tempo_por_aluno(df):
    df_grouped = df.groupby(['Nome Completo', 'Email'], as_index=False).agg({
        'Tempo Assistido': 'sum',
        'dias_sem_acesso': 'min',
        'Última atualização': 'max'  # Manter o valor mais recente de updated_at
    })
    df_grouped['tempo_total_formatado'] = df_grouped['Tempo Assistido'].apply(seconds_to_time)
    df_grouped = df_grouped.drop(columns=['Tempo Assistido'])
    return df_grouped

# Função para calcular progresso por curso
def calcular_progresso_por_curso(df):
    tempo_maximo_por_curso = {
        'Linux': time_to_seconds('06:49:11'),
        'Scratch': time_to_seconds('03:12:21'),
        'Introdução a Web': time_to_seconds('12:28:23'),
        'No Code': time_to_seconds('05:36:37'),
        'Python': time_to_seconds('15:41:33'),
        'JavaScript': time_to_seconds('09:32:53'),
        'Programação Orientada a Objetos': time_to_seconds('09:03:40'),
        'Programação Intermediária com Python - Python II': time_to_seconds('12:57:25'),
        'Banco de Dados': time_to_seconds('07:15:00'),
        'Projetos II': time_to_seconds('01:40:58'),
        'Tutorial Plataforma': time_to_seconds('01:00:00')
    }

    df['tempo_total_segundos'] = df['tempo_total_formatado'].apply(time_to_seconds)
    df['progresso'] = df.apply(
        lambda row: min(row['tempo_total_segundos'] / tempo_maximo_por_curso.get(row['Curso'], 1), 1) * 100
        if row['Curso'] in tempo_maximo_por_curso else 0, axis=1
    )
    df['progresso'] = df['progresso'].round(2)
    df = df.drop(columns=['tempo_total_segundos'])
    return df

# Função para obter dados da API externa
def obter_dados_api(url, headers=None):
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        print(f"Resposta da API {url}: {response.status_code} - {response.text[:200]}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao obter dados da API {url}: {e}")
        return None

# Função para cruzar dados com a API externa
def cruzar_dados_api(df_local, dados_api_externa):
    if dados_api_externa:
        df_api_externa = pd.DataFrame(dados_api_externa)
        df_api_externa = df_api_externa.rename(columns={
            'emailPd': 'Email',
            'registrationCode': 'registration_code'
        })
        colunas_desejadas = ['Email', 'registration_code']
        if 'status' in df_api_externa.columns:
            colunas_desejadas.append('status')
        df_cruzado = pd.merge(
            df_local,
            df_api_externa[colunas_desejadas],
            on='Email',
            how='left'
        )
        return df_cruzado
    return df_local

# Função para deletar todos os dados da API
def deletar_dados_api(url):
    try:
        response = requests.delete(url)
        if response.status_code in [200, 204]:
            print(f"Dados deletados com sucesso da API {url}")
        else:
            print(f"Erro ao deletar dados da API {url}. Status: {response.status_code}, Resposta: {response.text}")
    except requests.exceptions.RequestException as e:
        print(f"Erro de conexão ao tentar deletar dados da API {url}: {e}")

# Função para enviar dados para a API com validação de e-mails
def enviar_dados_api(url, df, colunas_desejadas):
    # Filtrar apenas e-mails que terminam em @pditabira.com ou @pdbomdespacho.com.br
    df_filtered = df[df['Email'].str.match(r'.*@(pditabira\.com|pdbomdespacho\.com\.br)$')]
    
    # Renomear 'Última atualização' de volta para 'updated_at' antes de enviar
    df_filtered = df_filtered.rename(columns={'Última atualização': 'updated_at'})
    
    # Selecionar apenas as colunas desejadas para o endpoint específico
    df_clean = df_filtered[colunas_desejadas].fillna(0)
    dados = df_clean.to_dict(orient='records')
    
    # Enviar apenas se houver dados após o filtro
    if dados:
        headers = {'Content-Type': 'application/json'}
        try:
            response = requests.post(url, json=dados, headers=headers)
            if response.status_code in [200, 201]:
                print(f"Dados enviados com sucesso para ")
            else:
                print(f"Erro ao enviar dados para {url}. Status: {response.status_code}, Resposta: {response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão com a API {url}: {e}")
    else:
        print(f"Nenhum dado a enviar para {url} após filtragem de e-mails.")

# URL da API de dados assistidos
url_relatorio_api = 'https://presence.projetodesenvolve.online/watchtime?fromCompleted=2024-12-01T03:00:00.000Z&toCompleted=2026-01-01T02:59:59.000Z&fromUpdated=2024-12-01T03:00:00.000Z&toUpdated=2026-01-01'

# URLs das APIs locais
url_tempo_por_aluno_e_curso = 'http://127.0.0.1:5000/api/tempo_por_aluno_e_curso'
url_tempo_por_aluno = 'http://127.0.0.1:5000/api/tempo_por_aluno'
url_progresso_por_curso = 'http://127.0.0.1:5000/api/progresso_por_curso'
url_api_externa = 'https://form.pdinfinita.com.br/enrolled'

# Headers para a API externa
headers_api_externa = {
    'api-key': 'Rm9ybUFwaUZlaXRtUGVsb0plYW5QaWVycmVQYXJhYURlc2Vudm9sdmU='
}

# Carregar e pré-processar o DataFrame da API
df_relatorio = carregar_e_preprocessar_api(url_relatorio_api)

if df_relatorio is not None:
    # Filtrar e-mails válidos logo no início
    df_relatorio = df_relatorio[df_relatorio['Email'].str.match(r'.*@(pditabira\.com|pdbomdespacho\.com\.br)$')]
    
    if not df_relatorio.empty:
        # Processar os dados
        df_tempo_por_aluno_e_curso = processar_tempo_por_aluno_e_curso(df_relatorio)
        df_tempo_por_aluno = processar_tempo_por_aluno(df_relatorio)
        df_progresso_por_curso = calcular_progresso_por_curso(df_tempo_por_aluno_e_curso)

        # Obter dados da API externa
        dados_api_externa = obter_dados_api(url_api_externa, headers=headers_api_externa)

        # Cruzar os dados com a API externa
        df_tempo_por_aluno_e_curso = cruzar_dados_api(df_tempo_por_aluno_e_curso, dados_api_externa)
        df_tempo_por_aluno = cruzar_dados_api(df_tempo_por_aluno, dados_api_externa)
        df_progresso_por_curso = cruzar_dados_api(df_progresso_por_curso, dados_api_externa)

        # Remover colunas desnecessárias
        df_tempo_por_aluno_e_curso = df_tempo_por_aluno_e_curso.drop(columns=['dias_sem_acesso'])
        df_tempo_por_aluno = df_tempo_por_aluno.drop(columns=['dias_sem_acesso'])
        df_progresso_por_curso = df_progresso_por_curso.drop(columns=['dias_sem_acesso'])

        # Definir colunas desejadas para cada endpoint
        colunas_tempo_por_aluno_e_curso = ['Nome Completo', 'Curso', 'Email', 'tempo_total_formatado', 'updated_at', 'registration_code', 'status']
        colunas_tempo_por_aluno = ['Nome Completo', 'Email', 'tempo_total_formatado', 'updated_at', 'registration_code', 'status']
        colunas_progresso_por_curso = ['Curso', 'Email', 'Nome Completo', 'progresso', 'registration_code', 'status', 'tempo_total_formatado', 'updated_at']

        # Deletar todos os dados da API local antes de enviar novos dados
        deletar_dados_api(url_tempo_por_aluno_e_curso)
        deletar_dados_api(url_tempo_por_aluno)
        deletar_dados_api(url_progresso_por_curso)

        # Enviar os novos dados para as APIs
        enviar_dados_api(url_tempo_por_aluno_e_curso, df_tempo_por_aluno_e_curso, colunas_tempo_por_aluno_e_curso)
        enviar_dados_api(url_tempo_por_aluno, df_tempo_por_aluno, colunas_tempo_por_aluno)
        enviar_dados_api(url_progresso_por_curso, df_progresso_por_curso, colunas_progresso_por_curso)
    else:
        print("Nenhum dado válido após filtragem inicial de e-mails.")

print("Processamento concluído. Dados enviados para as APIs.")