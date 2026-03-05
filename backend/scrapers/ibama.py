"""
AMBAUDIT - Ingestão CORRIGIDA
Mapeia corretamente + trata NaN
"""

import pandas as pd
import os
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv
from io import StringIO
import numpy as np
from cryptography.fernet import Fernet

load_dotenv()


def get_fernet():
    key = os.getenv("ENCRYPTION_KEY")
    if not key:
        raise ValueError("ENCRYPTION_KEY não encontrado no .env")
    return Fernet(key.encode())


def encrypt_cpf(valor):
    """Criptografa CPF/CNPJ. Retorna o valor original se vazio."""
    if not valor or str(valor).strip() in ('', 'None', 'nan'):
        return valor
    try:
        return get_fernet().encrypt(str(valor).encode()).decode()
    except Exception as e:
        print(f"  ⚠️ Erro ao criptografar: {e}")
        return valor

class PlanilhaIngestor:
    
    def __init__(self):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if supabase_url and supabase_key:
            self.supabase: Client = create_client(supabase_url, supabase_key)
            print("✅ Conectado ao Supabase")
        else:
            self.supabase = None
    
    def encontrar_planilha(self, pasta_downloads):
        print(f"\n🔍 Procurando em: {pasta_downloads}")
        
        extensoes = ['.xlsx', '.xls', '.csv', '.ods', '.html', '.htm']
        arquivos = []
        
        for arquivo in os.listdir(pasta_downloads):
            if any(arquivo.lower().endswith(ext) for ext in extensoes):
                caminho = os.path.join(pasta_downloads, arquivo)
                tamanho = os.path.getsize(caminho) / 1024
                modificado = datetime.fromtimestamp(os.path.getmtime(caminho))
                
                arquivos.append({
                    'nome': arquivo,
                    'caminho': caminho,
                    'tamanho_kb': tamanho,
                    'modificado': modificado
                })
        
        if not arquivos:
            return None
        
        arquivos.sort(key=lambda x: x['modificado'], reverse=True)
        
        print(f"\n📋 Arquivos encontrados:")
        for idx, arq in enumerate(arquivos[:15], 1):
            print(f"   {idx}. {arq['nome']}")
            print(f"      {arq['tamanho_kb']:.1f} KB | {arq['modificado'].strftime('%d/%m/%Y %H:%M')}")
        
        return arquivos
    
    def ler_planilha_debug(self, caminho):
        """Lê planilha"""
        print(f"\n📖 Lendo: {os.path.basename(caminho)}")
        
        # HTML
        for encoding in ['utf-8', 'latin-1', 'iso-8859-1']:
            try:
                with open(caminho, 'r', encoding=encoding, errors='ignore') as f:
                    conteudo = f.read()
                
                if '<table' in conteudo.lower():
                    df = pd.read_html(StringIO(conteudo))[0]
                    print(f"  ✅ {len(df)} linhas, {len(df.columns)} colunas")
                    
                    # Ajusta header
                    if df.iloc[0].astype(str).str.contains('Tipo|Data|CPF|Nome|Nº', case=False).any():
                        df.columns = df.iloc[0]
                        df = df[1:]
                        df = df.reset_index(drop=True)
                        print(f"  ✓ Header ajustado")
                    
                    return df
            except:
                continue
        
        # Excel
        try:
            df = pd.read_excel(caminho, engine='openpyxl')
            print(f"  ✅ Excel: {len(df)} linhas")
            return df
        except:
            pass
        
        return None
    
    def limpar_dados(self, df):
        print(f"\n🧹 Limpando...")
        df = df.dropna(how='all')
        df = df.dropna(axis=1, how='all')
        df = df.loc[:, df.columns.notna()]
        print(f"✅ {len(df)} linhas, {len(df.columns)} colunas")
        return df
    
    def mostrar_preview(self, df):
        print(f"\n📊 COLUNAS ORIGINAIS:")
        for idx, col in enumerate(df.columns, 1):
            print(f"   {idx}. '{col}'")
    
    def mapear_colunas(self, df):
        """Mapeamento ESPECÍFICO e CORRETO"""
        print(f"\n🔄 Mapeando...")
        
        # Mapeamento EXPLÍCITO por nome exato
        mapeamento = {
            'Nº': 'num_sequencia',
            'Tipo Infracao': 'tipo_infracao',
            'Data Infração': 'data_autuacao',  # ⭐ CORRIGIDO
            'Bioma': 'des_tipo_bioma',
            'Estado': 'nom_uf',
            'Município': 'nom_municipio_auto',
            'CPF ou CNPJ': 'num_cpf_cnpj',
            'Nome Autuado': 'nom_pessoa_infrator',  # ⭐ CORRIGIDO
            'Nº A.I.': 'num_auto_infracao',  # ⭐ ADICIONADO
            'Série A.I.': 'ser_auto_infracao',  # ⭐ ADICIONADO
            'Valor Multa': 'valor_multa_numerico',
            'Nº Processo': 'num_processo',  # ⭐ ADICIONADO
            'Status Débito': 'des_status_debito',
            'Sanções Aplicadas': 'num_enquadramento'
        }
        
        print(f"\n📋 Mapeamento:")
        for orig, dest in mapeamento.items():
            if orig in df.columns:
                print(f"   '{orig}' → {dest}")
            else:
                print(f"   ⚠️ '{orig}' não encontrada")
        
        # Renomeia
        df = df.rename(columns=mapeamento)
        
        # Colunas obrigatórias
        colunas_supabase = [
            'num_sequencia', 'tipo_infracao', 'data_autuacao', 'des_tipo_bioma',
            'nom_uf', 'nom_municipio_auto', 'num_cpf_cnpj', 'nom_pessoa_infrator',
            'num_auto_infracao', 'ser_auto_infracao', 'valor_multa_numerico',
            'num_processo', 'des_status_debito', 'num_enquadramento'
        ]
        
        # Adiciona faltantes
        for col in colunas_supabase:
            if col not in df.columns:
                df[col] = None
        
        # Seleciona apenas colunas Supabase
        df = df[colunas_supabase]
        
        # Normaliza estados para siglas
        mapa_estados = {
            'ACRE': 'AC', 'ALAGOAS': 'AL', 'AMAPA': 'AP', 'AMAPÁ': 'AP',
            'AMAZONAS': 'AM', 'BAHIA': 'BA', 'CEARA': 'CE', 'CEARÁ': 'CE',
            'DISTRITO FEDERAL': 'DF', 'ESPIRITO SANTO': 'ES', 'ESPÍRITO SANTO': 'ES',
            'GOIAS': 'GO', 'GOIÁS': 'GO', 'MARANHAO': 'MA', 'MARANHÃO': 'MA',
            'MATO GROSSO DO SUL': 'MS', 'MATO GROSSO': 'MT', 'MINAS GERAIS': 'MG',
            'PARA': 'PA', 'PARÁ': 'PA', 'PARAIBA': 'PB', 'PARAÍBA': 'PB',
            'PARANA': 'PR', 'PARANÁ': 'PR', 'PERNAMBUCO': 'PE', 'PIAUI': 'PI', 'PIAUÍ': 'PI',
            'RIO DE JANEIRO': 'RJ', 'RIO GRANDE DO NORTE': 'RN', 'RIO GRANDE DO SUL': 'RS',
            'RONDONIA': 'RO', 'RONDÔNIA': 'RO', 'RORAIMA': 'RR', 'SANTA CATARINA': 'SC',
            'SAO PAULO': 'SP', 'SÃO PAULO': 'SP', 'SERGIPE': 'SE', 'TOCANTINS': 'TO'
        }
        # Converte valor de centavos para reais
        if 'valor_multa_numerico' in df.columns:
            df['valor_multa_numerico'] = pd.to_numeric(df['valor_multa_numerico'], errors='coerce') / 100

        if 'nom_uf' in df.columns:
            df['nom_uf'] = df['nom_uf'].apply(
                lambda x: mapa_estados.get(str(x).strip().upper(), x) if x else x
            )

        # Criptografa CPF/CNPJ
        if 'num_cpf_cnpj' in df.columns:
            print(f"\n🔐 Criptografando CPF/CNPJ...")
            df['num_cpf_cnpj'] = df['num_cpf_cnpj'].apply(encrypt_cpf)
            print(f"✅ CPF/CNPJ criptografados")

        # ⭐ TRATA NaN - CONVERTE PARA None
        print(f"\n🔧 Tratando valores NaN...")
        df = df.replace({np.nan: None})
        
        # Converte colunas de texto para string
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).replace('nan', None).replace('None', None)
        
        print(f"✅ Dados prontos!")
        return df
    
    def salvar_supabase(self, df, lote=500):
        if not self.supabase or len(df) == 0:
            return False
        
        print(f"\n☁️ Salvando {len(df):,} registros...")
        
        # ⭐ GARANTE que NaN virou None
        df = df.replace({np.nan: None})
        
        registros = df.to_dict('records')
        salvos = 0
        erros = 0
        
        for i in range(0, len(registros), lote):
            chunk = registros[i:i+lote]
            num = (i // lote) + 1
            total = (len(registros) + lote - 1) // lote
            
            try:
                print(f"  [{num}/{total}] {len(chunk)} regs...", end=' ')
                self.supabase.table('raw_ibama_autuacoes').insert(chunk).execute()
                salvos += len(chunk)
                print(f"✅")
            except Exception as e:
                erro_msg = str(e)[:100]
                print(f"❌ {erro_msg}")
                erros += len(chunk)
        
        print(f"\n📊 RESULTADO:")
        print(f"   ✅ Salvos: {salvos:,}")
        print(f"   ❌ Erros: {erros:,}")
        if len(registros) > 0:
            print(f"   📈 Taxa: {salvos/len(registros)*100:.1f}%")
        
        return salvos > 0
    
    def processar(self, caminho=None):
        print("\n" + "="*60)
        print("🌳 AMBAUDIT - Ingestão")
        print("="*60)
        
        if not caminho:
            pasta = os.path.join(os.path.expanduser("~"), "Downloads")
            arquivos = self.encontrar_planilha(pasta)
            
            if not arquivos:
                caminho = input("\n📂 Caminho: ")
                if not os.path.exists(caminho):
                    print("❌ Não encontrado")
                    return False
            else:
                escolha = input(f"\nMais recente? (s/n): ")
                
                if escolha.lower() == 's':
                    caminho = arquivos[0]['caminho']
                else:
                    num = input(f"Qual? (1-{min(15, len(arquivos))}): ")
                    try:
                        caminho = arquivos[int(num) - 1]['caminho']
                    except:
                        print("❌ Inválido")
                        return False
        
        df = self.ler_planilha_debug(caminho)
        
        if df is None:
            return False
        
        df = self.limpar_dados(df)
        self.mostrar_preview(df)
        
        if input(f"\n🤔 Continuar? (s/n): ").lower() != 's':
            print("❌ Cancelado")
            return False
        
        df = self.mapear_colunas(df)
        
        print(f"\n📊 Preview mapeado:")
        print(df.head(2).to_string())
        
        if input(f"\n💾 Salvar {len(df):,}? (s/n): ").lower() != 's':
            csv = f"backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
            df.to_csv(csv, index=False, encoding='utf-8-sig')
            print(f"💾 CSV: {csv}")
            return False
        
        ok = self.salvar_supabase(df)
        
        if ok:
            print(f"\n{'='*60}")
            print(f"🎉 SUCESSO!")
            print(f"{'='*60}")
            print(f"📊 {len(df):,} registros")
            print(f"🎨 streamlit run frontend/app.py")
            print(f"{'='*60}\n")
        
        return ok


if __name__ == "__main__":
    ingestor = PlanilhaIngestor()
    
    print("\n" + "="*60)
    print("🌳 AMBAUDIT - Ingestão IBAMA")
    print("="*60)
    
    if input("\n🚀 Iniciar? (s/n): ").lower() == 's':
        ingestor.processar()
    else:
        print("\n👋 Cancelado")