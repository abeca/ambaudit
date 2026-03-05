"""
AMBAUDIT - Padronização de Estados e Municípios
Limpa e padroniza dados geográficos
"""

from supabase import create_client, Client
from dotenv import load_dotenv
import os
import unicodedata

load_dotenv()

def remover_acentos(texto):
    """Remove acentos de texto"""
    if not texto:
        return texto
    nfkd = unicodedata.normalize('NFKD', texto)
    return "".join([c for c in nfkd if not unicodedata.combining(c)])

def padronizar_estados():
    """Padroniza estados para siglas"""
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)
    
    mapa_estados = {
        'ACRE': 'AC',
        'ALAGOAS': 'AL',
        'AMAPA': 'AP',
        'AMAZONAS': 'AM',
        'BAHIA': 'BA',
        'CEARA': 'CE',
        'DISTRITO FEDERAL': 'DF',
        'ESPIRITO SANTO': 'ES',
        'GOIAS': 'GO',
        'MARANHAO': 'MA',
        'MATO GROSSO': 'MT',
        'MATO GROSSO DO SUL': 'MS',
        'MINAS GERAIS': 'MG',
        'PARA': 'PA',
        'PARAIBA': 'PB',
        'PARANA': 'PR',
        'PERNAMBUCO': 'PE',
        'PIAUI': 'PI',
        'RIO DE JANEIRO': 'RJ',
        'RIO GRANDE DO NORTE': 'RN',
        'RIO GRANDE DO SUL': 'RS',
        'RONDONIA': 'RO',
        'RORAIMA': 'RR',
        'SANTA CATARINA': 'SC',
        'SAO PAULO': 'SP',
        'SERGIPE': 'SE',
        'TOCANTINS': 'TO'
    }
    
    print("\n🗺️  ETAPA 1: PADRONIZANDO ESTADOS")
    print("="*60)
    
    total_atualizados = 0
    
    for nome_completo, sigla in mapa_estados.items():
        try:
            # Busca todos os registros com esse nome
            response = supabase.table('raw_ibama_autuacoes')\
                .select('id')\
                .eq('nom_uf', nome_completo)\
                .execute()
            
            if response.data:
                count = len(response.data)
                print(f"  {nome_completo} → {sigla}: {count} registros")
                
                # Atualiza
                supabase.table('raw_ibama_autuacoes')\
                    .update({'nom_uf': sigla})\
                    .eq('nom_uf', nome_completo)\
                    .execute()
                
                total_atualizados += count
        
        except Exception as e:
            print(f"  ❌ Erro em {nome_completo}: {str(e)[:60]}")
    
    print(f"\n✅ Estados: {total_atualizados} registros atualizados")
    return total_atualizados

def padronizar_municipios():
    """Padroniza nomes de municípios"""
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)
    
    print("\n🏙️  ETAPA 2: PADRONIZANDO MUNICÍPIOS")
    print("="*60)
    
    # Busca TODOS os registros
    print("📥 Buscando todos os registros...")
    response = supabase.table('raw_ibama_autuacoes')\
        .select('id, nom_municipio_auto')\
        .execute()
    
    if not response.data:
        print("⚠️  Nenhum registro encontrado")
        return 0
    
    print(f"✅ {len(response.data)} registros encontrados")
    
    # Agrupa por município para ver variações
    print("\n🔍 Identificando variações...")
    municipios_unicos = {}
    
    for registro in response.data:
        mun_original = registro.get('nom_municipio_auto')
        
        if not mun_original or mun_original.strip() == '':
            continue
        
        # Padroniza: UPPERCASE, remove espaços extras, remove acentos
        mun_limpo = mun_original.strip().upper()
        mun_limpo = ' '.join(mun_limpo.split())  # Remove espaços duplos
        
        # Cria chave sem acento para agrupar
        chave = remover_acentos(mun_limpo)
        
        if chave not in municipios_unicos:
            municipios_unicos[chave] = {
                'padrao': mun_limpo,
                'variantes': set(),
                'count': 0
            }
        
        municipios_unicos[chave]['variantes'].add(mun_original)
        municipios_unicos[chave]['count'] += 1
    
    # Mostra variações encontradas
    print(f"\n📋 Encontrados {len(municipios_unicos)} municípios únicos")
    print("\n⚠️  Variações que serão padronizadas:")
    
    tem_variacao = False
    for chave, dados in municipios_unicos.items():
        if len(dados['variantes']) > 1:
            tem_variacao = True
            print(f"\n  {dados['padrao']} ({dados['count']} registros)")
            for var in sorted(dados['variantes']):
                if var != dados['padrao']:
                    print(f"    ← {var}")
    
    if not tem_variacao:
        print("  ✅ Nenhuma variação encontrada!")
        return 0
    
    # Confirma atualização
    print(f"\n{'='*60}")
    resposta = input("Atualizar municípios? (s/n): ")
    
    if resposta.lower() != 's':
        print("❌ Cancelado")
        return 0
    
    # Atualiza cada variante para o padrão
    total_atualizados = 0
    
    for chave, dados in municipios_unicos.items():
        padrao = dados['padrao']
        
        for variante in dados['variantes']:
            if variante != padrao:
                try:
                    # Atualiza
                    result = supabase.table('raw_ibama_autuacoes')\
                        .update({'nom_municipio_auto': padrao})\
                        .eq('nom_municipio_auto', variante)\
                        .execute()
                    
                    if result.data:
                        count = len(result.data)
                        total_atualizados += count
                        print(f"  ✅ '{variante}' → '{padrao}': {count} registros")
                
                except Exception as e:
                    print(f"  ❌ Erro: {str(e)[:60]}")
    
    print(f"\n✅ Municípios: {total_atualizados} registros atualizados")
    return total_atualizados

def limpar_valores_nulos():
    """Remove registros com valores nulos em campos importantes"""
    
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)
    
    print("\n🧹 ETAPA 3: LIMPANDO VALORES VAZIOS")
    print("="*60)
    
    # Atualiza registros com string 'None' ou vazio para NULL
    campos = ['nom_uf', 'nom_municipio_auto', 'nom_pessoa_infrator', 'num_cpf_cnpj']
    
    for campo in campos:
        try:
            # None como string
            result1 = supabase.table('raw_ibama_autuacoes')\
                .update({campo: None})\
                .eq(campo, 'None')\
                .execute()
            
            # String vazia
            result2 = supabase.table('raw_ibama_autuacoes')\
                .update({campo: None})\
                .eq(campo, '')\
                .execute()
            
            total = (len(result1.data) if result1.data else 0) + (len(result2.data) if result2.data else 0)
            
            if total > 0:
                print(f"  {campo}: {total} registros limpos")
        
        except Exception as e:
            print(f"  ⚠️  {campo}: {str(e)[:60]}")
    
    print("✅ Limpeza concluída")

def main():
    print("\n" + "="*60)
    print("🌳 AMBAUDIT - Padronização de Dados")
    print("="*60)
    
    resposta = input("\n⚠️  Isso vai MODIFICAR o banco de dados. Continuar? (s/n): ")
    
    if resposta.lower() != 's':
        print("\n❌ Cancelado")
        return
    
    # Executa padronizações
    total_estados = padronizar_estados()
    total_municipios = padronizar_municipios()
    limpar_valores_nulos()
    
    # Resumo
    print(f"\n{'='*60}")
    print("🎉 PADRONIZAÇÃO COMPLETA!")
    print(f"{'='*60}")
    print(f"📊 Resumo:")
    print(f"   Estados atualizados: {total_estados}")
    print(f"   Municípios atualizados: {total_municipios}")
    print(f"   Total: {total_estados + total_municipios}")
    print(f"\n💡 Próximos passos:")
    print(f"   1. streamlit run frontend/app.py")
    print(f"   2. Aperte 'C' para limpar cache")
    print(f"   3. Veja todos os estados e municípios!")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    main()
