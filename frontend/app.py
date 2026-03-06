"""
AMBAUDIT - Dashboard Frontend
Visualização de autos de infração ambiental
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from supabase import create_client, Client
from dotenv import load_dotenv
from cryptography.fernet import Fernet
import os
from datetime import datetime


def get_secret(key_name):
    """Lê segredo do st.secrets (Streamlit Cloud) ou .env (local)"""
    try:
        return st.secrets[key_name]
    except Exception:
        return os.getenv(key_name)


def get_fernet():
    key = get_secret("ENCRYPTION_KEY")
    if not key:
        return None
    return Fernet(key.encode())


def decrypt_cpf(valor):
    """Descriptografa CPF/CNPJ. Retorna o valor original se não conseguir."""
    if not valor or str(valor).strip() in ('', 'None', 'nan'):
        return valor
    try:
        f = get_fernet()
        if f is None:
            return valor
        return f.decrypt(str(valor).encode()).decode()
    except Exception:
        return valor  # Retorna original se não conseguir descriptografar

# Configuração da página
st.set_page_config(
    page_title="AMBAUDIT - Autos de Infração Ambiental",
    page_icon="🌳",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS customizado
st.markdown("""
    <style>
    .main {
        padding: 0rem 1rem;
    }
    h1 {
        color: #4caf50;
    }
    .highlight {
        background-color: #ffeb3b;
        padding: 2px 5px;
        border-radius: 3px;
    }
    </style>
    """, unsafe_allow_html=True)

# Carrega variáveis de ambiente
load_dotenv()

@st.cache_resource
def init_supabase():
    """Inicializa conexão com Supabase"""
    supabase_url = get_secret("SUPABASE_URL")
    supabase_key = get_secret("SUPABASE_KEY")

    if not supabase_url or not supabase_key:
        st.error("⚠️ Credenciais do Supabase não encontradas (.env ou Streamlit Secrets)")
        st.stop()

    return create_client(supabase_url, supabase_key)

@st.cache_data(ttl=300)
def carregar_dados():
    """Carrega TODOS os dados do Supabase usando paginação"""
    try:
        supabase = init_supabase()
        
        todos_registros = []
        page_size = 1000
        offset = 0
        
        print("📥 Carregando dados em lotes...")
        
        while True:
            # Busca em lotes de 1000
            response = supabase.table('raw_ibama_autuacoes')\
                .select("*")\
                .range(offset, offset + page_size - 1)\
                .execute()
            
            if not response.data or len(response.data) == 0:
                break
            
            todos_registros.extend(response.data)
            print(f"  ✓ Lote {offset//page_size + 1}: {len(response.data)} registros (total: {len(todos_registros)})")
            
            offset += page_size
            
            # Segurança: não carregar mais de 50.000
            if offset > 50000:
                print("⚠️ Limite de segurança atingido (50k registros)")
                break
        
        if todos_registros:
            df = pd.DataFrame(todos_registros)
            
            print(f"✅ TOTAL CARREGADO: {len(df)} registros")
            
            # Limpeza básica de dados
            if 'nom_uf' in df.columns:
                df['nom_uf'] = df['nom_uf'].astype(str).str.strip()
            
            if 'nom_municipio_auto' in df.columns:
                df['nom_municipio_auto'] = df['nom_municipio_auto'].astype(str).str.strip()
            
            if 'nom_pessoa_infrator' in df.columns:
                df['nom_pessoa_infrator'] = df['nom_pessoa_infrator'].astype(str).str.strip()
            
            if 'num_cpf_cnpj' in df.columns:
                df['num_cpf_cnpj'] = df['num_cpf_cnpj'].astype(str).str.strip().apply(decrypt_cpf)
            
            if 'tipo_infracao' in df.columns:
                df['tipo_infracao'] = df['tipo_infracao'].astype(str).str.strip()
            
            # Substitui 'None' string por NaN
            df = df.replace('None', None)
            df = df.replace('', None)
            
            return df
        else:
            return pd.DataFrame()
            
    except Exception as e:
        st.error(f"Erro ao carregar dados: {e}")
        import traceback
        st.error(traceback.format_exc())
        return pd.DataFrame()

def formatar_valor(valor):
    """Retorna o valor já formatado (novo campo numérico)"""
    try:
        if pd.isna(valor):
            return 0.0
        return float(valor)
    except:
        return 0.0

@st.cache_data(ttl=60)
def carregar_status_opcoes():
    """Carrega opções de status_contato do Supabase"""
    try:
        supabase = init_supabase()
        response = supabase.table('status_contato_opcoes').select('nome').order('nome').execute()
        return [r['nome'] for r in response.data] if response.data else []
    except:
        return []

def mostrar_login():
    """Página de login e criação de conta"""
    st.markdown("""
        <style>
        .login-container { max-width: 420px; margin: 0 auto; padding-top: 60px; }
        </style>
    """, unsafe_allow_html=True)

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        st.title("🌳 AMBAUDIT")
        st.markdown("##### Plataforma de Inteligência em Infrações Ambientais")
        st.markdown("---")

        email = st.text_input("E-mail", key="login_email", placeholder="seu@email.com")
        senha = st.text_input("Senha", type="password", key="login_senha", placeholder="••••••••")

        if st.button("Entrar", use_container_width=True, type="primary", key="btn_login"):
            if email and senha:
                try:
                    supabase = init_supabase()
                    response = supabase.auth.sign_in_with_password({
                        "email": email,
                        "password": senha
                    })
                    st.session_state['user_email'] = response.user.email
                    st.session_state['user_id'] = response.user.id
                    st.rerun()
                except Exception:
                    st.error("E-mail ou senha incorretos.")
            else:
                st.warning("Preencha e-mail e senha.")

        st.markdown("<br>", unsafe_allow_html=True)
        st.caption("AMBAUDIT © 2026 — Dados: IBAMA")


def salvar_status_alterados(alteracoes):
    """Salva alterações de status_contato no Supabase"""
    try:
        supabase = init_supabase()
        for item in alteracoes:
            supabase.table('raw_ibama_autuacoes')\
                .update({'status_contato': item['status'] if item['status'] else None})\
                .eq('id', item['id'])\
                .execute()
        return True
    except Exception as e:
        st.error(f"Erro ao salvar: {e}")
        return False

def main():
    # Verifica autenticação
    if 'user_email' not in st.session_state:
        mostrar_login()
        st.stop()

    # Header
    st.title("🌳 AMBAUDIT")
    st.markdown("### Plataforma de Inteligência em Infrações Ambientais")
    st.markdown("---")
    
    # Carrega dados
    with st.spinner("🔄 Carregando dados do IBAMA..."):
        df = carregar_dados()
    
    if df.empty:
        st.warning("⚠️ Nenhum dado encontrado no banco de dados")
        st.info("Execute o scraper primeiro: `python backend/scrapers/ingerir_planilha.py`")
        st.stop()
    
    # Mostra total de registros carregados
    st.success(f"✅ {len(df):,} registros carregados do banco de dados")
    
    # Usa a coluna numérica nova do banco
    if 'valor_multa_numerico' in df.columns:
        df['valor_numerico'] = df['valor_multa_numerico'].apply(formatar_valor)
    elif 'valor_auto_formatado' in df.columns:
        # Fallback para dados antigos
        df['valor_numerico'] = df['valor_auto_formatado'].apply(lambda x: float(x)/100 if pd.notna(x) else 0.0)
    
    # SIDEBAR - Filtros
    with st.sidebar:
        # Usuário logado + logout
        st.caption(f"👤 {st.session_state['user_email']}")
        if st.button("🚪 Sair", use_container_width=True):
            try:
                supabase = init_supabase()
                supabase.auth.sign_out()
            except Exception:
                pass
            del st.session_state['user_email']
            del st.session_state['user_id']
            st.rerun()

        st.markdown("---")
        st.header("🔍 Filtros")
        
        # Filtro por tipo de infração
        if 'tipo_infracao' in df.columns:
            # Remove NULL/vazios
            tipos_validos = df['tipo_infracao'].dropna()
            tipos_validos = tipos_validos[tipos_validos != '']
            tipos_validos = tipos_validos[tipos_validos.astype(str) != 'None']
            
            tipos = ['Todos'] + sorted(tipos_validos.unique().tolist())
            tipo_selecionado = st.selectbox("Tipo de Infração", tipos)
        else:
            tipo_selecionado = 'Todos'
        
        # Filtro por estado
        if 'nom_uf' in df.columns:
            # Remove NULL/vazios ANTES de criar lista
            estados_validos = df['nom_uf'].dropna()
            estados_validos = estados_validos[estados_validos != '']
            estados_validos = estados_validos[estados_validos.astype(str) != 'None']
            
            estados = ['Todos'] + sorted(estados_validos.unique().tolist())
            estado_selecionado = st.selectbox("Estado", estados)
        else:
            estado_selecionado = 'Todos'
        
        # Filtro por status do débito
        if 'des_status_debito' in df.columns:
            status_validos = df['des_status_debito'].dropna()
            status_validos = status_validos[status_validos != '']
            status_validos = status_validos[status_validos.astype(str) != 'None']

            status_lista = ['Todos'] + sorted(status_validos.unique().tolist())
            status_selecionado = st.selectbox("Status do Débito", status_lista)
        else:
            status_selecionado = 'Todos'

        # Filtro por município
        if 'nom_municipio_auto' in df.columns:
            # Filtra por estado primeiro (se selecionado)
            if estado_selecionado != 'Todos':
                df_temp = df[df['nom_uf'] == estado_selecionado]
            else:
                df_temp = df
            
            # Remove NULL/vazios
            municipios_validos = df_temp['nom_municipio_auto'].dropna()
            municipios_validos = municipios_validos[municipios_validos != '']
            municipios_validos = municipios_validos[municipios_validos.astype(str) != 'None']
            
            # Remove duplicatas e ordena
            municipios_lista = sorted(municipios_validos.unique().tolist())
            
            municipios = ['Todos'] + municipios_lista
            municipio_selecionado = st.selectbox("Município", municipios)
        else:
            municipio_selecionado = 'Todos'
        
        # Filtro por status de contato
        opcoes_status = carregar_status_opcoes()
        if opcoes_status:
            status_contato_filtro = st.selectbox(
                "Status de Contato",
                ['Todos', 'Sem status'] + opcoes_status
            )
        else:
            status_contato_filtro = 'Todos'

        st.markdown("---")

        # Busca por CPF/CNPJ ou Nome
        st.header("🔎 Busca")
        busca_texto = st.text_input("CPF/CNPJ ou Nome do Infrator")

        st.markdown("---")

        # Gerenciar opções de status de contato
        with st.expander("⚙️ Gerenciar Status de Contato"):
            novo_status = st.text_input("Novo status", key="input_novo_status")
            if st.button("➕ Adicionar", key="btn_add_status", use_container_width=True):
                if novo_status.strip():
                    try:
                        supabase = init_supabase()
                        supabase.table('status_contato_opcoes').insert({'nome': novo_status.strip()}).execute()
                        st.cache_data.clear()
                        st.success(f"'{novo_status.strip()}' adicionado!")
                        st.rerun()
                    except Exception as e:
                        st.error(f"Erro: {str(e)[:80]}")

            opcoes_atuais = carregar_status_opcoes()
            if opcoes_atuais:
                st.caption("Status existentes:")
                for op in opcoes_atuais:
                    col_nome, col_del = st.columns([4, 1])
                    col_nome.write(op)
                    if col_del.button("🗑️", key=f"del_status_{op}"):
                        try:
                            supabase = init_supabase()
                            supabase.table('status_contato_opcoes').delete().eq('nome', op).execute()
                            st.cache_data.clear()
                            st.rerun()
                        except Exception as e:
                            st.error(f"Erro: {str(e)[:80]}")

        st.markdown("---")

        # Botão para atualizar dados
        if st.button("🔄 Atualizar Dados", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    # Aplica filtros
    df_filtrado = df.copy()
    
    if tipo_selecionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['tipo_infracao'] == tipo_selecionado]
    
    if estado_selecionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['nom_uf'] == estado_selecionado]
    
    if municipio_selecionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['nom_municipio_auto'] == municipio_selecionado]

    if status_selecionado != 'Todos':
        df_filtrado = df_filtrado[df_filtrado['des_status_debito'] == status_selecionado]

    if status_contato_filtro != 'Todos' and 'status_contato' in df_filtrado.columns:
        if status_contato_filtro == 'Sem status':
            df_filtrado = df_filtrado[df_filtrado['status_contato'].isna() | (df_filtrado['status_contato'] == '')]
        else:
            df_filtrado = df_filtrado[df_filtrado['status_contato'] == status_contato_filtro]

    if busca_texto:
        mask = (
            df_filtrado['num_cpf_cnpj'].astype(str).str.contains(busca_texto, case=False, na=False) |
            df_filtrado['nom_pessoa_infrator'].astype(str).str.contains(busca_texto, case=False, na=False)
        )
        df_filtrado = df_filtrado[mask]
    
    # MÉTRICAS PRINCIPAIS
    st.header("📊 Visão Geral")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_autos = len(df_filtrado)
        st.metric("Total de Autos", f"{total_autos:,}")
    
    with col2:
        if 'valor_numerico' in df_filtrado.columns:
            total_multas = df_filtrado['valor_numerico'].sum()
            st.metric("Valor Total Multas", f"R$ {total_multas:,.2f}")
        else:
            st.metric("Valor Total Multas", "N/A")
    
    with col3:
        if 'valor_numerico' in df_filtrado.columns and total_autos > 0:
            media_multa = df_filtrado['valor_numerico'].mean()
            st.metric("Média por Auto", f"R$ {media_multa:,.2f}")
        else:
            st.metric("Média por Auto", "N/A")
    
    with col4:
        if 'nom_uf' in df_filtrado.columns:
            estados_unicos = df_filtrado['nom_uf'].nunique()
            st.metric("Estados Atingidos", estados_unicos)
        else:
            st.metric("Estados Atingidos", "N/A")
    
    st.markdown("---")
    
    # GRÁFICOS
    if not df_filtrado.empty:
        tab1, tab2, tab3 = st.tabs(["📈 Análises", "📋 Dados Completos", "🗺️ Mapa"])
        
        with tab1:
            col1, col2 = st.columns(2)
            
            with col1:
                # Gráfico: Autos por Estado
                if 'nom_uf' in df_filtrado.columns:
                    st.subheader("Autos por Estado")
                    autos_por_estado = df_filtrado['nom_uf'].value_counts().reset_index()
                    autos_por_estado.columns = ['Estado', 'Quantidade']
                    
                    fig = px.bar(
                        autos_por_estado.head(20),  # Top 20
                        x='Estado',
                        y='Quantidade',
                        color='Quantidade',
                        color_continuous_scale='Greens'
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Gráfico: Autos por Tipo de Infração
                if 'tipo_infracao' in df_filtrado.columns:
                    st.subheader("Tipos de Infração")
                    tipos_count = df_filtrado['tipo_infracao'].value_counts().reset_index()
                    tipos_count.columns = ['Tipo', 'Quantidade']
                    
                    fig = px.pie(
                        tipos_count,
                        values='Quantidade',
                        names='Tipo',
                        color_discrete_sequence=px.colors.sequential.Greens
                    )
                    fig.update_layout(height=400)
                    st.plotly_chart(fig, use_container_width=True)
            
            # Gráfico: Top 10 Maiores Multas
            if 'valor_numerico' in df_filtrado.columns:
                st.subheader("Top 10 Maiores Multas")
                
                top_multas = df_filtrado.nlargest(10, 'valor_numerico')[
                    ['nom_pessoa_infrator', 'valor_numerico', 'nom_uf', 'tipo_infracao']
                ].copy()
                
                fig = px.bar(
                    top_multas,
                    x='valor_numerico',
                    y='nom_pessoa_infrator',
                    orientation='h',
                    color='tipo_infracao',
                    labels={'valor_numerico': 'Valor (R$)', 'nom_pessoa_infrator': 'Infrator'},
                    color_discrete_sequence=px.colors.sequential.Greens
                )
                fig.update_layout(height=400, showlegend=True)
                st.plotly_chart(fig, use_container_width=True)
        
        with tab2:
            # Tabela completa
            st.subheader(f"📋 {len(df_filtrado):,} Registros Encontrados")
            
            # Colunas auxiliares excluídas da seleção do usuário (mas id é mantido internamente)
            colunas_excluir_selecao = ['valor_numerico', 'created_at', 'data_coleta', 'fonte',
                                       'num_sequencia', 'ser_auto_infracao', 'valor_auto_formatado',
                                       'id', 'status_contato']
            colunas_disponiveis = [col for col in df_filtrado.columns if col not in colunas_excluir_selecao]

            # Ordem preferencial para exibição
            ordem_preferencial = [
                'nom_pessoa_infrator',
                'num_cpf_cnpj',
                'tipo_infracao',
                'data_autuacao',
                'valor_multa_numerico',
                'nom_uf',
                'nom_municipio_auto',
                'des_tipo_bioma',
                'num_auto_infracao',
                'num_processo',
                'des_status_debito',
                'num_enquadramento',
            ]

            colunas_ordenadas = [c for c in ordem_preferencial if c in colunas_disponiveis]
            colunas_ordenadas += [c for c in colunas_disponiveis if c not in colunas_ordenadas]

            colunas_exibir = st.multiselect(
                "Selecione as colunas para exibir",
                colunas_ordenadas,
                default=colunas_ordenadas
            )

            if colunas_exibir:
                # Monta df com id (oculto) + status_contato (editável) + colunas selecionadas
                colunas_editor = ['id', 'status_contato'] + colunas_exibir
                colunas_editor = [c for c in colunas_editor if c in df_filtrado.columns]
                df_exibir = df_filtrado[colunas_editor].copy()

                # Converte tipos para ordenação correta
                if 'data_autuacao' in df_exibir.columns:
                    df_exibir['data_autuacao'] = pd.to_datetime(df_exibir['data_autuacao'], dayfirst=True, errors='coerce')
                if 'valor_multa_numerico' in df_exibir.columns:
                    df_exibir['valor_multa_numerico'] = pd.to_numeric(df_exibir['valor_multa_numerico'], errors='coerce')

                # Configuração de colunas
                column_config = {
                    'id': None,  # oculta o id
                    'status_contato': st.column_config.SelectboxColumn(
                        "Status Contato",
                        options=[''] + carregar_status_opcoes(),
                        required=False,
                        help="Status de acompanhamento do contato"
                    ),
                    'nom_pessoa_infrator': st.column_config.TextColumn("Nome do Infrator"),
                    'num_cpf_cnpj': st.column_config.TextColumn("CPF/CNPJ"),
                    'tipo_infracao': st.column_config.TextColumn("Tipo de Infração"),
                    'data_autuacao': st.column_config.DatetimeColumn("Data de Autuação", format="DD/MM/YYYY"),
                    'valor_multa_numerico': st.column_config.NumberColumn("Valor da Multa", format="R$ %.2f"),
                    'nom_uf': st.column_config.TextColumn("Estado"),
                    'nom_municipio_auto': st.column_config.TextColumn("Município"),
                    'des_tipo_bioma': st.column_config.TextColumn("Bioma"),
                    'num_auto_infracao': st.column_config.TextColumn("Nº Auto Infração"),
                    'num_processo': st.column_config.TextColumn("Nº Processo"),
                    'des_status_debito': st.column_config.TextColumn("Status Débito"),
                    'num_enquadramento': st.column_config.TextColumn("Enquadramento Legal", width="large"),
                }

                # Apenas status_contato é editável
                colunas_bloqueadas = [c for c in df_exibir.columns if c != 'status_contato']

                edited_df = st.data_editor(
                    df_exibir,
                    disabled=colunas_bloqueadas,
                    column_config=column_config,
                    hide_index=True,
                    use_container_width=True,
                    height=500,
                    key="data_editor_principal"
                )

                # Linha de ação: salvar + info + download
                col_salvar, col_info, col_download = st.columns([1, 2, 1])

                with col_salvar:
                    if st.button("💾 Salvar Alterações", use_container_width=True):
                        mudancas = [
                            {'id': int(row_id), 'status': new_s if new_s else None}
                            for row_id, orig_s, new_s in zip(
                                df_exibir['id'],
                                df_exibir['status_contato'],
                                edited_df['status_contato']
                            )
                            if str(orig_s) != str(new_s)
                        ]
                        if mudancas:
                            if salvar_status_alterados(mudancas):
                                st.success(f"✅ {len(mudancas)} registro(s) atualizados")
                                st.cache_data.clear()
                                st.rerun()
                        else:
                            st.info("Nenhuma alteração detectada")

                with col_info:
                    if 'status_contato' in df_exibir.columns:
                        com_status = df_exibir['status_contato'].notna().sum()
                        st.caption(f"{com_status} de {len(df_exibir)} registros com status de contato definido")

                with col_download:
                    df_download = edited_df.drop(columns=['id', 'status_contato'], errors='ignore').copy()
                    if 'valor_multa_numerico' in df_download.columns:
                        df_download['valor_multa_numerico'] = df_download['valor_multa_numerico'].apply(
                            lambda x: f"{float(x):,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.') if pd.notna(x) else ''
                        )
                    csv = df_download.to_csv(index=False, encoding='utf-8-sig')
                    st.download_button(
                        label="📥 Download CSV",
                        data=csv,
                        file_name=f"ambaudit_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                        mime="text/csv"
                    )
            else:
                st.warning("⚠️ Selecione pelo menos uma coluna para exibir")
        
        with tab3:
            st.subheader("🗺️ Distribuição Geográfica")
            
            if 'nom_uf' in df_filtrado.columns:
                # Tabela por estado
                estados_agg = df_filtrado.groupby('nom_uf').agg({
                    'id': 'count',
                    'valor_numerico': 'sum' if 'valor_numerico' in df_filtrado.columns else 'count'
                }).reset_index()
                estados_agg.columns = ['Estado', 'Quantidade', 'Valor Total (R$)']
                estados_agg = estados_agg.sort_values('Quantidade', ascending=False)
                
                st.dataframe(
                    estados_agg,
                    use_container_width=True,
                    hide_index=True
                )
            else:
                st.info("Dados geográficos não disponíveis")
    
    else:
        st.warning("⚠️ Nenhum registro encontrado com os filtros aplicados")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: #666;'>
            <p>AMBAUDIT - Plataforma de Inteligência em Infrações Ambientais</p>
            <p>Dados: IBAMA | Última atualização: """ + datetime.now().strftime('%d/%m/%Y %H:%M') + """</p>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()