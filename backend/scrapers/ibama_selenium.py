"""
AMBAUDIT - Scraper HTML Completo
Copia tabela página por página - SEM depender de export
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select
from selenium.webdriver.chrome.service import Service
from selenium.common.exceptions import TimeoutException
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time
import os
import json
import random
from datetime import datetime
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

class IBAMAScraperHTML:
    """
    Scraper que copia HTML da tabela - método mais confiável
    """
    
    def __init__(self):
        self.url = "https://servicos.ibama.gov.br/ctf/publico/areasembargadas/ConsultaPublicaAreasEmbargadas.php"
        self.data_dir = os.path.join(os.path.dirname(__file__), '..', '..', 'data', 'raw')
        os.makedirs(self.data_dir, exist_ok=True)
        
        self.checkpoint_file = os.path.join(self.data_dir, 'checkpoint_html_completo.json')
        
        # Conecta ao Supabase
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_KEY")
        
        if supabase_url and supabase_key:
            self.supabase: Client = create_client(supabase_url, supabase_key)
            print("✅ Conectado ao Supabase")
        else:
            self.supabase = None
        
        # Stats
        self.stats = {
            'total_sessoes': 0,
            'total_paginas': 0,
            'total_registros_am': 0,
            'ultima_execucao': None
        }
    
    def carregar_checkpoint(self):
        """Carrega checkpoint global"""
        if os.path.exists(self.checkpoint_file):
            try:
                with open(self.checkpoint_file, 'r') as f:
                    checkpoint = json.load(f)
                    checkpoint['paginas_coletadas'] = set(checkpoint.get('paginas_coletadas', []))
                    return checkpoint
            except:
                return None
        return None
    
    def salvar_checkpoint(self, paginas_coletadas, stats):
        """Salva checkpoint"""
        checkpoint = {
            'paginas_coletadas': list(paginas_coletadas),
            'stats': stats,
            'timestamp': datetime.now().isoformat()
        }
        with open(self.checkpoint_file, 'w') as f:
            json.dump(checkpoint, f, indent=2)
    
    def gerar_proximas_paginas(self, total_paginas, paginas_coletadas, max_novas=100):
        """Gera próximas páginas a coletar"""
        todas = set(range(1, total_paginas + 1))
        faltantes = todas - paginas_coletadas
        
        if not faltantes:
            return []
        
        proximas = list(faltantes)[:max_novas]
        random.shuffle(proximas)
        
        return proximas
    
    def configurar_driver(self):
        """Configura Chrome"""
        print("🌐 Configurando navegador...")
        
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        driver = webdriver.Chrome(
            service=Service(ChromeDriverManager().install()),
            options=chrome_options
        )
        driver.maximize_window()
        
        return driver
    
    def aguardar_tabela(self, driver, timeout=10):
        """Aguarda tabela carregar"""
        try:
            wait = WebDriverWait(driver, timeout)
            wait.until(EC.presence_of_element_located((By.ID, "grid_autuacoes_ambientais_table")))
            wait.until(EC.presence_of_element_located((By.CLASS_NAME, "fwGridRow")))
            time.sleep(0.5)
            return True
        except TimeoutException:
            return False
    
    def extrair_tabela_html(self, driver):
        """
        Extrai TODA a tabela da página atual
        """
        try:
            html = driver.page_source
            soup = BeautifulSoup(html, 'html.parser')
            
            tabela = soup.find('table', {'id': 'grid_autuacoes_ambientais_table'})
            if not tabela:
                return None, None
            
            # Extrai headers
            headers = []
            thead = tabela.find('thead')
            if thead:
                for th in thead.find_all('th'):
                    col_name = th.get('column_name', '')
                    if col_name:
                        headers.append(col_name)
            
            # Extrai dados
            dados = []
            tbody = tabela.find('tbody')
            if tbody:
                linhas = tbody.find_all('tr', class_='fwGridRow')
                
                for linha in linhas:
                    celulas = linha.find_all('td', class_='fwGridCell')
                    if celulas and len(celulas) == len(headers):
                        linha_dados = [cel.get_text(strip=True) for cel in celulas]
                        dados.append(linha_dados)
            
            return headers, dados
            
        except Exception as e:
            print(f"  ❌ Erro ao extrair: {e}")
            return None, None
    
    def filtrar_amazonas(self, dados, headers):
        """Filtra registros do Amazonas"""
        if not dados or not headers:
            return []
        
        # Encontra índice da coluna UF
        try:
            idx_uf = headers.index('nom_uf')
        except:
            return dados  # Se não tem coluna UF, retorna tudo
        
        # Filtra
        amazonas = []
        for linha in dados:
            try:
                uf = linha[idx_uf].strip().upper()
                if uf in ['AM', 'AMAZONAS']:
                    amazonas.append(linha)
            except:
                continue
        
        return amazonas
    
    def navegar_para_pagina(self, driver, numero_pagina):
        """Navega para página específica"""
        try:
            selects = driver.find_elements(By.TAG_NAME, "select")
            for select_elem in selects:
                try:
                    select = Select(select_elem)
                    select.select_by_value(str(numero_pagina))
                    time.sleep(1)
                    return True
                except:
                    try:
                        select.select_by_visible_text(str(numero_pagina))
                        time.sleep(1)
                        return True
                    except:
                        continue
        except:
            pass
        
        try:
            links = driver.find_elements(By.XPATH, f"//a[text()='{numero_pagina}']")
            if links:
                links[0].click()
                time.sleep(1)
                return True
        except:
            pass
        
        return False
    
    def salvar_supabase(self, df, descricao):
        """Salva no Supabase"""
        if not self.supabase or len(df) == 0:
            return False
        
        try:
            print(f"  ☁️ {descricao}...", end=' ')
            registros = df.to_dict('records')
            self.supabase.table('raw_ibama_autuacoes').insert(registros).execute()
            print(f"✅ {len(registros)} AM")
            return True
        except Exception as e:
            print(f"❌ {str(e)[:60]}")
            
            # Backup CSV
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            csv_path = os.path.join(self.data_dir, f'backup_{timestamp}.csv')
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
            print(f"  💾 Backup: {csv_path}")
            
            return False
    
    def executar_sessao(self, total_paginas=1388, max_paginas_por_sessao=100):
        """
        Executa uma sessão de coleta
        """
        
        # Carrega checkpoint
        checkpoint = self.carregar_checkpoint()
        
        if checkpoint:
            paginas_coletadas = checkpoint['paginas_coletadas']
            self.stats = checkpoint.get('stats', self.stats)
            
            print(f"\n📊 HISTÓRICO GLOBAL:")
            print(f"   Sessões: {self.stats['total_sessoes']}")
            print(f"   Páginas coletadas: {len(paginas_coletadas)}/{total_paginas}")
            print(f"   Registros AM: {self.stats['total_registros_am']}")
            print(f"   Progresso: {len(paginas_coletadas)/total_paginas*100:.1f}%")
            
            if len(paginas_coletadas) >= total_paginas:
                print("\n✅ TODAS AS PÁGINAS JÁ FORAM COLETADAS!")
                return True
        else:
            paginas_coletadas = set()
            print("\n🆕 PRIMEIRA SESSÃO")
        
        print(f"\n🌳 AMBAUDIT - Sessão #{self.stats['total_sessoes'] + 1}")
        print(f"🎯 Filtro: Amazonas")
        print(f"💾 Salva a cada 20 páginas\n")
        
        driver = None
        dados_acumulados = {}
        paginas_nesta_sessao = set()
        headers = None
        
        try:
            driver = self.configurar_driver()
            print("✅ Navegador pronto\n")
            
            driver.get(self.url)
            time.sleep(3)
            
            print("="*60)
            print("🤖 RESOLVA CAPTCHA + ENTRE")
            print("="*60)
            input("⏸️  ENTER... ")
            
            # Datas
            print("\n📅 Preenchendo datas...")
            try:
                hoje = datetime.now()
                data_fim = hoje.strftime('%d/%m/%Y')
                data_inicio = hoje.replace(year=hoje.year-1).strftime('%d/%m/%Y')
                
                campo_inicial = driver.find_element(By.NAME, "dat_inicial")
                campo_inicial.clear()
                campo_inicial.send_keys(data_inicio)
                
                campo_final = driver.find_element(By.NAME, "dat_final")
                campo_final.clear()
                campo_final.send_keys(data_fim)
                
                print(f"✅ {data_inicio} até {data_fim}")
            except:
                print("⚠️ Erro nas datas")
            
            print("\n🔍 CLIQUE EM CONSULTAR")
            input("⏸️  ENTER após clicar... ")
            time.sleep(3)
            
            if not self.aguardar_tabela(driver):
                print("❌ Tabela não carregou")
                return False
            
            # Gera próximas páginas
            proximas = self.gerar_proximas_paginas(
                total_paginas,
                paginas_coletadas,
                max_paginas_por_sessao
            )
            
            if not proximas:
                print("\n✅ Não há mais páginas a coletar!")
                return True
            
            print(f"\n🎯 {len(proximas)} páginas nesta sessão")
            print(f"   Exemplos: {proximas[:10]}...\n")
            
            inicio = datetime.now()
            
            # Loop de coleta
            for idx, num_pagina in enumerate(proximas):
                
                total_coletadas = len(paginas_coletadas) + len(paginas_nesta_sessao)
                progresso = total_coletadas / total_paginas * 100
                regs_am = sum(len(d) for d in dados_acumulados.values())
                
                print(f"[{total_coletadas:4d}/{total_paginas}] Pág {num_pagina:4d} | {progresso:5.1f}% | AM: {regs_am:4d}", end='')
                
                # Navega
                if num_pagina != 1 or idx > 0:
                    if not self.navegar_para_pagina(driver, num_pagina):
                        print(" ❌ nav")
                        continue
                    
                    if not self.aguardar_tabela(driver, timeout=15):
                        print(" ❌ timeout")
                        
                        # Salva emergência
                        if dados_acumulados and headers:
                            todos_dados = []
                            for pag in sorted(dados_acumulados.keys()):
                                todos_dados.extend(dados_acumulados[pag])
                            
                            if todos_dados:
                                df = pd.DataFrame(todos_dados, columns=headers)
                                self.salvar_supabase(df, "EMERGÊNCIA")
                                dados_acumulados = {}
                        
                        time.sleep(5)
                        if not self.aguardar_tabela(driver, timeout=15):
                            print(f"\n⚠️ Bloqueado após {len(paginas_nesta_sessao)} páginas")
                            break
                
                # Extrai HTML
                headers_pag, dados_pag = self.extrair_tabela_html(driver)
                
                if headers is None and headers_pag:
                    headers = headers_pag
                
                if dados_pag:
                    # Filtra Amazonas
                    dados_am = self.filtrar_amazonas(dados_pag, headers_pag)
                    
                    if dados_am:
                        dados_acumulados[num_pagina] = dados_am
                        print(f" ✓ ({len(dados_am)} AM)")
                    else:
                        print(f" ⚠️ (0 AM)")
                    
                    paginas_nesta_sessao.add(num_pagina)
                else:
                    print(f" ⚠️ sem dados")
                    paginas_nesta_sessao.add(num_pagina)
                
                # Salva a cada 20 páginas
                if len(paginas_nesta_sessao) % 20 == 0 and dados_acumulados:
                    todos_dados = []
                    for pag in sorted(dados_acumulados.keys()):
                        todos_dados.extend(dados_acumulados[pag])
                    
                    if todos_dados and headers:
                        df = pd.DataFrame(todos_dados, columns=headers)
                        self.salvar_supabase(df, f"Lote {len(paginas_nesta_sessao)}")
                        
                        # Atualiza stats
                        self.stats['total_registros_am'] += len(todos_dados)
                        
                        # Atualiza checkpoint
                        paginas_coletadas.update(paginas_nesta_sessao)
                        self.stats['total_paginas'] = len(paginas_coletadas)
                        self.salvar_checkpoint(paginas_coletadas, self.stats)
                        
                        dados_acumulados = {}
                
                time.sleep(0.5)
            
            # Salva final
            if dados_acumulados and headers:
                print(f"\n💾 Salvando últimas páginas...")
                todos_dados = []
                for pag in sorted(dados_acumulados.keys()):
                    todos_dados.extend(dados_acumulados[pag])
                
                df = pd.DataFrame(todos_dados, columns=headers)
                self.salvar_supabase(df, "FINAL")
                
                self.stats['total_registros_am'] += len(todos_dados)
            
            # Atualiza checkpoint final
            paginas_coletadas.update(paginas_nesta_sessao)
            self.stats['total_sessoes'] += 1
            self.stats['total_paginas'] = len(paginas_coletadas)
            self.stats['ultima_execucao'] = datetime.now().isoformat()
            self.salvar_checkpoint(paginas_coletadas, self.stats)
            
            # Estatísticas
            tempo = (datetime.now() - inicio).total_seconds()
            
            print(f"\n{'='*60}")
            print(f"🎉 SESSÃO CONCLUÍDA!")
            print(f"{'='*60}")
            print(f"⏱️  Tempo: {tempo/60:.1f} min")
            print(f"📄 Páginas nesta sessão: {len(paginas_nesta_sessao)}")
            print(f"\n📊 TOTAL GLOBAL:")
            print(f"   Sessões: {self.stats['total_sessoes']}")
            print(f"   Páginas: {len(paginas_coletadas)}/{total_paginas} ({len(paginas_coletadas)/total_paginas*100:.1f}%)")
            print(f"   Registros AM: ~{self.stats['total_registros_am']}")
            print(f"   Faltam: {total_paginas - len(paginas_coletadas)} páginas")
            print(f"{'='*60}\n")
            
            return True
            
        except KeyboardInterrupt:
            print("\n\n⚠️ INTERROMPIDO")
            
            # Salva tudo
            if dados_acumulados and headers:
                todos_dados = []
                for pag in sorted(dados_acumulados.keys()):
                    todos_dados.extend(dados_acumulados[pag])
                
                if todos_dados:
                    df = pd.DataFrame(todos_dados, columns=headers)
                    self.salvar_supabase(df, "INTERRUPÇÃO")
            
            # Checkpoint
            paginas_coletadas.update(paginas_nesta_sessao)
            self.stats['total_sessoes'] += 1
            self.stats['total_paginas'] = len(paginas_coletadas)
            self.salvar_checkpoint(paginas_coletadas, self.stats)
            
            return False
            
        except Exception as e:
            print(f"\n❌ Erro: {e}")
            import traceback
            traceback.print_exc()
            return False
            
        finally:
            if driver:
                print("\n🔒 Fechando navegador...")
                driver.quit()


if __name__ == "__main__":
    scraper = IBAMAScraperHTML()
    
    print("\n" + "="*60)
    print("🌳 AMBAUDIT - Scraper HTML Completo")
    print("="*60)
    print("\n💡 MÉTODO MAIS CONFIÁVEL:")
    print("   1. Copia HTML da tabela página por página")
    print("   2. Filtra Amazonas no código")
    print("   3. Salva incrementalmente")
    print("   4. Checkpoint global entre sessões")
    print("\n⚡ Sistema de sessões:")
    print("   - Cada sessão: ~60-100 páginas (~6-10 min)")
    print("   - Bloqueia? Roda de novo!")
    print("   - Nunca coleta duplicado")
    print("   - Acumula progresso\n")
    
    print("OPÇÕES:")
    print("1. Rodar NOVA sessão")
    print("2. Ver progresso")
    print("3. Resetar checkpoint")
    
    opcao = input("\nEscolha (1-3): ")
    
    if opcao == '2':
        checkpoint = scraper.carregar_checkpoint()
        if checkpoint:
            stats = checkpoint['stats']
            paginas = checkpoint['paginas_coletadas']
            
            print(f"\n📊 PROGRESSO:")
            print(f"   Sessões: {stats.get('total_sessoes', 0)}")
            print(f"   Páginas: {len(paginas)}/1388 ({len(paginas)/1388*100:.1f}%)")
            print(f"   Registros AM: ~{stats.get('total_registros_am', 0)}")
            print(f"   Última execução: {stats.get('ultima_execucao', 'N/A')}")
        else:
            print("\n⚠️ Nenhuma sessão executada")
    
    elif opcao == '3':
        confirma = input("\n⚠️ Apagar TODO histórico? (s/n): ")
        if confirma.lower() == 's':
            if os.path.exists(scraper.checkpoint_file):
                os.remove(scraper.checkpoint_file)
            print("✅ Checkpoint limpo")
    
    elif opcao == '1':
        print(f"\n🚀 Iniciando sessão...")
        
        sucesso = scraper.executar_sessao(
            total_paginas=1388,
            max_paginas_por_sessao=100
        )
        
        if sucesso:
            checkpoint = scraper.carregar_checkpoint()
            if checkpoint:
                paginas = len(checkpoint['paginas_coletadas'])
                faltam = 1388 - paginas
                
                print(f"\n📈 PRÓXIMOS PASSOS:")
                print(f"   Páginas coletadas: {paginas}/1388")
                
                if faltam > 0:
                    sessoes_faltam = (faltam // 60) + 1
                    print(f"   Faltam ~{sessoes_faltam} sessões")
                    print(f"   Tempo estimado: ~{sessoes_faltam * 7} minutos")
                    print(f"\n💡 Rode de novo para continuar!")
                else:
                    print(f"\n🎉 COLETA COMPLETA!")
                
                print(f"\n🎨 Dashboard: streamlit run frontend/app.py")
        
    else:
        print("\n👋 Cancelado")