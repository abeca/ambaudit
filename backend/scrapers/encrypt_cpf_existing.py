"""
AMBAUDIT - Migração: Criptografa CPF/CNPJ existentes no banco
Execute UMA VEZ após definir ENCRYPTION_KEY no .env
"""

from cryptography.fernet import Fernet
from supabase import create_client, Client
from dotenv import load_dotenv
import os

load_dotenv()


def get_fernet():
    key = os.getenv("ENCRYPTION_KEY")
    if not key:
        raise ValueError("ENCRYPTION_KEY não encontrado no .env")
    return Fernet(key.encode())


def ja_criptografado(valor):
    """Verifica se o valor já está criptografado (começa com 'gAAAAA')"""
    if not valor:
        return False
    return str(valor).startswith('gAAAAA')


def migrar():
    supabase_url = os.getenv("SUPABASE_URL")
    supabase_key = os.getenv("SUPABASE_KEY")
    supabase: Client = create_client(supabase_url, supabase_key)
    fernet = get_fernet()

    print("\n" + "="*60)
    print("🔐 AMBAUDIT - Criptografia de CPF/CNPJ")
    print("="*60)
    print("⚠️  Execute apenas UMA VEZ!")
    print("="*60)

    confirma = input("\nConfirmar migração? (s/n): ")
    if confirma.lower() != 's':
        print("❌ Cancelado")
        return

    # Carrega todos os registros com CPF/CNPJ
    print("\n📥 Carregando registros...")
    todos = []
    offset = 0
    page_size = 1000

    while True:
        resp = supabase.table('raw_ibama_autuacoes')\
            .select('id, num_cpf_cnpj')\
            .range(offset, offset + page_size - 1)\
            .execute()

        if not resp.data:
            break
        todos.extend(resp.data)
        offset += page_size
        print(f"  {len(todos)} carregados...", end='\r')

        if len(resp.data) < page_size:
            break

    print(f"\n✅ {len(todos)} registros carregados")

    # Criptografa e atualiza em lotes
    atualizados = 0
    ignorados = 0
    erros = 0
    lote_size = 100

    print("\n🔐 Criptografando...")

    for i, reg in enumerate(todos):
        cpf = reg.get('num_cpf_cnpj')

        if not cpf or cpf in ('None', '', 'nan'):
            ignorados += 1
            continue

        if ja_criptografado(cpf):
            ignorados += 1
            continue

        try:
            cpf_enc = fernet.encrypt(str(cpf).encode()).decode()
            supabase.table('raw_ibama_autuacoes')\
                .update({'num_cpf_cnpj': cpf_enc})\
                .eq('id', reg['id'])\
                .execute()
            atualizados += 1
        except Exception as e:
            erros += 1

        if (i + 1) % lote_size == 0:
            print(f"  [{i+1}/{len(todos)}] ✅ {atualizados} | ⏭️ {ignorados} | ❌ {erros}", end='\r')

    print(f"\n\n{'='*60}")
    print("🎉 MIGRAÇÃO CONCLUÍDA!")
    print(f"{'='*60}")
    print(f"  ✅ Criptografados: {atualizados}")
    print(f"  ⏭️  Ignorados (vazios/já enc.): {ignorados}")
    print(f"  ❌ Erros: {erros}")
    print(f"{'='*60}\n")


if __name__ == "__main__":
    migrar()
