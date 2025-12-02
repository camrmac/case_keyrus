@echo off
REM Script para configurar e testar conexão Azure
REM Para Windows

echo ============================================
echo   Setup Pipeline Azure - Keyrus Data Lake
echo ============================================
echo.

REM Verificar se .env existe
if not exist .env (
    echo [1/4] Criando arquivo .env...
    copy AZURE_ENV_TEMPLATE.txt .env
    echo.
    echo IMPORTANTE: Edite o arquivo .env e preencha suas credenciais!
    echo   - AZURE_STORAGE_ACCOUNT_NAME
    echo   - AZURE_STORAGE_ACCOUNT_KEY
    echo   - AZURE_CONTAINER_NAME
    echo.
    notepad .env
    echo.
    pause
) else (
    echo [1/4] Arquivo .env ja existe - OK
)

echo.
echo [2/4] Verificando dependencias...
pip install -r requirements.txt --quiet
echo Dependencias instaladas - OK
echo.

echo [3/4] Testando conexao com Azure...
echo.
python run_pipeline_azure.py --test-connection

if %ERRORLEVEL% EQU 0 (
    echo.
    echo [4/4] Criando estrutura de pastas no Azure...
    echo.
    python run_pipeline_azure.py --create-structure
    
    echo.
    echo ============================================
    echo   Setup Concluido com Sucesso!
    echo ============================================
    echo.
    echo Proximo passo:
    echo   python run_pipeline_azure.py --stage full
    echo.
) else (
    echo.
    echo ============================================
    echo   Erro na Conexao!
    echo ============================================
    echo.
    echo Verifique:
    echo   1. Arquivo .env esta preenchido corretamente
    echo   2. AZURE_STORAGE_ACCOUNT_KEY esta completa
    echo   3. AZURE_STORAGE_ACCOUNT_NAME esta correto
    echo   4. Container existe no Azure
    echo.
)

pause

