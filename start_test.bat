@echo off
REM 端到端测试启动脚本 - Windows批处理

echo ========================================
echo 端到端测试 - 服务启动
echo ========================================
echo.

REM 检查conda环境
echo [1/5] 检查Conda环境...
call conda activate whisper_env
if %errorlevel% neq 0 (
    echo ERROR: 无法激活whisper_env环境
    echo 请先创建环境: conda create -n whisper_env python=3.11
    pause
    exit /b 1
)

REM 运行前置条件检查
echo.
echo [2/5] 检查前置条件...
python check_prerequisites.py
if %errorlevel% neq 0 (
    echo.
    echo ERROR: 前置条件检查失败，请先解决上述问题
    pause
    exit /b 1
)

echo.
echo [3/5] 准备启动服务...
echo.
echo ========================================
echo 请在不同终端窗口中运行以下命令:
echo ========================================
echo.
echo Terminal 1 - Java Backend:
echo   cd java-backend
echo   mvn spring-boot:run
echo.
echo Terminal 2 - Python Worker (当前窗口):
echo   conda activate whisper_env
echo   cd videoToMarkdown
echo   python worker_manager.py
echo.
echo Terminal 3 - Frontend:
echo   cd frontend
echo   npm run dev
echo.
echo ========================================
echo 服务启动后访问: http://localhost:5173
echo ========================================
echo.

pause
