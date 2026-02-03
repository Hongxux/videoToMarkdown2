@echo off
REM gRPC代码生成脚本 (Windows)
REM 
REM 用法: .\generate_grpc.bat
REM 
REM 需要先安装:
REM   - grpcio-tools: pip install grpcio-tools

echo ========================================
echo gRPC Code Generator for Video Processing
echo ========================================

SET SCRIPT_DIR=%~dp0
SET PROTO_DIR=%SCRIPT_DIR%proto
SET PYTHON_OUT=%SCRIPT_DIR%generated_grpc
SET PYTHON_TARGET=%SCRIPT_DIR%MVP_Module2_HEANCING\module2_content_enhancement\grpc_generated

echo.
echo Proto directory: %PROTO_DIR%
echo Python output: %PYTHON_OUT%
echo.

REM 创建输出目录
if not exist "%PYTHON_OUT%" mkdir "%PYTHON_OUT%"
if not exist "%PYTHON_TARGET%" mkdir "%PYTHON_TARGET%"

echo [1/3] Generating Python gRPC code...
python -m grpc_tools.protoc ^
    -I%PROTO_DIR% ^
    --python_out=%PYTHON_OUT% ^
    --grpc_python_out=%PYTHON_OUT% ^
    %PROTO_DIR%\video_processing.proto

if %ERRORLEVEL% NEQ 0 (
    echo [ERROR] Python gRPC generation failed!
    echo.
    echo Make sure grpcio-tools is installed:
    echo   pip install grpcio-tools
    pause
    exit /b 1
)
echo [OK] Python gRPC code generated

echo.
echo [2/3] Copying Python gRPC files to project...
copy /Y "%PYTHON_OUT%\video_processing_pb2.py" "%PYTHON_TARGET%\"
copy /Y "%PYTHON_OUT%\video_processing_pb2_grpc.py" "%PYTHON_TARGET%\"

REM 创建 __init__.py
echo # Generated gRPC code > "%PYTHON_TARGET%\__init__.py"
echo from .video_processing_pb2 import * >> "%PYTHON_TARGET%\__init__.py"
echo from .video_processing_pb2_grpc import * >> "%PYTHON_TARGET%\__init__.py"

echo [OK] Python files copied to: %PYTHON_TARGET%

echo.
echo [Extra] Copying Python gRPC files to root proto folder (for server)...
copy /Y "%PYTHON_OUT%\video_processing_pb2.py" "%PROTO_DIR%\"
copy /Y "%PYTHON_OUT%\video_processing_pb2_grpc.py" "%PROTO_DIR%\"
echo [OK] Python files copied to: %PROTO_DIR%

echo.
echo [3/3] Java gRPC code generation...
echo Note: Java gRPC is auto-generated via Maven protobuf plugin during build.
echo       Just run: mvn compile
echo.

echo ========================================
echo Generation completed!
echo ========================================
echo.
echo Generated files:
echo   [Python]
echo     %PYTHON_TARGET%\video_processing_pb2.py
echo     %PYTHON_TARGET%\video_processing_pb2_grpc.py
echo.
echo   [Java]
echo     Auto-generated during: mvn compile
echo.
echo Next steps:
echo   1. Start Python gRPC Server:
echo      python python_grpc_server.py
echo.
echo   2. Start Java Spring Boot:
echo      cd MVP_Module2_HEANCING\enterprise_services\java_orchestrator
echo      mvn spring-boot:run
echo.
echo   3. Open Frontend:
echo      MVP_Module2_HEANCING\enterprise_services\frontend\index.html
echo.
pause
