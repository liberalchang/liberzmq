@echo off
chcp 65001 >nul
setlocal enabledelayedexpansion

echo LiberZMQ构建脚本
echo ==================================================

REM 检查Python是否可用
python --version >nul 2>&1
if errorlevel 1 (
    echo 错误: Python未安装或不在PATH中
    pause
    exit /b 1
)

REM 解析命令行参数
set "ACTION=%1"
if "%ACTION%"=="" set "ACTION=all"

if "%ACTION%"=="clean" goto :clean
if "%ACTION%"=="deps" goto :deps
if "%ACTION%"=="test" goto :test
if "%ACTION%"=="docs" goto :docs
if "%ACTION%"=="package" goto :package
if "%ACTION%"=="all" goto :all
if "%ACTION%"=="help" goto :help

echo 未知参数: %ACTION%
goto :help

:help
echo 用法: build.bat [action]
echo.
echo 可用的操作:
echo   clean   - 清理构建文件
echo   deps    - 安装依赖
echo   test    - 运行测试
echo   docs    - 构建文档
echo   package - 构建包
echo   all     - 执行所有步骤 (默认)
echo   help    - 显示此帮助
echo.
pause
exit /b 0

:clean
echo 清理构建文件...
if exist build rmdir /s /q build
if exist dist rmdir /s /q dist
if exist liberzmq.egg-info rmdir /s /q liberzmq.egg-info
if exist docs\_build rmdir /s /q docs\_build

REM 清理__pycache__目录
for /d /r . %%d in (__pycache__) do (
    if exist "%%d" rmdir /s /q "%%d"
)

echo 构建文件清理完成
if "%ACTION%"=="clean" goto :end
goto :eof

:deps
echo 安装依赖...
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
python -m pip install sphinx sphinx-rtd-theme pytest pytest-cov wheel twine
echo 依赖安装完成
if "%ACTION%"=="deps" goto :end
goto :eof

:test
echo 运行测试...
python -m casetest.run_tests --quick
if errorlevel 1 (
    echo 警告: 测试失败，但继续构建
)
echo 测试完成
if "%ACTION%"=="test" goto :end
goto :eof

:docs
echo 构建Sphinx文档...
if not exist docs (
    echo 错误: docs目录不存在
    goto :error
)

cd docs
sphinx-build -b html . _build\html
if errorlevel 1 (
    echo 错误: Sphinx文档构建失败
    cd ..
    goto :error
)
cd ..
echo 文档构建完成
if "%ACTION%"=="docs" goto :end
goto :eof

:package
echo 构建Python包...
python setup.py sdist bdist_wheel
if errorlevel 1 (
    echo 错误: 包构建失败
    goto :error
)

echo 检查包完整性...
twine check dist\*
if errorlevel 1 (
    echo 警告: 包检查失败
)

echo Python包构建完成
if "%ACTION%"=="package" goto :end
goto :eof

:all
call :clean
call :deps
call :test
call :docs
call :package

echo.
echo ==================================================
echo 构建完成！
echo.
echo 生成的文件:
if exist dist (
    for %%f in (dist\*) do echo   - %%f
)
if exist docs\_build\html\index.html (
    echo   - docs\_build\html\index.html (文档)
)
echo.
echo 使用说明:
echo   安装包: pip install dist\liberzmq-1.0.0-py3-none-any.whl
echo   查看文档: 打开 docs\_build\html\index.html
echo   发布包: twine upload dist\*
echo.
goto :end

:error
echo.
echo 构建过程中出现错误！
pause
exit /b 1

:end
echo.
echo 操作完成
pause
exit /b 0