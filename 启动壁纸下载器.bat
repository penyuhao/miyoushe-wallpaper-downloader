@echo off
chcp 65001 >nul
title 米游社壁纸下载器 v3.1
cd /d "%~dp0"

echo.
echo ================================================
echo     米游社壁纸下载器  v3.1
echo     纯 Python 标准库 · 零依赖 · 并行扫描 + 多线程下载
echo ================================================
echo.
echo 正在启动服务，请稍候...
echo.

rem 优先用 python，找不到再退回 py 启动器
where python >nul 2>nul
if %errorlevel%==0 (
    python "米游社壁纸下载器.py"
    goto :done
)

where py >nul 2>nul
if %errorlevel%==0 (
    py -3 "米游社壁纸下载器.py"
    goto :done
)

echo [错误] 没有找到 Python，无法启动。
echo.
echo 请先安装 Python 3.7 或更高版本： https://www.python.org/downloads/
echo 安装时记得勾选 "Add Python to PATH"。
echo.
pause
exit /b 1

:done
if errorlevel 1 (
    echo.
    echo [错误] 程序非正常退出，请查看上方提示信息。
    echo.
    pause
)
