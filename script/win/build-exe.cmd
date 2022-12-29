@ECHO OFF & SETLOCAL
for %%i in ("%~dp0..") DO SET "folder=%%~fi"
@ECHO ON
del /S /Q "%folder%\dist\"
ECHO D | xcopy /Y /E "%folder%\assets" "%folder%\dist\assets"
ECHO D | xcopy /Y /E "%folder%\tools" "%folder%\dist\tools"
conda run -n ppe pyinstaller "%folder%\app.spec" --distpath %folder%\dist\ --workpath %folder%\build
