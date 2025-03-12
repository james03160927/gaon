@echo off
echo Checking Python installation...
python --version

echo.
echo Creating virtual environment...
python -m venv venv

echo.
echo Activating virtual environment...
call venv\Scripts\activate

echo.
echo Installing package...
pip install -e .

echo.
echo Checking installation location...
where gaon

echo.
echo Checking if gaon is installed...
pip list | findstr gaon

echo.
echo Installation complete. Try running:
echo gaon --help
pause 