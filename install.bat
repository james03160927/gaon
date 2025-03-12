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
echo Installing dependencies from requirements.txt...
pip install -r requirements.txt

echo.
echo Installing package...
pip install -e .

echo.
echo Checking installation location...
where gaon

echo.
echo Checking installed packages...
pip list

echo.
echo Installation complete. Try running:
echo gaon --help
pause 