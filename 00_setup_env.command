echo "🔍 Checking if Homebrew is installed..."
if ! command -v brew &> /dev/null
then
    echo "🍺 Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"
else
    echo "✅ Homebrew is already installed."
fi

echo "🐍 Checking if Python 3 is installed..."
if ! command -v python3 &> /dev/null
then
    echo "🔧 Installing Python 3..."
    brew install python
else
    echo "✅ Python 3 is already installed."
fi

python3 -m pip install --upgrade pip

echo "📄 Installing from requirements.txt..."

cd "$(dirname "$0")"

if [ ! -f "requirements.txt" ]; then
    echo "❌ ERROR: requirements.txt not found in the current directory."
    exit 1
fi

if python3 -m pip install -r requirements.txt; then
    read -p "🎉 requirements installed!!! Press enter to close this window" 
    exit
else
    echo "❌ ERROR: Failed to install dependencies."
    exit 1
fi


