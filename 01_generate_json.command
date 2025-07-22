echo "Start Generating Json file"

cd "$(dirname "$0")"

if python3 ./code/make_template_json.py; then
    read -p "🎉 Successful!! Please check output folder and publish the card" 
    exit
else
    echo "❌ ERROR: please check the env set up by running: 00_setup_env.command"
    exit 1
fi