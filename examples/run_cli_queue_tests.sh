set -x
P=$1
python -m src.cloud_tasks.cli delete_queue --config examples/run_cli_config.yml --provider ${P} -f
python -m src.cloud_tasks.cli delete_queue --config examples/run_cli_config.yml --provider ${P} -f --queue-name my-test-queue
if [ "$P" == "aws" ]; then
    sleep 62
fi
echo

python -m src.cloud_tasks.cli load_queue --config examples/run_cli_config.yml --provider ${P} --tasks examples/run_cli_tasks.json
python -m src.cloud_tasks.cli show_queue --config examples/run_cli_config.yml --provider ${P}
python -m src.cloud_tasks.cli show_queue --config examples/run_cli_config.yml --provider ${P} --detail
echo

python -m src.cloud_tasks.cli load_queue --config examples/run_cli_config.yml --provider ${P} --queue-name my-test-queue --tasks examples/run_cli_tasks.json
python -m src.cloud_tasks.cli load_queue --config examples/run_cli_config.yml --provider ${P} --queue-name my-test-queue --tasks examples/run_cli_tasks.json
python -m src.cloud_tasks.cli show_queue --config examples/run_cli_config.yml --provider ${P} --queue-name my-test-queue
python -m src.cloud_tasks.cli show_queue --config examples/run_cli_config.yml --provider ${P}
echo

python -m src.cloud_tasks.cli purge_queue --config examples/run_cli_config.yml --provider ${P} -f
python -m src.cloud_tasks.cli purge_queue --config examples/run_cli_config.yml --provider ${P} -f
python -m src.cloud_tasks.cli show_queue --config examples/run_cli_config.yml --provider ${P}
python -m src.cloud_tasks.cli show_queue --config examples/run_cli_config.yml --provider ${P} --queue-name my-test-queue
python -m src.cloud_tasks.cli delete_queue --config examples/run_cli_config.yml --provider ${P} -f
python -m src.cloud_tasks.cli delete_queue --config examples/run_cli_config.yml --provider ${P} -f --queue-name my-test-queue
