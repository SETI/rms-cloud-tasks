# Run as tests/manual/run_cli_queue_tests.sh [provider] [args]
set -e
P=$1
ARGS="${@:2}"

echo
echo "****** Deleting queues ******"
echo
set -x
python -m src.cloud_tasks.cli delete_queue --config tests/manual/run_cli_config.yml --provider ${P} -f ${ARGS}
python -m src.cloud_tasks.cli delete_queue --config tests/manual/run_cli_config.yml --provider ${P} -f --queue-name my-test-queue ${ARGS}
set +x
if [ "$P" == "aws" ]; then
    sleep 62  # AWS requires at least 60 seconds between deleting and creating a queue
fi
echo

echo
echo "****** Loading first queue (80 items) ******"
echo
set -x
python -m src.cloud_tasks.cli load_queue --config tests/manual/run_cli_config.yml --provider ${P} --task-file tests/manual/run_cli_tasks.json ${ARGS}
python -m src.cloud_tasks.cli show_queue --config tests/manual/run_cli_config.yml --provider ${P} ${ARGS}
python -m src.cloud_tasks.cli show_queue --config tests/manual/run_cli_config.yml --provider ${P} --detail ${ARGS}
set +x
echo

echo
echo "****** Loading second queue (80 + 80 items) ******"
echo
set -x
python -m src.cloud_tasks.cli load_queue --config tests/manual/run_cli_config.yml --provider ${P} --queue-name my-test-queue --task-file tests/manual/run_cli_tasks.json ${ARGS}
python -m src.cloud_tasks.cli load_queue --config tests/manual/run_cli_config.yml --provider ${P} --queue-name my-test-queue --task-file tests/manual/run_cli_tasks.json ${ARGS}
python -m src.cloud_tasks.cli show_queue --config tests/manual/run_cli_config.yml --provider ${P} --queue-name my-test-queue ${ARGS}
python -m src.cloud_tasks.cli show_queue --config tests/manual/run_cli_config.yml --provider ${P} ${ARGS}
set +x

echo
echo "****** Purging first queue (second queue should not be affected) ******"
echo
set -x
python -m src.cloud_tasks.cli purge_queue --config tests/manual/run_cli_config.yml --provider ${P} -f ${ARGS}
python -m src.cloud_tasks.cli purge_queue --config tests/manual/run_cli_config.yml --provider ${P} -f ${ARGS}
python -m src.cloud_tasks.cli show_queue --config tests/manual/run_cli_config.yml --provider ${P} ${ARGS}
python -m src.cloud_tasks.cli show_queue --config tests/manual/run_cli_config.yml --provider ${P} --queue-name my-test-queue ${ARGS}
set +x

echo
echo "****** Deleting queues ******"
echo
set -x
python -m src.cloud_tasks.cli delete_queue --config tests/manual/run_cli_config.yml --provider ${P} -f ${ARGS}
python -m src.cloud_tasks.cli delete_queue --config tests/manual/run_cli_config.yml --provider ${P} -f --queue-name my-test-queue ${ARGS}
set +x
