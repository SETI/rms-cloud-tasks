# Run as tests/manual/run_cli_image_tests.sh [provider] [args]
set -e
P=$1
ARGS=${@:2}

echo
echo "****** Listing all images ******"
echo
set -x
python -m src.cloud_tasks.cli list_images --config tests/manual/run_cli_config.yml --provider ${P} ${ARGS}
set +x

echo
echo "****** Listing 5 Ubutu images in reverse order ******"
echo
set -x
python -m src.cloud_tasks.cli list_images --config tests/manual/run_cli_config.yml --provider ${P} --filter ubuntu --sort-by=-name --limit 5 ${ARGS}
set +x

echo
echo "****** Listing 5 Ubutu images in reverse order with detail ******"
echo
set -x
python -m src.cloud_tasks.cli list_images --config tests/manual/run_cli_config.yml --provider ${P} --filter ubuntu --sort-by=-name --limit 5 --detail ${ARGS}
set +x

echo
echo "****** Listing user instances ******"
echo
set -x
python -m src.cloud_tasks.cli list_images --config tests/manual/run_cli_config.yml --provider ${P} --filter node --user --sort-by=-name ${ARGS}
set +x
