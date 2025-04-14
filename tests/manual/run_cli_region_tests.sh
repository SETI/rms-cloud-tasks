# Run as tests/manual/run_cli_region_tests.sh [provider] [args]
set -e
P=$1
ARGS="${@:2}"

echo
echo "****** Listing US regions ******"
echo
set -x
python -m src.cloud_tasks.cli list_regions --config tests/manual/run_cli_config.yml --provider ${P} --prefix us ${ARGS}
set +x

echo
echo "****** Listing US regions with detail ******"
echo
set -x
python -m src.cloud_tasks.cli list_regions --config tests/manual/run_cli_config.yml --provider ${P}  --prefix us --detail ${ARGS}
set +x

echo
echo "****** Listing US regions with zones ******"
echo
set -x
python -m src.cloud_tasks.cli list_regions --config tests/manual/run_cli_config.yml --provider ${P} --prefix us --zones ${ARGS}
set +x

echo
echo "****** Listing US regions with zones and detail ******"
echo
set -x
python -m src.cloud_tasks.cli list_regions --config tests/manual/run_cli_config.yml --provider ${P} --prefix us --zones --detail ${ARGS}
set +x
