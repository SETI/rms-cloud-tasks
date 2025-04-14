# Run as tests/manual/run_cli_inst_type_tests.sh [provider] [args]
set -e
P=$1
ARGS="${@:2}"

echo
echo "****** Listing all instance types ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} ${ARGS}
set +x

echo
echo "****** Listing all instance types with spot pricing ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --use-spot ${ARGS}
set +x

echo
echo "****** Listing instance types matching m3. and m3-.* ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types m3.,m3- --sort-by=-total_price,name ${ARGS}
set +x

echo
echo "****** Listing instance types matching m3. and m3-.* with spot pricing ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types m3.,m3- --sort-by=-total_price,name --use-spot ${ARGS}
set +x

echo
echo "****** Listing instance types with 8-16 CPUs and 32-64GB of memory ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --min-cpu 8 --max-cpu 16 --min-total-memory 32 --max-total-memory 64 --limit 4 --sort-by=cpu,mem,-total_price,name --detail ${ARGS}
set +x

echo
echo "****** Listing instance types with 8-16 CPUs and 32-64GB of memory (ARM64) ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --min-cpu 8 --max-cpu 16 --min-total-memory 32 --max-total-memory 64 --limit 12 --sort-by=cpu,mem,-total_price,name --filter arm64 --detail ${ARGS}
set +x

echo
echo "****** No matching instance types ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types badtype --sort-by=cpu,mem,-total_price,name ${ARGS}
set +x

echo
echo "****** No matching instance types with spot pricing ******"
echo
set -x
python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types badtype --sort-by=cpu,mem,-total_price,name --use-spot ${ARGS}
set +x

if [ "${P}" == "aws" ]; then
    echo
    echo "****** Comparing spot prices for m5.large in different regions ******"
    echo
    set -x
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types m5.large --use-spot ${ARGS}  # Default us-west-2
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types m5.large --use-spot --region us-west-1 ${ARGS}
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types m5.large --use-spot --region us-east-1 ${ARGS}
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types m5.large --use-spot --region us-east-2 ${ARGS}
    set +x
else
    echo
    echo "****** Comparing spot prices for n2-highmem-16 in different regions ******"
    echo
    set -x
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types n2-highmem-16 --use-spot --detail ${ARGS}  # Default us-central1
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types n2-highmem-16 --use-spot --region southamerica-east1 --detail ${ARGS}
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types n2-highmem-16 --use-spot --region europe-west8 --detail ${ARGS}
    python -m src.cloud_tasks.cli list_instance_types --config tests/manual/run_cli_config.yml --provider ${P} --instance-types n2-highmem-16 --use-spot --region northamerica-northeast1 --detail ${ARGS}
    set +x
fi
