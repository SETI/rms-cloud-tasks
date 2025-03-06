# Number Adder Worker

This worker processes tasks that require adding two numbers together. Each task is processed independently, making it well-suited for distributed cloud computing.

## How It Works

1. The worker receives a task from a queue
2. It extracts the two numbers (`num1` and `num2`) from the task data
3. It adds the numbers together
4. It writes the result to a file named `{task_id}.out` in the `results` directory

## Task Format

Each task should have the following format:

```json
{
  "id": "unique-task-id",
  "data": {
    "num1": 42,
    "num2": 58
  }
}
```

## Requirements

- Python 3.8+
- Dependencies listed in `requirements.txt`

## Usage

This worker is designed to be used with the cloud_tasks system, which will:

1. Pull this repository on worker startup
2. Set up a configuration file
3. Run `worker.py --config=/path/to/config.json`
4. Handle task polling and completion

## Local Testing

To test this worker locally:

```bash
# Create a test config file
cat > test_config.json << EOF
{
  "sample_task": {
    "id": "test-task",
    "data": {
      "num1": 42,
      "num2": 58
    }
  }
}
EOF

# Run the worker
python worker.py --config=test_config.json
```

This should create a file `results/test-task.out` containing the result `100`.