# Extraction Batch Processor

This batch processor handles the asynchronous processing of data extractions, separate from the API server.

## Architecture

The system now follows a clear separation of concerns:

1. **API Server (Flask)**: Handles HTTP requests, updates database states, and schedules extractions.
2. **Batch Processor**: Independently polls the database for scheduled/paused/in-progress extractions and processes them.

## Running the Batch Processor

You can run the batch processor using the provided script:

```bash
python run_extraction_processor.py
```

Optional arguments:

- `--interval`: Polling interval in seconds (default: 60)
- `--log-level`: Logging level (default: INFO)

Example:

```bash
python run_extraction_processor.py --interval 30 --log-level DEBUG
```

## Extraction Status Flow

Extractions now follow this status flow:

1. `scheduled`: Extraction has been requested and is waiting to be picked up by the batch processor
2. `in_progress`: Extraction is currently being processed
3. `paused`: Extraction has been paused by the user
4. `completed`: Extraction has completed successfully
5. `failed`: Extraction has failed

## Running as a Service

For production use, you should run the batch processor as a system service.

Example systemd service file:

```ini
[Unit]
Description=Extraction Batch Processor
After=network.target

[Service]
User=your_user
WorkingDirectory=/path/to/your/app
ExecStart=/path/to/your/virtualenv/bin/python run_extraction_processor.py
Restart=always
RestartSec=10

[Install]
WantedBy=multi-user.target
```

## Troubleshooting

If extractions get stuck in a particular state, you can manually update their status in the database:

```sql
UPDATE extraction_progress SET status = 'scheduled' WHERE id = <extraction_id>;
```

The batch processor will pick up these extractions on the next polling cycle. 