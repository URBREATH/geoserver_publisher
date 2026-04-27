import logging
import sys

# Single, shared logger for the whole application.
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(name)s - %(message)s',
    stream=sys.stdout,
    force=True,
)

logger = logging.getLogger("publisher")
