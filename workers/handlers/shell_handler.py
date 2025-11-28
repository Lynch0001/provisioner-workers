import logging

from workers.config.logging_config import setup_logging

setup_logging()
logger = logging.getLogger(__name__)


def run_command(cmd, cwd=None):
    """Run shell command, raise on error."""
    import subprocess
    result = subprocess.run(cmd, cwd=cwd, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        raise RuntimeError(f"Command failed: {cmd}\nSTDOUT: {result.stdout}\nSTDERR: {result.stderr}")
    return result.stdout.strip()