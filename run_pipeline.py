import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)


def run_step(module_name: str):
    logger.info(f"Starting Step: {module_name}...")

    # This acts like typing command in terminal
    result = subprocess.run([sys.executable, "-m", module_name])

    if result.returncode != 0:
        logger.error(f"FAILED.Pipeline Halted at {module_name}.")
        sys.exit(1)  # Stops the pipeline if something fails

    logger.info(f"FINISHED {module_name}\n")


if __name__ == "__main__":
    logger.info("========================================")
    logger.info("  🧠 MARKETMIND AI - PIPELINE BUILDER 🧠  ")
    logger.info("========================================\n")

    # exact sequence of our pipeline
    pipeline_steps = [
        "src.scraper",
        "src.migration",
        "src.analyst",
        "src.historicalData",
        "src.data_merger",
        "src.predictor",
    ]

    for step in pipeline_steps:
        run_step(step)

    logger.info("========================================")
    logger.info("🎉 PIPELINE COMPLETE! All data and models generated.")
    logger.info("👉 Next Step: Run 'streamlit run src/app.py' to open the UI.")
    logger.info("========================================")
