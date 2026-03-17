import os
from huggingface_hub import HfApi, create_repo, snapshot_download
from dotenv import load_dotenv
import logging
import sys

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger(__name__)

# load the vault
load_dotenv()


class CloudStorage:
    def __init__(self, repo_id: str):
        self.api = HfApi()
        self.repo_id = repo_id
        self.token = os.getenv("HF_TOKEN")

        if not self.token:
            logger.error("HF TOKEN NOT FOUND. CREATE AND PLACE.")
            sys.exit(1)

        # create the repo on huggingface if not exists
        try:
            create_repo(
                repo_id=self.repo_id,
                repo_type="dataset",
                token=self.token,
                exist_ok=True,
            )
            logger.info(f"Connected to Hugging Face Repo {self.repo_id}")

        except Exception as e:
            logger.error(f"Failed to connect to Hugging face repo: {e}")
            sys.exit(1)

    def push_to_cloud(self, local_folder: str, cloud_folder: str):
        logger.info(f"Uploading Local Folder {local_folder}...")

        try:
            self.api.upload_folder(
                folder_path=local_folder,
                path_in_repo=cloud_folder,
                repo_id=self.repo_id,
                repo_type="dataset",
                token=self.token,
            )
            logger.info(f"Successfully uploaded local folder {local_folder}.")

        except Exception as e:
            logger.error(f"Upload Failed: {e}")

    def pull_from_cloud(self):
        logger.info(f"Downloading snapshot from {self.repo_id}...")

        try:
            snapshot_download(
                repo_id=self.repo_id,
                repo_type="dataset",
                local_dir=".",
                token=self.token,
            )
            logger.info(f"successfully Downloaded the snapshot from {self.repo_id}.")

        except Exception as e:
            logger.error(f"Download Failed or Repo is empty: {e}")


if __name__ == "__main__":
    HF_USERNAME = "saurabhSJ"
    REPO_NAME = f"{HF_USERNAME}/MarketMind-Data"

    logger.info("========================================")
    logger.info("   ☁️ STARTING CLOUD SYNC ☁️   ")
    logger.info("========================================")

    storage = CloudStorage(repo_id=REPO_NAME)

    # upload the Data folder
    storage.push_to_cloud(local_folder="data", cloud_folder="data")
    storage.push_to_cloud(local_folder="models", cloud_folder="models")

    logger.info("========================================")
    logger.info("🎉 CLOUD SYNC COMPLETE!")
    logger.info("========================================")
