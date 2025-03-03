# Description: This script extracts the saved episodes of the current user and loads them into BigQuery.
import pandas as pd
import os
import logging
from load2bq import load2bq
from dotenv import load_dotenv
from validations import check_if_valid_data
from connect_to_spotify import connect2spotify

load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    filename=f"logs/{os.path.basename(__file__).split('.')[0]}.log",
)

logging.info("The job of getting the played tracks started.")

sp = connect2spotify("user-library-read")


def get_saved_episodes(offset: int = 0) -> pd.DataFrame:
    saved_episodes = sp.current_user_saved_episodes(limit=50, offset=offset)
    total_episodes = saved_episodes["total"]
    episodes_list = []

    for episode in saved_episodes["items"]:
        episode_row = {
            "episode_id": episode["episode"]["id"],
            "episode_name": episode["episode"]["name"],
            "episode_description": episode["episode"]["description"],
            "episode_duration": episode["episode"]["duration_ms"],
            "explicit": episode["episode"]["explicit"],
            "episode_url": episode["episode"]["external_urls"]["spotify"],
            "is_externally_hosted": episode["episode"]["is_externally_hosted"],
            "is_playable": episode["episode"]["is_playable"],
            "language": episode["episode"]["language"],
            "release_date": episode["episode"]["release_date"],
            "type": episode["episode"]["type"],
            "show_description": episode["episode"]["show"]["description"],
            "show_explicit": episode["episode"]["show"]["explicit"],
            "show_url": episode["episode"]["show"]["external_urls"]["spotify"],
            "show_id": episode["episode"]["show"]["id"],
            "show_name": episode["episode"]["show"]["name"],
            "show_publisher": episode["episode"]["show"]["publisher"],
            "shwo_total_episodes": episode["episode"]["show"]["total_episodes"],
            "show_is_externally_hosted": episode["episode"]["show"][
                "is_externally_hosted"
            ],
            "show_media_type": episode["episode"]["show"]["media_type"],
            "aded_at": episode["added_at"],
        }
        episodes_list.append(episode_row)

    return pd.DataFrame(episodes_list), total_episodes


def main():
    try:
        saved_episodes, total_episodes = get_saved_episodes()
        logging.info("First 50 episodes successfully extracted from API.")
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    while len(saved_episodes) % 50 == 0 and len(saved_episodes) != total_episodes:
        logging.info(
            f"The number of saved episode is greater than {len(saved_episodes)}. The offset increased to {len(saved_episodes)+1}"
        )
        try:
            saved_episodes_new_offset, total_episodes = get_saved_episodes(
                offset=len(saved_episodes) + 1
            )
            saved_episodes = pd.concat([saved_episodes, saved_episodes_new_offset])
            logging.info(
                "All data successfully extracted from API, proceeding to validation stage"
            )
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise

    if check_if_valid_data(saved_episodes, "episode_id"):
        logging.info("Data valid for albums table, proceed to Load stage")
        load2bq(saved_episodes, "saved_episodes")


main()
