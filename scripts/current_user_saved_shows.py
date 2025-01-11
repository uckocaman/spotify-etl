# Description: This script gets the albums and tracks data from the Spotify API and loads it into BigQuery.
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

logging.info("The job of getting the saved shows started.")

sp = connect2spotify("user-library-read")


def get_saved_shows(offset: int = 0):
    saved_shows = sp.current_user_saved_shows(limit=50, offset=offset)
    total_shows = saved_shows["total"]
    saved_shows_list = []

    for show in saved_shows["items"]:
        shows_row = {
            "show_id": show["show"]["id"],
            "show_name": show["show"]["name"],
            "show_description": show["show"]["description"],
            "explicit": show["show"]["explicit"],
            "show_url": show["show"]["external_urls"]["spotify"],
            "is_externally_hosted": show["show"]["is_externally_hosted"],
            "language": show["show"]["languages"][0],
            "type": show["show"]["type"],
            "show_publisher": show["show"]["publisher"],
            "shwo_total_episodes": show["show"]["total_episodes"],
            "show_media_type": show["show"]["media_type"],
            "aded_at": show["added_at"],
        }
        saved_shows_list.append(shows_row)

    return pd.DataFrame(saved_shows_list), total_shows


def main():
    try:
        saved_shows, total_shows = get_saved_shows()
        logging.info(
            "Data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception("Something went wrong while extracting data from API")
        raise

    while len(saved_shows) % 50 == 0 and len(saved_shows) != total_shows:
        logging.info(
            f"The number of saved show is greater than {len(saved_shows)}. The offset increased to {len(saved_shows)+1}"
        )
        try:
            saved_shows_new_offset, total_shows = get_saved_shows(
                offset=len(saved_shows) + 1
            )
            saved_shows = pd.concat([saved_shows, saved_shows_new_offset])
            logging.info(
                "All data successfully extracted from API, proceeding to validation stage"
            )
        except:
            logging.exception("Something went wrong while extracting data from API")
            raise

    if check_if_valid_data(saved_shows, "show_id"):
        logging.info("Data valid for shows table, proceed to Load stage")
        load2bq(saved_shows, "saved_shows")


main()
