import spotipy
import pandas as pd
import os
import logging
from load2bq import load2bq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s :: %(levelname)s :: %(message)s",
    filename=f"logs/{os.path.basename(__file__).split('.')[0]}.log",
)
logging.info("The job of getting the playlists started.")

sp = spotipy.Spotify(
    auth_manager=spotipy.oauth2.SpotifyOAuth(
        client_id=os.environ.get("SPOTIFY_CLIENT_ID"),
        client_secret=os.environ.get("SPOTIFY_CLIENT_SECRET"),
        redirect_uri="http://localhost:7777/callback",
        scope="user-library-read",
    )
)


def check_if_valid_data(df: pd.DataFrame, primary_key="None") -> bool:
    # Check if dataframe is empty
    if df.empty:
        logging.info("No songs downloaded. Finishing execution")
        return False

    # Primary Key Check
    if primary_key != "None" and not pd.Series(df[primary_key]).is_unique:
        logging.exception("Primary Key check is violated")
        raise

    # Check for nulls
    if df.isnull().values.any():
        logging.exception("Null values found")
        raise
    return True


def getPlaylists(sp: spotipy, user_id: str) -> pd.DataFrame:
    playlists = sp.user_playlists(user_id)
    playlist_list = []

    while playlists:
        for playlist in playlists["items"]:
            playlists_row = {
                "playlist_id": playlist["id"],
                "playlist_name": playlist["name"],
                "playlist_url": playlist["external_urls"]["spotify"],
                "playlist_owner_id": playlist["owner"]["display_name"],
                "playlist_owner": playlist["owner"]["external_urls"]["spotify"],
                "playlist_owner_url": playlist["owner"]["id"],
                "playlist_owner_type": playlist["owner"]["type"],
                "is_public": playlist["public"],
                "total_track": playlist["tracks"]["total"],
                "playlist_type": playlist["type"],
            }
            playlist_list.append(playlists_row)

        playlists = sp.next(playlists) if playlists["next"] else None
    playlists_df = pd.DataFrame(playlist_list)

    return playlists_df


def get_playlist_tracks(username: str, playlist_id_list: list) -> pd.DataFrame:
    playlist_tracks_list = []

    for i in playlist_id_list:
        results = sp.user_playlist_tracks(username, i)
        playlist_items = results["items"]

        for item in playlist_items:
            playlist_tracks_row = {
                "playlist_id": i,
                "track_id": item["track"]["id"],
                "track_name": item["track"]["name"],
                "artist_id": item["track"]["artists"][0]["id"],
                "artist_name": item["track"]["artists"][0]["name"],
                "artist_type": item["track"]["artists"][0]["type"],
                "album_id": item["track"]["album"]["id"],
                "album_name": item["track"]["album"]["name"],
                "album_type": item["track"]["album"]["album_type"],
                "album_release_date": item["track"]["album"]["release_date"],
                "album_total_tracks": item["track"]["album"]["total_tracks"],
                "track_type": item["track"]["type"],
                "duraiton": item["track"]["duration_ms"],
                "added_at": item["added_at"],
                "added_by": item["added_by"]["id"],
                "is_explicit": item["track"]["explicit"],
            }
            playlist_tracks_list.append(playlist_tracks_row)

        while results["next"]:
            results = sp.next(results)
            playlist_items.append(results["items"])

    return pd.DataFrame(playlist_tracks_list)


if __name__ == "__main__":
    try:
        playlists_df = getPlaylists(sp, sp.me()["id"])
        logging.info(
            "Playlists data successfully extracted from API, proceeding to validation stage"
        )
    except:
        logging.exception(
            "Something went wrong while extracting playlists data from API"
        )
        raise

    if check_if_valid_data(playlists_df, "playlist_id"):
        logging.info("Data valid, proceeding to load stage")
        load2bq(playlists_df, table_id="my_playlists", load_type="WRITE_TRUNCATE")

        playlist_tracks_df = get_playlist_tracks(
            sp.me()["id"], playlists_df["playlist_id"].to_list()
        )
        playlist_tracks_df["album_release_date"] = playlist_tracks_df[
            "album_release_date"
        ].fillna("1900-01-01")

        if check_if_valid_data(playlist_tracks_df):
            logging.info("Data valid, proceeding to load stage")
            load2bq(
                playlist_tracks_df,
                table_id="my_playlists_tracks",
                load_type="WRITE_TRUNCATE",
            )
