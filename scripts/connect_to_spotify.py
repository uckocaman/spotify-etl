import spotipy
import os
import logging

def connect2spotify(scope: str) -> spotipy.Spotify:
    """
    Connects to the Spotify API using the provided scope.

    Args:
        scope (str): The scope of the Spotify API access.

    Returns:
        spotipy.Spotify: An authenticated Spotify client.
    """
    try:
        sp = spotipy.Spotify(
            auth_manager=spotipy.oauth2.SpotifyOAuth(
                client_id=os.environ.get("SPOTIFY_CLIENT_ID"),
                client_secret=os.environ.get("SPOTIFY_CLIENT_SECRET"),
                redirect_uri="http://localhost:7777/callback",
                scope=scope,
            )
        )
        logging.info("Successfully connected to Spotify API")
        return sp
    except Exception as e:
        logging.error("Failed to connect to Spotify API: %s", e)
        raise
