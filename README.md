# Spotify ETL Pipeline

A Python-based ETL (Extract, Transform, Load) pipeline that extracts data from Spotify's Web API and loads it into Google BigQuery for analysis.

## Overview

This project automatically collects various personal Spotify data including:
- Recently played tracks
- Saved tracks, albums and shows 
- User playlists and their tracks
- Top tracks (short, medium and long term)
- Podcast episodes
- Genre recommendations

The data is validated and loaded into BigQuery tables for further analysis.

## Prerequisites

- Python 3.13.1
- Spotify Developer Account and API credentials
- Google Cloud Project with BigQuery enabled
- conda/miniconda

## Installation

1. Clone the repository
```sh
git clone https://github.com/uckocaman/spotify-etl.git
cd spotify-etl
```

2. Create conda environment
```sh
conda env create -f environment.yml
conda activate spotify-etl
```

3. Configure environment variables in .env:

```sh
SPOTIFY_CLIENT_ID=your_spotify_client_id
SPOTIFY_CLIENT_SECRET=your_spotify_client_secret
GCP_PROJECT_ID=your_gcp_project_id
dataset_id=your_bigquery_dataset_id
```

## Usage
The project contains multiple scripts that can be run individually:
```sh
python scripts/my_played_tracks.py      # Extract recently played tracks
python scripts/current_user_saved_tracks.py   # Extract saved tracks
python scripts/my_playlists.py          # Extract playlists
```

Each script:

1. Connects to Spotify API
2. Extracts relevant data
3. Validates the data
4. Loads it into BigQuery

### Project Structure
```
├── scripts/
│   ├── connect_to_spotify.py       # Spotify API connection
│   ├── load2bq.py                 # BigQuery loading utility
│   ├── validations.py             # Data validation functions
│   └── [data extraction scripts]  # Individual data extraction scripts
├── environment.yml                # Conda environment specification
└── README.md
```

### Data Flow


1. Extract: Data is extracted from Spotify Web API using spotipy library
2. Transform: Data is validated using functions in validations.py
3. Load: Validated data is loaded to BigQuery using google-cloud-bigquery

## Contributing
Contributions are welcome! Please feel free to submit a Pull Request.

## License
This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

* [Spotify Web API](https://developer.spotify.com/documentation/web-api)
* [spotipy](https://spotipy.readthedocs.io/en/2.25.1/)
* [Google Cloud BigQuery](https://cloud.google.com/bigquery)