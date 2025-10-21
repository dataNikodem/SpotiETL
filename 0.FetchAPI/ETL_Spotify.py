from spotify_api import SpotifyAPI
from concurrent.futures import ThreadPoolExecutor
from colorama import Fore, Style
import functools
from datetime import datetime
import pandas as pd
import pyodbc

serverName = "add your server name"

env_vars = {}
with open(".env", "r", encoding="utf-8") as f:
    for line in f:
        line = line.strip()
        key, value = line.split("=", 1)
        env_vars[key] = value.strip('"')
clientID = env_vars["ClientID"]
clientSecret = env_vars["ClientSecret"]
dbUser = env_vars["DBUser"]
dbPassword = env_vars["DBPassword"]


input_artists = []
with open("DataInput.txt", "r", encoding="utf-8") as inputfile:
    for line in inputfile:
        line = line.strip()
        input_artists.append(line)
spotify = SpotifyAPI(clientID, clientSecret)
spotify.get_token()


def log_issues_to_file(func):
    def wrapped_func(*args, **kwargs):

        debug_message, result = func(*args, **kwargs)
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        logs = []
        if debug_message:
            logs.append(debug_message)
        if isinstance(result, str) and result.startswith("Sorry"):
            logs.append(result)
        elif isinstance(result, list) and len(result) == 0:
            logs.append("Empty list result")
        if logs:
            now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            with open("issues_report.txt", "a", encoding="utf-8") as f:
                for log in logs:
                    f.write(
                        f"[DEBUG][{now}] {func.__name__} >>>>> {log} | input: {args[0] if args else 'N/A'}\n"
                    )
        return result

    return wrapped_func


@log_issues_to_file
def fetch_artist(name):

    debug_message = None

    artist = spotify.search_for_type(type_value=name, type="artist", limit=1)[0]
    error_message = "Sorry, no artist matches your search."

    if isinstance(artist, str) and error_message in artist:
        debug_message = f"Artist data invalid: {artist}, name: {name}"
        print(
            Fore.RED
            + f"[DEBUG] Artist data invalid: {artist}, name: {name}"
            + Style.RESET_ALL
        )
    return debug_message, {
        "ArtistSpotifyID": artist["id"],
        "ArtistName": artist["name"],
        "Followers": artist["followers"]["total"],
        "Popularity": artist["popularity"],
    }


@log_issues_to_file
def fetch_genres(artist_name):

    debug_message = None

    artist_genre = spotify.search_for_type(
        type_value=artist_name, type="artist", limit=1
    )[0]
    error_message = "Sorry, no artist matches your search."

    if isinstance(artist_genre, str) and error_message in artist_genre:
        debug_message = f"Artist data invalid: {artist_genre}, name: {artist_name}"
        print(
            Fore.RED
            + f"[DEBUG] Artist data invalid: {artist_genre}, name: {artist_name}"
            + Style.RESET_ALL
        )
    if artist_genre["genres"] == []:
        debug_message = f"Artist has no genres listed: name: {artist_name}"
        print(
            Fore.YELLOW
            + f"[DEBUG] Artist has no genres listed: name: {artist_name}"
            + Style.RESET_ALL
        )
    return debug_message, {"Name": artist_genre["genres"],
                           "ArtistSpotifyID": artist_genre["id"]}


@log_issues_to_file
def fetch_albums(artist):

    debug_message = None

    artist_id = artist["ArtistSpotifyID"]
    albums_raw = spotify.get_artist_albums(artist_id=artist_id)
    error_message = "Sorry, no albums found for this artist."

    if isinstance(albums_raw, str) and error_message in albums_raw:
        debug_message = f"Album data invalid: {albums_raw}, artist: {artist}"
        print(
            Fore.RED
            + f"[DEBUG] Album data invalid: {albums_raw}, artist: {artist}"
            + Style.RESET_ALL
        )
        return []
    return debug_message, [
        {
            "AlbumSpotifyID": album["id"],
            "ArtistSpotifyID": artist_id,
            "AlbumName": album["name"],
            "ReleaseDate": album["release_date"],
            "ReleaseDatePrecision": album["release_date_precision"],
            "TotalTracks": album["total_tracks"],
        }
        for album in albums_raw
    ]


@log_issues_to_file
def fetch_albumTracks(album):

    debug_message = None

    album_id = album["AlbumSpotifyID"]
    artist_id = album["ArtistSpotifyID"]
    tracks_raw = spotify.get_album_tracks(album_id=album_id)
    error_message = "Sorry, no tracks found for this album."

    if isinstance(tracks_raw, str) and error_message in tracks_raw:
        debug_message = f"Track data invalid: {tracks_raw}, album: {album}"
        print(
            Fore.RED
            + f"[DEBUG] Track data invalid: {tracks_raw}, album: {album}"
            + Style.RESET_ALL
        )
        return []
    return debug_message, [
        {
            "TrackSpotifyID": t["id"],
            "Title": t["name"],
            "DurationMS": t["duration_ms"],
            "TrackNumber": t["track_number"],
            "AlbumSpotifyID": album_id,
            "ArtistSpotifyID": artist_id,
        }
        for t in tracks_raw
    ]


with ThreadPoolExecutor(max_workers=5) as executor:
    artist_output = list(executor.map(fetch_artist, input_artists))
    nested_genres = list(executor.map(fetch_genres, input_artists))
    nested_albums = list(executor.map(fetch_albums, artist_output))
genre_output = []
for sublist in nested_genres:
    name_sublst = sublist["Name"]
    artist_id = sublist["ArtistSpotifyID"]
    if name_sublst:
        for genre in name_sublst:
            genre_output.append({"Name": genre, "ArtistSpotifyID":artist_id})
albums_output = []
for sublist in nested_albums:
    for album in sublist:
        albums_output.append(album)
with ThreadPoolExecutor(max_workers=5) as executor:
    nested_tracks = list(executor.map(fetch_albumTracks, albums_output))
tracks_output = []
for sublist in nested_tracks:
    for track in sublist:
        tracks_output.append(track)
df_artists = pd.DataFrame(
    data=artist_output,
)
df_albums = pd.DataFrame(data=albums_output)
df_albumTracks = pd.DataFrame(data=tracks_output)
df_genres = pd.DataFrame(data=genre_output)

df_genres.drop_duplicates(subset=["Name"], inplace=True)
df_albumTracks.drop_duplicates(subset=["TrackSpotifyID"], inplace=True)
df_albums.drop_duplicates(subset=["AlbumSpotifyID"], inplace=True)
df_artists.drop_duplicates(subset=["ArtistSpotifyID"], inplace=True)

df_albums["AlbumName"] = df_albums["AlbumName"].str.strip('"')
df_albumTracks["Title"] = df_albumTracks["Title"].str.strip('"')


conn = pyodbc.connect(
    "DRIVER={SQL Server};"
    f"SERVER={serverName};"
    "DATABASE=SpotifyDataBase;"
    f"UID={dbUser};"
    f"PWD={dbPassword}"
)
cursor = conn.cursor()

try:

    for _, row in df_artists.iterrows():
        cursor.execute(
            """
            INSERT INTO bronze.artists (ArtistSpotifyID, ArtistName, Followers, Popularity)
            VALUES (?, ?, ?, ?)
        """,
            row["ArtistSpotifyID"],
            row["ArtistName"],
            row["Followers"],
            row["Popularity"],
        )
    for _, row in df_albums.iterrows():
        cursor.execute(
            """
            INSERT INTO bronze.albums (AlbumSpotifyID, ArtistSpotifyID, AlbumName, ReleaseDate, ReleaseDatePrecision, TotalTracks)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            row["AlbumSpotifyID"],
            row["ArtistSpotifyID"],
            row["AlbumName"],
            row["ReleaseDate"],
            row["ReleaseDatePrecision"],
            row["TotalTracks"],
        )
    for _, row in df_albumTracks.iterrows():
        cursor.execute(
            """
            INSERT INTO bronze.albumTracks (TrackSpotifyID, Title, DurationMS, TrackNumber, AlbumSpotifyID, ArtistSpotifyID)
            VALUES (?, ?, ?, ?, ?, ?)
        """,
            row["TrackSpotifyID"],
            row["Title"],
            row["DurationMS"],
            row["TrackNumber"],
            row["AlbumSpotifyID"],
            row["ArtistSpotifyID"],
        )
    for _, row in df_genres.iterrows():
        artistSpotify_id = None if pd.isna(row["ArtistSpotifyID"]) else str(row["ArtistSpotifyID"])
        cursor.execute(
            """
            INSERT INTO bronze.genres (Name, ArtistSpotifyID)
            VALUES (?, ?)
        """,
            
            row["Name"],
            artistSpotify_id, 
        )
    conn.commit()
    cursor.close()
    conn.close()
except Exception as e:
    print(Fore.RED + f"Database error: {e}" + Style.RESET_ALL)
    with open("issues_report.txt", "a", encoding="utf-8") as f:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"[DEBUG][{now}] Database error: {e}\n")
