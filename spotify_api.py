from base64 import b64encode
import requests
import json
import certifi
import time


class SpotifyAPI:

    def __init__(self, clientID, clientSecret):
        self.clientID = clientID
        self.clientSecret = clientSecret
        self.token = None
        self.token_expires = 0

    def get_token(self):

        url = "https://accounts.spotify.com/api/token"

        auth_string = self.clientID + ":" + self.clientSecret
        auth_bytes = auth_string.encode("utf-8")
        auth_base64 = str(b64encode(auth_bytes), "utf8")

        headers = {
            "Authorization": "Basic " + auth_base64,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        data = {"grant_type": "client_credentials"}

        result = requests.post(
            url=url, headers=headers, data=data, verify=certifi.where()
        )

        if result.status_code != 200:
            raise Exception(
                f"Could not authenticate client. Status code: {result.status_code}, response: {result.text}"
            )
        json_result = json.loads(
            result.content
        )
        self.token = json_result["access_token"]

        self.token_expires = time.time() + json_result["expires_in"] - 60
        return self.token

    def __is_token_expired(self):
        return self.token is None or time.time() > self.token_expires

    def __get_valid_auth_header(self):
        if self.__is_token_expired():
            self.get_token()
        return {"Authorization": "Bearer " + self.token}

    def search_for_type(
        self,
        type_value: str,
        type: str = "artist",
        market: str = "US",
        limit: int = 5,
        offset: int = 0,
    ):

        url = "https://api.spotify.com/v1/search"  # allowed types: "album", "artist", "playlist", "track", "show", "episode", "audiobook"
        headers = self.__get_valid_auth_header()
      
        params = {
            "q": type_value,
            "type": type,
            "market": market,
            "limit": limit,
            "offset": offset,
        }

        result = requests.get(url=url, headers=headers, params=params)
        if result.status_code != 200:
            raise Exception(
                f"Search query failed. Status code: {result.status_code}, response: {result.text}"
            )
        json_result = json.loads(result.content)[f"{type}s"]["items"]

        return (
            json_result
            if len(json_result) > 0
            else f"Sorry, no {type} matches your search."
        )

    def get_artist_albums(
        self,
        artist_id: str,
        include_groups: str = "album",
        market: str = "US",
        limit: int = 20,
        offset: int = 0,
    ):  # include_groups can be "album", "single", "appears_on", "compilation"

        url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
        headers = self.__get_valid_auth_header()

        params = {
            "include_groups": include_groups,
            "market": market,
            "limit": limit,
            "offset": offset,
        }

        result = requests.get(url=url, headers=headers, params=params)
        if result.status_code != 200:
            raise Exception(
                f"Get artist albums query failed. Status code: {result.status_code}, response: {result.text}"
            )
        json_result = json.loads(result.content)["items"]

        return (
            json_result
            if len(json_result) > 0
            else "Sorry, no albums found for this artist."
        )

    def get_album_tracks(
        self, album_id, market: str = "US", limit: int = 30, offset: int = 0
    ):
        url = f"https://api.spotify.com/v1/albums/{album_id}/tracks"
        headers = self.__get_valid_auth_header()

        params = {"market": market, "limit": limit, "offset": offset}

        result = requests.get(url=url, headers=headers, params=params)
        if result.status_code != 200:
            raise Exception(
                f"Get albums track query faild. Status code: {result.status_code}, response: {result.text}"
            )
        json_result = json.loads(result.content)["items"]

        return (
            json_result
            if len(json_result) > 0
            else "Sorry, no tracks found for this album."
        )

    def get_artists(self, ids):
        url = "https://api.spotify.com/v1/artists"
        headers = self.__get_valid_auth_header()
        params = {"ids": ids}

        result = requests.get(url=url, headers=headers, params=params)
        if result.status_code != 200:
            raise Exception(
                f"Get artists query faild. Status code: {result.status_code}, response: {result.text}"
            )
        json_result = json.loads(result.content)["artists"]

        return (
            json_result
            if len(json_result) > 0
            else "Sorry, no artists found for this id."
        )
