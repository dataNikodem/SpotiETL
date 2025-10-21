USE SpotifyDataBase
GO

CREATE SCHEMA gold

CREATE TABLE gold.fact_artists
( 
	ArtistID INT NOT NULL,
	LatestReleaseDateID INT,
	GenreID INT,
	Followers BIGINT NOT NULL,
	Popularity SMALLINT NOT NULL,
	AvailableAlbumsCount INT,
	ArtistTotalTracks INT,
	TotalAlbumsDurationMS BIGINT,
	AvgTrackDurationMS INT,
	AvgAlbumTracks DECIMAL(6,2)
);

CREATE TABLE gold.fact_albums
( 
	AlbumID INT NOT NULL,
	ArtistID INT NOT NULL,
	ReleaseDateID INT NOT NULL,
	TotalTracks SMALLINT NOT NULL, 
	AlbumDurationMS BIGINT,
	AvgTrackDurationMS DECIMAL(12,2),
	MaxTrackDurationMS INT,
	MinTrackDurationMS INT
);

CREATE TABLE gold.fact_tracks
(
	TrackID INT NOT NULL, 
	ReleaseDateID INT NOT NULL,
	DurationMS INT NOT NULL, 
	TrackNumber INT NOT NULL
);

CREATE TABLE gold.dim_artists
(
	ArtistID INT PRIMARY KEY,
	ArtistName NVARCHAR(50) NOT NULL
);

CREATE TABLE gold.dim_albums
(
	AlbumID INT PRIMARY KEY,
	AlbumName NVARCHAR(80)
);

CREATE TABLE gold.dim_tracks
(
	TrackID INT PRIMARY KEY,
	TrackName NVARCHAR(200)
);

CREATE TABLE gold.dim_time
(
	DateID INT PRIMARY KEY,
	FullDate DATE NOT NULL,
	Year SMALLINT NOT NULL,
	MONTH SMALLINT NOT NULL,
	MonthName VARCHAR(50) NOT NULL,
	Day VARCHAR(50) NOT NULL,
	DayOfWeek TINYINT NOT NULL,
	DayName VARCHAR(50) NOT NULL
);

CREATE TABLE gold.dim_genres
(
	GenreID INT PRIMARY KEY,
	GenreName VARCHAR(50)
);

ALTER TABLE gold.fact_artists
ADD CONSTRAINT FK_fact_artists_ArtistID_to_dim_artists_ArtistID
    FOREIGN KEY (ArtistID) REFERENCES gold.dim_artists(ArtistID);
ALTER TABLE gold.fact_artists
ADD CONSTRAINT FK_fact_artists_LatestReleaseDateID_to_dim_time_DateID
    FOREIGN KEY (LatestReleaseDateID) REFERENCES gold.dim_time(DateID);
ALTER TABLE gold.fact_artists
ADD CONSTRAINT FK_fact_artists_GenreID_to_dim_genres_GenreID
	FOREIGN KEY (GenreID) REFERENCES gold.dim_genres(GenreID);

ALTER TABLE gold.fact_albums
ADD CONSTRAINT FK_fact_albums_AlbumID_to_dim_albums_AlbumID
	FOREIGN KEY (AlbumID) REFERENCES gold.dim_albums(AlbumID);
ALTER TABLE gold.fact_albums
ADD CONSTRAINT FK_fact_album_ArtistID_to_dim_artists_ArtistID
	FOREIGN KEY (ArtistID) REFERENCES gold.dim_artists(ArtistID);
ALTER TABLE gold.fact_albums
ADD CONSTRAINT FK_fact_albums_ReleaseDateID_to_dim_time_DateID
	FOREIGN KEY (ReleaseDateID) REFERENCES gold.dim_time(DateID);

ALTER TABLE gold.fact_tracks
ADD CONSTRAINT FK_fact_tracks_TrackID_to_dim_tracks_TrackID
	FOREIGN KEY (TrackID) REFERENCES gold.dim_tracks(TrackID);
ALTER TABLE gold.fact_tracks
ADD CONSTRAINT FK_fact_tracks_ReleaseDateID_to_dim_time_DateID
	FOREIGN KEY (ReleaseDateID) REFERENCES gold.dim_time(DateID);