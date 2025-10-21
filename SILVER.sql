USE SpotifyDataBase
GO

CREATE SCHEMA silver
GO

CREATE TABLE silver.artists (
    ArtistID INT IDENTITY PRIMARY KEY,
    ArtistSpotifyID NVARCHAR(50) UNIQUE,
    ArtistName NVARCHAR(200) NOT NULL,
    Followers BIGINT NOT NULL,
    Popularity SMALLINT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    CreatedBy NVARCHAR(100) DEFAULT SUSER_SNAME()
);

CREATE TABLE silver.albums (
    AlbumID INT IDENTITY PRIMARY KEY,
    ArtistID INT NOT NULL 
        CONSTRAINT FK_Albums_Artists REFERENCES silver.artists(ArtistID),
    AlbumSpotifyID NVARCHAR(50) UNIQUE,
    AlbumName NVARCHAR(200) NOT NULL,
    ReleaseDate DATE NOT NULL,
    ReleaseDatePrecision NVARCHAR(10) NOT NULL,
    TotalTracks SMALLINT NOT NULL,
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    CreatedBy NVARCHAR(100) DEFAULT SUSER_SNAME()
);

CREATE TABLE silver.albumTracks (
    TrackID INT IDENTITY PRIMARY KEY,
    TrackSpotifyID NVARCHAR(50) UNIQUE,
    Title NVARCHAR(200) NOT NULL,
    DurationMS INT,
    TrackNumber SMALLINT,
    ArtistID INT NOT NULL 
        CONSTRAINT FK_AlbumTracks_Artists REFERENCES silver.artists(ArtistID),
    AlbumID INT NOT NULL 
        CONSTRAINT FK_AlbumTracks_Albums REFERENCES silver.albums(AlbumID),
    CreatedAt DATETIME2 DEFAULT SYSDATETIME(),
    CreatedBy NVARCHAR(100) DEFAULT SUSER_SNAME()
);

CREATE TABLE silver.genres (
    GenreID INT IDENTITY PRIMARY KEY,
    Name NVARCHAR(100) UNIQUE
);

CREATE TABLE silver.artistGenre (
    ArtistID INT NOT NULL 
        CONSTRAINT FK_albumGenre_ArtistID REFERENCES silver.artists(ArtistID),
    GenreID INT NOT NULL 
        CONSTRAINT FK_albumGenre_GenreID REFERENCES silver.genre(GenreID),
    CONSTRAINT PK_albumGenre PRIMARY KEY (ArtistID, GenreID)
);
go
