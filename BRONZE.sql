USE SpotifyDataBase
GO
    
CREATE TABLE bronze.artists (
    InsertedAt DATETIME2 DEFAULT SYSDATETIME(),
    ArtistSpotifyID NVARCHAR(50),
    ArtistName NVARCHAR(200),
    Followers BIGINT,
    Popularity SMALLINT,
);

CREATE TABLE bronze.albums (
    InsertedAt DATETIME2 DEFAULT SYSDATETIME(),
    AlbumSpotifyID NVARCHAR(50),
    ArtistSpotifyID NVARCHAR(50),
    AlbumName NVARCHAR(200),
    ReleaseDate NVARCHAR(20),
    ReleaseDatePrecision NVARCHAR(10),
    TotalTracks SMALLINT,
);

CREATE TABLE bronze.albumTracks (
    InsertedAt DATETIME2 DEFAULT SYSDATETIME(),
    TrackSpotifyID NVARCHAR(50),
    Title NVARCHAR(200),
    DurationMS INT,
    TrackNumber SMALLINT,
    AlbumSpotifyID NVARCHAR(50),
    ArtistSpotifyID NVARCHAR(50)
);

CREATE TABLE bronze.genres (
    InsertedAt DATETIME2 DEFAULT SYSDATETIME(),
    Name NVARCHAR(100),
    ArtistSpotifyID NVARCHAR(50)
);
