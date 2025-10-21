USE SpotifyDataBase
GO

CREATE OR ALTER PROCEDURE usp_bronze_to_silver_albumTracks_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE silver.albumTracks AS target
        USING (
            SELECT DISTINCT
                bAT.TrackSpotifyID,
                bAT.Title,
                bAT.DurationMS,
                bAT.TrackNumber,
                sA.AlbumID,
                sAR.ArtistID
            FROM bronze.albumTracks AS bAT
            INNER JOIN silver.albums AS sA 
                ON sA.AlbumSpotifyID = bAT.AlbumSpotifyID
            INNER JOIN silver.artists AS sAR 
                ON sAR.ArtistID = sA.ArtistID
        ) AS source
        ON target.TrackSpotifyID = source.TrackSpotifyID

        WHEN NOT MATCHED BY TARGET
        THEN INSERT (
            TrackSpotifyID,
            Title,
            DurationMS,
            TrackNumber,
            AlbumID,
            ArtistID
        )
        VALUES (
            source.TrackSpotifyID,
            source.Title,
            source.DurationMS,
            source.TrackNumber,
            source.AlbumID,
            source.ArtistID
        );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
            
        THROW;
    END CATCH
END;
GO
