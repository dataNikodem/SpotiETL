USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_bronze_to_silver_album_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE silver.albums AS target
        USING (
            SELECT AlbumSpotifyID,
                    AlbumName,
                    ReleaseDate,
                    ReleaseDatePrecision,
                    TotalTracks,
                    ArtistID
            FROM (
                SELECT --DISTINCT
                    b.AlbumSpotifyID,
                    b.AlbumName,
                    CAST(
                        CASE
                            WHEN LEN(b.ReleaseDate) = 4 THEN CONCAT(b.ReleaseDate, '-01-01')
                            WHEN LEN(b.ReleaseDate) = 7 THEN CONCAT(b.ReleaseDate, '-01')
                            ELSE b.ReleaseDate
                        END AS DATE
                    ) AS ReleaseDate,
                    b.ReleaseDatePrecision,
                    b.TotalTracks,
                    a.ArtistID,
                    ROW_NUMBER() OVER (PARTITION BY b.AlbumSpotifyID ORDER BY b.InsertedAt DESC) AS rn
                FROM bronze.albums b
                INNER JOIN silver.artists a
                    ON b.ArtistSpotifyID = a.ArtistSpotifyID
            ) AS dedup
            WHERE rn = 1
        ) AS source
        ON target.AlbumSpotifyID = source.AlbumSpotifyID

        WHEN MATCHED AND (
            target.ReleaseDate <> source.ReleaseDate
            OR target.ReleaseDatePrecision <> source.ReleaseDatePrecision
        )
        THEN UPDATE SET
            ReleaseDate = source.ReleaseDate,
            ReleaseDatePrecision = source.ReleaseDatePrecision,
            ArtistID = source.ArtistID

        WHEN NOT MATCHED BY TARGET
        THEN INSERT (AlbumSpotifyID, AlbumName, ReleaseDate, ReleaseDatePrecision, TotalTracks, ArtistID)
             VALUES (source.AlbumSpotifyID, source.AlbumName, source.ReleaseDate, source.ReleaseDatePrecision, source.TotalTracks, source.ArtistID);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
            
        THROW;
    END CATCH
END;
