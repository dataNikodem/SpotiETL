USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_bronze_to_silver_artistGenre_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE silver.artistGenre AS target
        USING (
            SELECT ArtistID, GenreID
            FROM (
                SELECT 
                    a.ArtistID,
                    g.GenreID,
                    ROW_NUMBER() OVER (PARTITION BY a.ArtistID, g.GenreID ORDER BY b.InsertedAt DESC) AS rn
                FROM bronze.genres b
                INNER JOIN silver.artists a
                    ON a.ArtistSpotifyID = b.ArtistSpotifyID
                INNER JOIN silver.genres g
                    ON g.Name = b.Name
                WHERE b.ArtistSpotifyID IS NOT NULL
                  AND b.Name IS NOT NULL
            ) AS dedup
            WHERE rn = 1
        ) AS source
        ON target.ArtistID = source.ArtistID
           AND target.GenreID = source.GenreID

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ArtistID, GenreID)
            VALUES (source.ArtistID, source.GenreID);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH
END;
GO