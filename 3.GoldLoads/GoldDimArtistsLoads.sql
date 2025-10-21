USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_dim_artists_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE gold.dim_artists AS target
        USING (
            SELECT DISTINCT
                ArtistID,
                ArtistName
            FROM silver.artists
        ) AS source
        ON target.ArtistID = source.ArtistID

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (ArtistID, ArtistName)
            VALUES (source.ArtistID, source.ArtistName);

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO
