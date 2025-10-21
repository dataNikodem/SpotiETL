USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_dim_albums_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE gold.dim_albums AS target
        USING (
            SELECT DISTINCT
                AlbumID,
                AlbumName
            FROM silver.albums
        ) AS source
        ON target.AlbumID = source.AlbumID

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (AlbumID, AlbumName)
            VALUES (source.AlbumID, source.AlbumName);

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO
