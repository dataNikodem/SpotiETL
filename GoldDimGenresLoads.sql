USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_dim_genres_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE gold.dim_genres AS target
        USING (
            SELECT DISTINCT
                GenreID,
                Name
            FROM silver.genres
        ) AS source
        ON target.GenreID = source.GenreID


        WHEN NOT MATCHED BY TARGET THEN
            INSERT (GenreID, GenreName)
            VALUES (source.GenreID, source.Name);

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO
