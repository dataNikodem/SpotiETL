USE SpotifyDataBase;
GO

CREATE OR ALTER PROC usp_bronze_to_silver_genres_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE silver.genres AS target
        USING (
            SELECT DISTINCT
                Name
            FROM bronze.genres
            WHERE Name IS NOT NULL
        ) AS source
        ON target.Name = source.Name

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (Name)
            VALUES (source.Name);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO

