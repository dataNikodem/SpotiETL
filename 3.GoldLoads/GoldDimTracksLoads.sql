USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_dim_tracks_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE gold.dim_tracks AS target
        USING (
            SELECT DISTINCT
                TrackID,
                Title 
            FROM silver.albumTracks
        ) AS source
        ON target.TrackID = source.TrackID

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (TrackID, TrackName)
            VALUES (source.TrackID, source.Title);

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO