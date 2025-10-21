USE SpotifyDataBase;
GO

CUSE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_fact_tracks_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE gold.fact_tracks AS target
        USING (
            SELECT DISTINCT
                sat.TrackID,
                CAST(CONVERT(CHAR(8), salb.ReleaseDate, 112) AS INT) AS ReleaseDateID,
                sat.DurationMS,
                sat.TrackNumber
            FROM silver.albumTracks AS sat
			INNER JOIN silver.albums salb
				ON sat.AlbumID = salb.AlbumID
        ) AS source
        ON target.TrackID = source.TrackID

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (TrackID, ReleaseDateID, DurationMS, TrackNumber)
            VALUES (source.TrackID, source.ReleaseDateID, source.DurationMS, source.TrackNumber);

        COMMIT TRANSACTION;
    END TRY

    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;

        THROW;
    END CATCH;
END;
GO