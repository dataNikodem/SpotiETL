USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_fact_albums_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

            MERGE gold.fact_albums AS target
            USING (
                SELECT DISTINCT
                    salb.AlbumID,
                    salb.ArtistID,
                    CAST(CONVERT(CHAR(8), salb.ReleaseDate, 112) AS INT) AS ReleaseDateID,
                    MAX(salb.TotalTracks) AS TotalTracks,
                    SUM(saltr.DurationMS) AS AlbumDurationMS,
                    AVG(saltr.DurationMS) AS AvgTrackDurationMS,
                    MAX(saltr.DurationMS) AS MaxTrackDurationMS,
                    MIN(saltr.DurationMS) AS MinTrackDurationMS
                FROM silver.albums AS salb
                INNER JOIN silver.albumTracks AS saltr
                    ON salb.AlbumID = saltr.AlbumID
                GROUP BY 
                    salb.AlbumID,
                    salb.ArtistID,
                    salb.ReleaseDate
            ) AS source
                ON target.AlbumID = source.AlbumID

            WHEN MATCHED AND (
                target.ReleaseDateID <> source.ReleaseDateID
            )
            THEN UPDATE SET
                target.ReleaseDateID = source.ReleaseDateID

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    AlbumID,
                    ArtistID,
                    ReleaseDateID,
                    TotalTracks,
                    AlbumDurationMS,
                    AvgTrackDurationMS,
                    MaxTrackDurationMS,
                    MinTrackDurationMS
                )
                VALUES (
                    source.AlbumID,
                    source.ArtistID,
                    source.ReleaseDateID,
                    source.TotalTracks,
                    source.AlbumDurationMS,
                    source.AvgTrackDurationMS,
                    source.MaxTrackDurationMS,
                    source.MinTrackDurationMS
                );

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO
