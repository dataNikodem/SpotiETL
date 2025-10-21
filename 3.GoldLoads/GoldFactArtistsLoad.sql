USE SpotifyDataBase;
GO

CREATE OR ALTER PROCEDURE usp_silver_to_gold_fact_artists_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;
            
            MERGE gold.fact_artists AS target
            USING (
                SELECT DISTINCT
                    sart.ArtistID,
                    CAST(CONVERT(CHAR(8), MAX(salb.ReleaseDate), 112) AS INT) AS LatestReleaseDateID,
                    MAX(sag.GenreID) AS GenreID,
                    MAX(sart.Followers) AS Followers,
                    MAX(sart.Popularity) AS Popularity,
                    COUNT(DISTINCT salb.AlbumID) AS AvailableAlbumsCount,
                    COUNT(salb.TotalTracks) AS ArtistTotalTracks,
                    SUM(saltr.DurationMS) AS TotalAlbumsDurationMS,
                    AVG(saltr.DurationMS) AS AvgTrackDurationMS,
                    AVG(salb.TotalTracks) AS AvgAlbumTracks
                FROM silver.artists AS sart
                LEFT JOIN silver.albums AS salb
                    ON sart.ArtistID = salb.ArtistID
                LEFT JOIN silver.albumTracks AS saltr
                    ON salb.AlbumID = saltr.AlbumID
                LEFT JOIN silver.artistGenre AS sag
                    ON sart.ArtistID = sag.ArtistID
                GROUP BY 
                    sart.ArtistID
            ) AS source
                ON target.ArtistID = source.ArtistID

            WHEN MATCHED AND (
                target.Followers <> source.Followers
                OR target.Popularity <> source.Popularity
                OR target.LatestReleaseDateID <> source.LatestReleaseDateID
            )
            THEN UPDATE SET
                target.Followers = source.Followers,
                target.Popularity = source.Popularity,
                target.LatestReleaseDateID = source.LatestReleaseDateID

            WHEN NOT MATCHED BY TARGET THEN
                INSERT (
                    ArtistID,
                    LatestReleaseDateID,
                    GenreID,
                    Followers,
                    Popularity,
                    AvailableAlbumsCount,
                    ArtistTotalTracks,
                    TotalAlbumsDurationMS,
                    AvgTrackDurationMS,
                    AvgAlbumTracks
                )
                VALUES (
                    source.ArtistID,
                    source.LatestReleaseDateID,
                    source.GenreID,
                    source.Followers,
                    source.Popularity,
                    source.AvailableAlbumsCount,
                    source.ArtistTotalTracks,
                    source.TotalAlbumsDurationMS,
                    source.AvgTrackDurationMS,
                    source.AvgAlbumTracks
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