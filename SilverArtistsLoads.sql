USE SpotifyDataBase
GO

CREATE OR ALTER PROCEDURE usp_bronze_to_silver_artist_load
AS
BEGIN
    SET NOCOUNT ON;

    BEGIN TRY
        BEGIN TRANSACTION;

        MERGE silver.artists AS target
        USING (
            SELECT DISTINCT 
                ArtistSpotifyID,
                ArtistName,
                Followers,
                Popularity
            FROM (
                SELECT --DISTINCT
                    ArtistSpotifyID,
                    ArtistName,
                    Followers,
                    Popularity,
                    ROW_NUMBER() OVER (PARTITION BY ArtistSpotifyID ORDER BY InsertedAt DESC) AS rn
                FROM bronze.artists
            ) AS numbered
            WHERE rn = 1
        ) AS source
        ON target.ArtistSpotifyID = source.ArtistSpotifyID

        WHEN MATCHED AND (
            target.ArtistName <> source.ArtistName
            OR target.Followers <> source.Followers
            OR target.Popularity <> source.Popularity
        )
        THEN UPDATE SET
            ArtistName = source.ArtistName,
            Followers  = source.Followers,
            Popularity = source.Popularity

        WHEN NOT MATCHED BY TARGET
        THEN INSERT (ArtistSpotifyID, ArtistName, Followers, Popularity)
             VALUES (source.ArtistSpotifyID, source.ArtistName, source.Followers, source.Popularity);

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF XACT_STATE() <> 0
            ROLLBACK TRANSACTION;
            
        THROW;
    END CATCH
END;
GO