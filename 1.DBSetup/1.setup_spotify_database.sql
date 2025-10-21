use master
go

create database SpotifyDataBase;
go

create login sv_spotify with password = 'P@ssw0rd!';
go

use SpotifyDataBase;
go

create user sv_spotify for login sv_spotify;
create role SpotifyServiceUser;
go

create schema bronze;
go

grant select, insert, update, delete, execute 
on schema::bronze to SpotifyServiceUser;
alter role SpotifyServiceUser add member sv_spotify;
go
