CREATE TABLE query1 AS
SELECT g.name, moviecount
FROM (SELECT h.genreid, count(h.movieid) as moviecount
FROM hasagenre h
GROUP BY h.genreid) q1h, genres g
WHERE q1h.genreid = g.genreid;

-- QUERY 2
CREATE TABLE q2h1 AS
SELECT avg(r.rating) as rating, h.genreid
FROM hasagenre h, ratings r
WHERE h.movieid = r.movieid
GROUP BY genreid;

CREATE TABLE query2 AS
SELECT name, rating
FROM genres, q2h1
WHERE genres.genreid = q2h1.genreid;

DROP TABLE q2h1;
DROP TABLE q2helper1;
DROP TABLE q2helper2;

-- QUERY 3
CREATE TABLE query3 AS
SELECT title, countofratings
FROM movies, (
SELECT movieid, count(rating) as countofratings
FROM ratings
GROUP BY movieid
HAVING count(rating)>=10) helper
WHERE movies.movieid = helper.movieid;


--QUERY 4
CREATE TABLE query4 AS
SELECT movies.movieid, movies.title
FROM movies, hasagenre, genres
WHERE movies.movieid = hasagenre.movieid
AND hasagenre.genreid = genres.genreid
AND genres.name = 'Comedy';


--QUERY 5
CREATE TABLE query5 AS
SELECT movies.title, helper.average
FROM movies, 
(
SELECT ratings.movieid, avg(rating) as average
FROM movies, ratings
WHERE movies.movieid = ratings.movieid
GROUP BY ratings.movieid
) helper
WHERE movies.movieid = helper.movieid;


-- QUERY 6
CREATE TABLE query6 AS
SELECT avg(ratings.rating) AS average
FROM movies, hasagenre, genres, ratings
WHERE movies.movieid = hasagenre.movieid
AND hasagenre.genreid = genres.genreid
AND ratings.movieid = movies.movieid
AND genres.name = 'Comedy';


-- QUERY 7
CREATE TABLE query7 AS
SELECT avg(ratings.rating) AS average
FROM movies, hasagenre, genres, ratings
WHERE movies.movieid = hasagenre.movieid
AND hasagenre.genreid = genres.genreid
AND ratings.movieid = movies.movieid
AND genres.name = 'Romance'
AND movies.movieid IN
(
SELECT movies.movieid
FROM movies, hasagenre, genres
WHERE movies.movieid = hasagenre.movieid
AND hasagenre.genreid = genres.genreid
AND genres.name = 'Comedy'
);


-- QUERY 8
CREATE TABLE query8 AS
SELECT avg(ratings.rating) AS average
FROM movies, hasagenre, genres, ratings
WHERE movies.movieid = hasagenre.movieid
AND hasagenre.genreid = genres.genreid
AND ratings.movieid = movies.movieid
AND genres.name = 'Romance'
AND movies.movieid NOT IN
(
SELECT movies.movieid
FROM movies, hasagenre, genres
WHERE movies.movieid = hasagenre.movieid
AND hasagenre.genreid = genres.genreid
AND genres.name = 'Comedy'
);


-- QUERY 9
CREATE TABLE query9 AS
SELECT r.movieid, r.rating
FROM ratings r
WHERE r.userid = :v1;

-- QUERY 10
CREATE TABLE user2movie AS
SELECT r.movieid, r.rating
FROM ratings r
WHERE r.userid = :v1;

CREATE TABLE q10h1 AS
SELECT movieid, avg(rating) AS rating
FROM ratings
GROUP BY movieid;

-- Create movie to movie similarity table. Each row has two movies and a similarity
CREATE TABLE movie2movie AS
SELECT q1.movieid as movieid1, q2.movieid as movieid2, (1-(abs(q1.rating - q2.rating)/5)) as sim
FROM q10h1 q1, q10h1 q2
WHERE q1.movieid!=q2.movieid;

-- Generate predictions using weighted table
CREATE TABLE prediction AS
SELECT m.movieid1 as candidate,
  CASE SUM(m.sim) WHEN 0.0 THEN 0.0
                  ELSE SUM(m.sim*u.rating)/SUM(m.sim)
  END
AS predictionscore
FROM movie2movie m, user2movie u
WHERE m.movieid2 = u.movieid
AND m.movieid1 NOT IN (SELECT movieid FROM user2movie)
GROUP BY m.movieid1 ORDER BY predictionscore DESC;

-- Generate recommendations using prediction scores
CREATE TABLE recommendation AS
SELECT title
FROM movies, prediction
WHERE movies.movieid = prediction.candidate
AND prediction.predictionscore>3.9;
