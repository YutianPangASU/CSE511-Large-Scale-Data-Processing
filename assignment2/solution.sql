-- q1
DROP TABLE IF EXISTS query1;
CREATE TABLE query1 AS
(
	SELECT G.name, count as moviecount
	FROM (
		SELECT count(H.movieid), H.genreid
		FROM hasagenre H
		GROUP BY H.genreid) C, genres G
	WHERE C.genreid = G.genreid
);


-- q2
DROP TABLE IF EXISTS query2;
CREATE TABLE query2 AS
(
	SELECT G.name, C.rating
	FROM (
		SELECT H.genreid, AVG(R.rating) as rating
		FROM hasagenre H, ratings R
		WHERE H.movieid = R.movieid
		GROUP BY genreid) C, genres G
	WHERE G.genreid = C.genreid
);


-- q3
DROP TABLE IF EXISTS query3;
CREATE TABLE query3 AS
(
	SELECT M.title, C.CountOfRatings
	FROM (
		SELECT count(R.rating) as CountOfRatings, R.movieid
		FROM ratings R
		GROUP BY R.movieid) C, movies M
	WHERE C.movieid = M.movieid AND C.CountOfRatings>=10
);


-- q4
DROP TABLE IF EXISTS query4;
CREATE TABLE query4 AS
(
	SELECT M.movieid, M.title
	FROM movies M, hasagenre H
	WHERE M.movieid = H.movieid AND H.genreid=(SELECT G.genreid 
											   FROM genres G 
											   WHERE G.name = 'Comedy')
);


-- q5
DROP TABLE IF EXISTS query5;
CREATE TABLE query5 AS
(
	SELECT M.title, C.average
	FROM (
		SELECT AVG(R.rating) AS average, R.movieid
		FROM ratings R
		GROUP BY R.movieid) C, movies M
	WHERE M.movieid = C.movieid
);


-- q6
DROP TABLE IF EXISTS query6;
CREATE TABLE query6 AS
(
	SELECT AVG(R.rating) AS average
	FROM hasagenre H, ratings R
	WHERE H.movieid = R.movieid AND H.genreid = (SELECT G.genreid 
											     FROM genres G 
											     WHERE G.name = 'Comedy')
);


-- q7
DROP TABLE IF EXISTS query7;
CREATE TABLE query7 AS
(
	SELECT AVG(R.rating)
	FROM ratings R
	WHERE R.movieid IN
		(SELECT H.movieid
		 FROM hasagenre H
		 WHERE H.movieid IN
			(SELECT H.movieid
			FROM hasagenre H
			WHERE H.genreid = (SELECT G.genreid 
							   FROM genres G 
							   WHERE G.name = 'Romance')
			) 
		 AND H.genreid = (SELECT G.genreid 
						  FROM genres G 
						  WHERE G.name = 'Comedy'))
);


-- q8
DROP TABLE IF EXISTS query8;
CREATE TABLE query8 AS
(
	SELECT AVG(R.rating)
	FROM ratings R
	WHERE R.movieid IN
		(SELECT H.movieid
		 FROM hasagenre H
		 WHERE H.movieid NOT IN
			(SELECT H.movieid
			FROM hasagenre H
			WHERE H.genreid = (SELECT G.genreid 
							   FROM genres G 
							   WHERE G.name = 'Comedy')
			) 
		 AND H.genreid = (SELECT G.genreid 
						  FROM genres G 
						  WHERE G.name = 'Romance'))
);


-- q9
DROP TABLE IF EXISTS query9;
CREATE TABLE query9 AS
(
	SELECT R.movieid, R.rating
	FROM ratings R
	WHERE R.userid = :v1
);


-- q10
DROP TABLE IF EXISTS avg_rating;
CREATE TABLE avg_rating AS
(
	SELECT C.average, M.movieid
	FROM (
		SELECT AVG(R.rating) AS average, R.movieid
		FROM ratings R
		GROUP BY R.movieid) C, movies M
	WHERE M.movieid = C.movieid
);


DROP TABLE IF EXISTS similarity;
CREATE TABLE similarity AS 
(
SELECT A.movieid AS movieid1, B.movieid AS movieid2, 1-(abs(A.average - B.average)/5) AS sim 
FROM avg_rating A, avg_rating B 
WHERE A.movieid != B.movieid
);


DROP TABLE IF EXISTS user_ratings;
CREATE TABLE user_ratings AS
(
	SELECT R.movieid, R.rating
	FROM ratings R
	WHERE R.userid = :v1
);


DROP TABLE IF EXISTS predictions;
CREATE TABLE predictions AS
(
SELECT S.movieid1 AS movieid, SUM(S.sim*U.rating)/SUM(S.sim) AS pred_rating
FROM similarity S, user_ratings U
WHERE S.movieid2 = U.movieid AND S.movieid1 NOT IN (SELECT U.movieid FROM user_ratings U) 
GROUP BY S.movieid1 
ORDER BY pred_rating ASC
);


DROP TABLE IF EXISTS recommendation;
CREATE TABLE recommendation AS
(
SELECT M.title
FROM predictions P, movies M
WHERE P.movieid = M.movieid AND P.pred_rating > 3.9
);

-- SELECT * FROM recommendation;








