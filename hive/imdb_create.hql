CREATE TABLE imdb
(id INT,
movie_title STRING, 
year INT, 
time_stamp STRING,
ranking_number INT)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\;'
LINES TERMINATED BY '\n';
