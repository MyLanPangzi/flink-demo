 CREATE USER 'maxwell'@'%' IDENTIFIED BY '!QAZ2wsx';
 GRANT ALL ON *.* TO 'maxwell'@'%';
 GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'%';

-- # or for running maxwell locally:

 CREATE USER 'maxwell'@'localhost' IDENTIFIED BY 'XXXXXX';
 GRANT ALL ON maxwell.* TO 'maxwell'@'localhost';
 GRANT SELECT, REPLICATION CLIENT, REPLICATION SLAVE ON *.* TO 'maxwell'@'localhost';