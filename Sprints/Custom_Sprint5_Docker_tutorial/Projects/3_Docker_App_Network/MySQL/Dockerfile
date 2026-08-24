#Pull official MySQL image from Docker Hub
FROM mysql:9

#Set environment variables for MySQL root password, database name, user, and password
ENV MYSQL_ROOT_PASSWORD=demopassword
ENV MYSQL_DATABASE=demo_db
ENV MYSQL_USER=demo_user
ENV MYSQL_PASSWORD=demopassword

#Copy initialization script to the container
COPY init.sql /docker-entrypoint-initdb.d/

#Expose MySQL port 3307 for not conflicting original MySQL port 3306 on host machine
EXPOSE 3307

