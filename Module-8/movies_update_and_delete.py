import mysql.connector
from mysql.connector import errorcode

config = {
    "user": "movies_user",
    "password": "popcorn",
    "host": "127.0.0.1",
    "database": "movies",
    "raise_on_warnings": True
}

def show_films(cursor, title):
     cursor.execute("""SELECT film_name AS Name, film_director as Director, genre_name AS Genre, studio_name AS 'Studio Name'
                    FROM film
                    INNER JOIN genre ON film.genre_id = genre.genre_id
                    INNER JOIN studio ON film.studio_id = studio.studio_id""".format(title))
     
     films = cursor.fetchall()

     print("\n -- {} --".format(title))
     for film in films:
          print(" Film Name: {}\n Director: {}\n Genre: {}\n Studio Name: {}\n".format(film[0], film[1], film[2], film[3]))

try:
     db = mysql.connector.connect(**config)
     
     print("\n Database user {} connected to MySQL on host with database {}".format(config["user"], config["host"], config["database"]))

     cursor = db.cursor()

     #Display
     show_films(cursor, "DISPLAYING FILMS")

     #Insert
     cursor.execute("""INSERT INTO film (film_director, film_name, film_releaseDate, film_runtime, genre_id, studio_id)
                    VALUES('Gerad Johnstone', 'M3GAN', '2022', 102, 1, 2)""")
     
     show_films(cursor, "DISPLAYING FILMS AFTER INSERT")

     #Update
     cursor.execute("""UPDATE film
                    SET genre_id = 1
                    WHERE film_name = 'Alien'""")
     
     show_films(cursor, "DISPLAYING FILMS AFTER UPDATE - Changed Alien to Horror")

     #Delete
     cursor.execute("""DELETE FROM film
                    WHERE film_name = 'Gladiator'""")
     
     show_films(cursor, "DISPLAYING FILMS AFTER DELETE")
    
     input("\n Press any key to continue...")

except mysql.connector.Error as err:
     if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
          print(" The supplied username or password are invalid")

     elif err.errno == errorcode.ER_BAD_DB_ERROR:
          print(" The specified database does not exist")

     else:
          print(err)

finally:
     db.close()

