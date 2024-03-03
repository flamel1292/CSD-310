import mysql.connector
from mysql.connector import errorcode

def completion_time(cursor):
        cursor.execute("SELECT NOW()")
        datetime = cursor.fetchone()
        print("\nThis report was completed on: {}".format(datetime[0]))

def create_database(config):
    try:
        db = mysql.connector.connect(**config)
        
        print("\nDatabase user {} connected to MySQL on host {}".format(config["user"], config["host"]))

        cursor = db.cursor()

        cursor.execute("SHOW DATABASES LIKE 'OutlandAdventures';")
        database_exists = cursor.fetchone()

        if database_exists:
            cursor.execute("DROP DATABASE OutlandAdventures;") #Deletes database if it exists
        
        cursor.execute("CREATE DATABASE OutlandAdventures;") #Creates new database named OutlandAdventures
        
        #Displays existing databases
        cursor.execute("SHOW DATABASES;")

        print("\n--DATABASE LIST--")
        for dbname in cursor:
            print(dbname[0])


    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("The supplied username or password are invalid")
        else:
            print(err)

    finally:
        cursor.close()
        db.close()

def create_tables(config):
    config["database"] = "OutlandAdventures" #Adds OutlandAdventures database to the connector config

    try:
        db = mysql.connector.connect(**config)
        
        print("\nDatabase user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))

        cursor = db.cursor()

        #List of CREATE TABLE statements due to being unable to use a sing string to create multiple tables
        tables = [
            """CREATE TABLE EMPLOYEES(
               EmployeeID int NOT NULL AUTO_INCREMENT,
               FirstName varchar(255) NOT NULL,
               LastName varchar(255) NOT NULL,
               JobTitle varchar(255) NOT NULL,
               PRIMARY KEY(EmployeeID));""",
            """CREATE TABLE CUSTOMERS(
               CustomerID int NOT NULL AUTO_INCREMENT,
               FirstName varchar(255)NOT NULL,
               LastName varchar(255) NOT NULL,
               Inoculation bool DEFAULT false,
               TravelVisa bool DEFAULT false,
               PRIMARY KEY(CustomerID));""",
            """CREATE TABLE Inventory(
               ItemID int NOT NULL AUTO_INCREMENT,
               Description varchar(255) NOT NULL,
               UnitPrice decimal(30, 2) DEFAULT 0.99,
               RentalPrice decimal(30, 2) DEFAULT 0.99,
               Quantity int DEFAULT 1,
               IntakeDate date DEFAULT (CURRENT_DATE),
               PRIMARY KEY(ItemID));""",
            """CREATE TABLE EquipmentSales(
               TransactionID int NOT NULL AUTO_INCREMENT,
               Category varchar(255) NOT NULL DEFAULT 'rental',
               ItemID int NOT NULL,
               Quantity int,
               Price decimal(30, 2) DEFAULT 0.00,
               EmployeeID int,
               CustomerID int,
               TransactionDate date DEFAULT (CURRENT_DATE),
               CHECK(Category IN('rental', 'sale')),
               PRIMARY KEY(TransactionID, ItemID, EmployeeID, CustomerID),
               FOREIGN KEY(ItemID) REFERENCES Inventory(ItemID),
               FOREIGN KEY(EmployeeID) REFERENCES Employees(EmployeeID),
               FOREIGN KEY(CustomerID) REFERENCES Customers(CustomerID));""",
            """CREATE TABLE Locations(
               LocationID int NOT NULL AUTO_INCREMENT,
               Location varchar(255) NOT NULL,
               PRIMARY KEY(LocationID));""",
            """CREATE TABLE Trips(
               TripID int NOT NULL AUTO_INCREMENT,
               CustomerID int NOT NULL,
               EmployeeID int NOT NULL,
               LocationID int NOT NULL,
               Price double,
               TripDate date DEFAULT (CURRENT_DATE),
               PRIMARY KEY(TripID),
               FOREIGN KEY(CustomerID) REFERENCES Customers(CustomerID),
               FOREIGN KEY(EmployeeID) REFERENCES Employees(EmployeeID),
               FOREIGN KEY(LocationID) REFERENCES Locations(LocationID));""",
            """CREATE TRIGGER calculate_price
               BEFORE INSERT ON EquipmentSales
               FOR EACH ROW
               BEGIN
                 IF NEW.Category = 'sale' THEN
                    SET NEW.Price = NEW.Quantity * (SELECT UnitPrice FROM Inventory WHERE ItemID = NEW.ItemID);
                 ELSE
                    SET NEW.Price = NEW.Quantity * (SELECT RentalPrice FROM Inventory WHERE ItemID = NEW.ItemID);
                 END IF;
               END;"""
        ]

        #List of INSERT statements to populate each table, similar reasoning for CREATE statements just for consistency and avoiding issues.
        insert = [
            """INSERT Employees(FirstName, LastName, JobTitle)
               VALUES
               ('Blythe', 'Timmerson', 'Admin'),
               ('Jim', 'Ford', 'Admin'),
               ('John','MacNell', 'Guide'),
               ('D.B', 'Marland', 'Guide'),
               ('Anita', 'Gallegos','Marketing'),
               ('Dimitrios', 'Stravopolous', 'Supply'),
               ('Mei', 'Wong', 'Developer');""",
            """INSERT Customers(FirstName, LastName, Inoculation, TravelVisa)
               VALUES
               ('Albert', 'Rosenfeld', true, true),
               ('Robert', 'Kessler', true, true),
               ('Samantha', 'Dixon', true, true),
               ('Wanda', 'Valentino', true, true),
               ('Alan', 'Smithee', true, true),
               ('Eleanor', 'Mitchell', true, true);""",
            """INSERT INTO Inventory (ItemID, Description, UnitPrice, RentalPrice, Quantity, IntakeDate)
               VALUES
               (1, 'Hiking Boots', 99.99, 20.00, 4, '2019-08-15'),
               (2, 'Hiking Poles', 65.99, 15.00, 6, '2022-01-23'),
               (3, 'Tents', 350.00, 50.00, 3, '2018-04-30'),
               (4, 'Backpacks', 99.95, 30.00, 7, '2023-07-28'),
               (5, 'Flashlights', 30.99, 5.00, 9, '2017-08-08'),
               (6, 'Water Bottles', 45.98, 10.00, 7, '2021-05-22'),
               (7, 'First Aid Kits', 34.99, 15.00, 5, '2020-01-01');""",
            """INSERT INTO EquipmentSales (Category, ItemID, Quantity, EmployeeID, CustomerID, TransactionDate)
               VALUES
               ('sale', 1, 3, 1, 1, '2023-05-12'),
               ('sale', 2, 2, 2, 2, '2023-07-24'),
               ('rental', 3, 1, 3, 3, '2021-06-03'),
               ('sale', 1, 4, 4, 4, '2022-04-28'),
               ('rental', 2, 2, 2, 5, '2022-09-27'),
               ('sale', 3, 3, 3, 6, '2021-05-18'),
               ('rental', 1, 3, 2, 4, '2023-07-15'),
               ('sale', 3, 1, 1, 3, '2022-05-20'),
               ('sale', 2, 2, 4, 2, '2021-12-10'),
               ('rental', 5, 4, 3, 5, '2022-03-05'),
               ('sale', 4, 1, 5, 1, '2023-09-28'),
               ('sale', 6, 2, 2, 4, '2023-08-18'),
               ('sale', 1, 1, 1, 3, '2023-11-02'),
               ('rental', 3, 3, 4, 2, '2021-06-14'),
               ('sale', 6, 1, 3, 5, '2021-04-25'),
               ('rental', 2, 5, 5, 1, '2021-01-30'),
               ('rental', 1, 2, 3, 1, '2021-05-10'),
               ('sale', 4, 1, 2, 2, '2022-08-23'),
               ('rental', 7, 3, 5, 3, '2022-11-15'),
               ('sale', 2, 1, 4, 4, '2022-02-08'),
               ('rental', 5, 2, 1, 5, '2023-07-30'),
               ('rental', 3, 1, 2, 1, '2021-09-12'),
               ('sale', 6, 1, 3, 4, '2023-04-18'),
               ('rental', 1, 2, 5, 2, '2022-12-05'),
               ('sale', 7, 1, 4, 5, '2022-01-22'),
               ('rental', 4, 3, 1, 3, '2022-11-03'),
               ('rental', 3, 5, 2, 4, '2022-08-15'),
               ('sale', 6, 2, 1, 3, '2021-05-20'),
               ('rental', 2, 3, 5, 6, '2021-11-10'),
               ('sale', 4, 1, 3, 2, '2023-03-25'),
               ('rental', 7, 4, 4, 1, '2022-09-03'),
               ('rental', 1, 2, 2, 5, '2021-12-12'),
               ('sale', 5, 3, 1, 6, '2022-01-08'),
               ('rental', 3, 2, 5, 4, '2023-06-17'),
               ('sale', 7, 1, 4, 3, '2022-07-22'),
               ('sale', 2, 4, 3, 2, '2022-04-05');""",
            """INSERT Locations(LocationID, Location)
               VALUES
               (1, 'South Africa'),
               (2, 'Tanzania'),
               (3, 'Italy'),
               (4, 'Spain'),
               (5, 'Nepal'),
               (6, 'Vietnam');""",
            """INSERT Trips(CustomerID, EmployeeID, LocationID, Price, TripDate)
               VALUES
               (3, 3, 6, 1623.93, '2021-03-04'),
               (5, 4, 1, 2027.34, '2022-10-28'),
               (1, 4, 3, 1051.24, '2023-04-22'),
               (2, 3, 3, 2424.86, '2021-12-24'),
               (6, 3, 5, 1709.65, '2022-06-23'),
               (4, 4, 2, 1815.34, '2023-08-06'),
               (3, 3, 6, 1623.93, '2023-03-04'),
               (5, 4, 1, 2027.34, '2021-10-28'),
               (1, 4, 3, 1051.24, '2023-04-22'),
               (2, 3, 3, 2424.86, '2022-12-24'),
               (6, 3, 5, 1709.65, '2021-06-23'),
               (4, 4, 2, 1815.34, '2023-08-06'),
               (3, 3, 6, 1623.93, '2021-03-04'),
               (5, 4, 1, 2027.34, '2022-10-28'),
               (1, 4, 3, 1051.24, '2023-04-22'),
               (2, 3, 3, 2424.86, '2021-12-24'),
               (6, 3, 5, 1709.65, '2022-06-23'),
               (4, 4, 2, 1815.34, '2023-08-06'),
               (3, 3, 6, 1623.93, '2023-03-04'),
               (5, 4, 1, 2027.34, '2021-10-28'),
               (1, 4, 3, 1051.24, '2023-04-22'),
               (2, 3, 3, 2424.86, '2022-12-24'),
               (6, 3, 5, 1709.65, '2021-06-23'),
               (4, 4, 2, 1815.34, '2023-08-06'),
               (1, 1, 1, 1000.00, '2021-05-10'),
               (2, 2, 2, 1200.50, '2021-07-15'),
               (3, 3, 3, 800.75, '2021-09-20'),
               (4, 4, 4, 1500.25, '2022-02-28'),
               (5, 5, 5, 2000.00, '2022-04-10'),
               (6, 6, 6, 1800.50, '2022-06-15'),
               (1, 1, 1, 900.75, '2022-08-20'),
               (2, 2, 2, 1100.25, '2022-10-28'),
               (3, 3, 3, 1300.50, '2023-01-05'),
               (4, 4, 4, 1700.25, '2023-03-10'),
               (5, 5, 5, 1900.75, '2023-05-15'),
               (6, 6, 6, 2100.50, '2023-07-20'),
               (1, 1, 1, 950.75, '2023-09-25'),
               (2, 2, 2, 1150.25, '2023-11-01'),
               (3, 3, 3, 1350.50, '2023-12-15'),
               (4, 4, 4, 1600.25, '2021-01-20'),
               (5, 5, 5, 1800.75, '2021-03-28'),
               (6, 6, 6, 2000.50, '2021-06-05'),
               (1, 1, 1, 1000.75, '2021-08-10'),
               (2, 2, 2, 1200.25, '2021-10-15');"""
        ]

        #Executing both the CREATE & INSERT statements
        for table in tables:
            cursor.execute(table)
        
        for query in insert:
            cursor.execute(query)
        
        db.commit()

        #Display existing tables
        cursor.execute("SHOW TABLES;")

        print("\n--TABLE LIST IN OutlandAdventures--")
        for tablename in cursor:
            print(tablename[0])
        
        #Display Employees table
        cursor.execute("""SELECT *
                       FROM EMPLOYEES""")
        
        print("\n--DISPLAYING EMPLOYEES--")
        for employee in cursor:
            print("\nEmployee ID: {} | First Name: {} | Last Name: {} | Job Title: {}".format(employee[0], employee[1], employee[2], employee[3]))
        
        #Display Customers table
        cursor.execute("""SELECT *
                       FROM Customers""")
        
        print("\n--DISPLAYING Customer--")
        for customer in cursor:
            print("\nCustomer ID: {} | First Name: {} | Last Name: {} | Inoculated: {} | Travel Visa: {}".format(customer[0], customer[1], customer[2], customer[3], customer[4]))

        #Display Inventory table
        cursor.execute("""SELECT *
                       FROM Inventory""")
        
        print("\n--DISPLAYING Inventory--")
        for inventory in cursor:
            print("\nItem ID: {} | Description: {} | Unit Price: {} | Rental Price: {} | Quantity: {} | Intake Date: {}".format(inventory[0], inventory[1], inventory[2], inventory[3], inventory[4], inventory[5]))

        #Display EquipmentSales table
        cursor.execute("""SELECT *
                       FROM EquipmentSales""")
        
        print("\n--DISPLAYING EquipmentSales--")
        for sale in cursor:
            print("\nTransaction ID: {} | Category: {} | Item ID: {} | Quantity: {} | Price: {} | Employee ID: {} | Customer ID: {} | Transaction Date: {}".format(sale[0], sale[1], sale[2], sale[3], sale[4], sale[5], sale[6], sale[7]))

        #Display Locations table
        cursor.execute("""SELECT *
                       FROM Locations""")
        
        print("\n--DISPLAYING Locations--")
        for location in cursor:
            print("\nLocation ID: {} | Location: {}".format(location[0], location[1]))

        #Display Trips table
        cursor.execute("""SELECT *
                       FROM Trips""")
        
        print("\n--DISPLAYING Trips--")
        for trip in cursor:
            print("\nTrip ID: {} | Customer ID: {} | Employee ID: {} | Location ID: {} | Price: {} | Trip Date: {}".format(trip[0], trip[1], trip[2], trip[3], trip[4], trip[5]))

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("The supplied username or password are invalid")

        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          print("The specified database does not exist")

        else:
            print(err)

    finally:
        cursor.close()
        db.close()


def display_reports(config):
    config["database"] = "OutlandAdventures" #Adds OutlandAdventures database to the connector config

    try:
        db = mysql.connector.connect(**config)
        
        print("\nDatabase user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))

        cursor = db.cursor()

        #Asks for specific year to search through records
        year = int(input("\nPlease enter the year you would like to search sales and rental data: "))

        #Question: Do enough customers buy equipment to keep equipment sales?
        cursor.execute(f"""SELECT
                              i.ItemID,
                              i.Description,
                              COALESCE(YEAR(s.TransactionDate), {year}) AS SalesYear,
                              SUM(CASE WHEN s.Category = 'sale' THEN s.Quantity ELSE 0 END) AS TotalSalesQuantity,
                              SUM(CASE WHEN s.Category = 'sale' THEN s.Price ELSE 0 END) AS TotalSalesAmount,
                              SUM(CASE WHEN s.Category = 'rental' THEN s.Quantity ELSE 0 END) AS TotalRentalsQuantity,
                              SUM(CASE WHEN s.Category = 'rental' THEN s.Price ELSE 0 END) AS TotalRentalsAmount
                           FROM Inventory i
                           LEFT JOIN EquipmentSales s 
                           ON i.ItemID = s.ItemID AND YEAR(s.TransactionDate) = {year}
                           GROUP BY i.ItemID, i.Description, SalesYear;""")
        
        print(f"\n--DISPLAYING SALES AND RENTAL DATA FOR THE YEAR {year}--")
        for record in cursor:
            print("\nItem ID: {} | Description: {} | Sales Year: {} | Total Sales: {} | Total Sales Revenue: {} | Total Rentals: {} | Total Rental Revenue: {}".format(record[0], record[1], record[2], record[3], record[4], record[5], record[6]))

        completion_time(cursor)

        #Question: Is there anyone of those locations that has a downward trend in bookings?
        year = int(input("\nPlease enter the year you would like to search for trends: "))

        cursor.execute(f"""SELECT
                              l.LocationID,
                              l.Location,
                              COUNT(CASE WHEN YEAR(t.TripDate) = {year - 1} THEN t.TripID END),
                              COUNT(CASE WHEN YEAR(t.TripDate) = {year} THEN t.TripID END),
                              CASE
                                  WHEN COUNT(CASE WHEN YEAR(t.TripDate) = {year} THEN t.TripID END) > COUNT(CASE WHEN YEAR(t.TripDate) = {year - 1} THEN t.TripID END) THEN 'Rise'
                                  WHEN COUNT(CASE WHEN YEAR(t.TripDate) = {year} THEN t.TripID END) < COUNT(CASE WHEN YEAR(t.TripDate) = {year - 1} THEN t.TripID END) THEN 'Fall'
                                  ELSE 'No Change'
                              END
                          FROM Locations l
                          LEFT JOIN Trips t
                          ON l.LocationID = t.LocationID
                          GROUP BY l.LocationID, l.Location;""")
        
        print("\n--DISPLAYING TRENDS FOR TRIPS TAKEN TO LOCATIONS--")
        for record in cursor:
            print(f"\nLocation ID: {record[0]} | Location: {record[1]} | Trips in {year - 1}: {record[2]} | Trips in {year}: {record[3]} | Trend: {record[4]}")
               
        completion_time(cursor)

        #Question: Are there inventory items that are over five years old?
        cursor.execute("""SELECT ItemID, Description, TRUNCATE(DATEDIFF(CURDATE(), IntakeDate)/365, 0)
                          FROM Inventory
                          WHERE DATEDIFF(CURDATE(), IntakeDate)/365 >= 5;""")
        
        print("\n--DISPLAYING INVENTORY ITEMS 5+ YEARS OLD--")
        for record in cursor:
            print("\nItem ID: {} | Description: {} | Item Age: {}".format(record[0], record[1], record[2]))
        
        completion_time(cursor)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("The supplied username or password are invalid")

        elif err.errno == errorcode.ER_BAD_DB_ERROR:
          print("The specified database does not exist")

        else:
            print(err)

    finally:
        cursor.close()
        db.close()

def main():
    config = {
    "user": "root",
    "password": input("Please enter your root database password: "), #YOUR MYSQL PASSWORD HERE!
    "host": "127.0.0.1",
    "raise_on_warnings": True
    }

    create_database(config)

    input("\nPress ENTER to continue...")

    create_tables(config)

    input("\nPress ENTER to continue...")

    display_reports(config)

    input("\nPress ENTER to exit...")

if __name__ == '__main__':
    main()