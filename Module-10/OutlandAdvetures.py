import mysql.connector
from mysql.connector import errorcode

config = {
    "user": "root",
    "password": input("Please enter your root database password: "), #YOUR MYSQL PASSWORD HERE!
    "host": "127.0.0.1",
    "raise_on_warnings": True
}

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
        input("\nPress ENTER to continue...")

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
               UnitPrice double DEFAULT 0.99,
               Quantity int DEFAULT 1,
               IntakeDate date DEFAULT (CURRENT_DATE),
               PRIMARY KEY(ItemID));""",
            """CREATE TABLE EquipmentSales(
               TransactionID int NOT NULL AUTO_INCREMENT,
               Category varchar(255) NOT NULL DEFAULT 'rental',
               ItemID int NOT NULL,
               Quantity int,
               Price double,
               EmployeeID int,
               CustomerID int,
               TransactionDate date DEFAULT (CURRENT_DATE),
               CHECK(Category IN('rental', 'sale', 'return')),
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
               FOREIGN KEY(LocationID) REFERENCES Locations(LocationID));"""
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
            """INSERT INTO Inventory (ItemID, Description, UnitPrice, Quantity, IntakeDate)
               VALUES
               (1, 'Hiking Boots', 99.99, 4, '2019-08-15'),
               (2, 'Hiking Poles', 65.99, 6, '2022-01-23'),
               (3, 'Tents', 350, 3, '2018-04-30'),
               (4, 'Backpacks', 99.95, 7, '2023-07-28'),
               (5, 'Flashlights', 30.99, 9, '2017-08-08'),
               (6, 'Water Bottles', 45.98, 7, '2021-05-22'),
               (7, 'First Aid Kits', 34.99, 5, '2024-01-01');""",
            """INSERT INTO EquipmentSales (Category, ItemID, Quantity, Price, EmployeeID, CustomerID, TransactionDate)
               VALUES
               ('sale', 1, 3, 299.97, 1, 1, '2023-05-12'),
               ('sale', 2, 2, 131.98, 2, 2, '2023-07-24'),
               ('rental', 3, 1, 350.00, 3, 3, '2020-06-03'),
               ('sale', 1, 4, 399.96, 4, 4, '2022-04-28'),
               ('rental', 2, 2, 131.98, 2, 5, '2022-09-27'),
               ('sale', 3, 3, 1050.00, 3, 6, '2021-05-18');""",
            """INSERT Locations(LocationID, Location)
               VALUES
               (1, 'South Africa'),
               (2, 'Tanzania'),
               (3, 'Italy'),
               (4, 'Spain'),
               (5, 'Nepal'),
               (6, 'Vietnam');""",
            """INSERT Trips(TripID, CustomerID, EmployeeID, LocationID, Price, TripDate)
               VALUES
               (1, 3, 3, 6, 1623.93, '2024-03-04'),
               (2, 5, 4, 1, 2027.34, '2024-10-28'),
               (3, 1, 4, 3, 1051.24, '2024-04-22'),
               (4, 2, 3, 3, 2424.86, '2024-12-24'),
               (5, 6, 3, 5, 1709.65, '2024-06-23'),
               (6, 4, 4, 2, 1815.34, '2024-08-06');"""
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
            print("\nItem ID: {} | Description: {} | Unit Price: {} | Quantity: {} | Intake Date: {}".format(inventory[0], inventory[1], inventory[2], inventory[3], inventory[4]))

        #Display EquipmentSales table
        cursor.execute("""SELECT *
                       FROM EquipmentSales""")
        
        print("\n--DISPLAYING EquipmentSales--")
        for sale in cursor:
            print("\nTransaction ID: {} | Category: {} | Item ID: {} | Quantity: {} | Price {} | Employee ID: {} | Customer ID: {} | Transaction Date: {}".format(sale[0], sale[1], sale[2], sale[3], sale[4], sale[5], sale[6], sale[7]))

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
        input("\nPress ENTER to continue...")


def display_reports(config):
    config["database"] = "OutlandAdventures" #Adds OutlandAdventures database to the connector config

    try:
        db = mysql.connector.connect(**config)
        
        print("\nDatabase user {} connected to MySQL on host {} with database {}".format(config["user"], config["host"], config["database"]))

        cursor = db.cursor()

        #Question: Do enough customers buy equipment to keep equipment sales? 
        #This can be narrowed down using WHERE and by Category and TransactionDate BETWEEN
        cursor.execute("""SELECT EquipmentSales.ItemID AS 'Item ID', Inventory.Description, SUM(EquipmentSales.Quantity) AS 'Amount Sold'
                          FROM EquipmentSales
                          LEFT JOIN Inventory
                          ON EquipmentSales.ItemID = Inventory.ItemID
                          GROUP BY EquipmentSales.ItemID;""")
        
        print("\n--DISPLAYING TOTAL AMOUNT SOLD OF EACH ITEM IN INVENTORY--")        
        for record in cursor:
            print("\nItemID: {} | Description: {} | Amount Sold: {}".format(record[0], record[1],record[2]))

        #Question: Is there anyone of those locations that has a downward trend in bookings?
        #This can be narrowed down by WHERE TripDate BETWEEN
        cursor.execute("""SELECT Locations.Location, COUNT(Trips.LocationID) AS 'Total Trips'
                          FROM Trips
                          LEFT JOIN Locations
                          ON Trips.LocationID = Locations.LocationID
                          GROUP BY Trips.LocationID;""")
        
        print("\n--DISPLAYING AMOUNT OF TRIPS TAKEN TO A LOCATION--")
        for record in cursor:
            print("\nLocation: {} | Total Trips: {}".format(record[0], record[1]))

        #Question: Are there inventory items that are over five years old?
        cursor.execute("""SELECT ItemID, Description, DATEDIFF(CURDATE(), IntakeDate)/365 AS 'Age'
                          FROM Inventory
                          WHERE DATEDIFF(CURDATE(), IntakeDate)/365 >= 5""")
        
        print("\n--DISPLAYING INVENTORY ITEMS 5+ YEARS OLD--")
        for record in cursor:
            print("\nItem ID: {} | Description: {} | Item Age: {}".format(record[0], record[1], record[2]))

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
        input("\nPress ENTER to continue...")

create_database(config)

create_tables(config)

display_reports(config)

input("\nPress ENTER to exit...")