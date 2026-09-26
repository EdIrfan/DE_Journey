import sqlite3

connection = sqlite3.connect("Sales_DB/sales.db")

cursor = connection.cursor()


# cursor.execute("DROP TABLE orders")


cursor.execute("""
    CREATE TABLE IF NOT EXISTS orders(
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_name TEXT NOT NULL,
        product_name TEXT NOT NULL,
        quantity INTEGER NOT NULL,
        price REAL NOT NULL,
        total REAL NOT NULL
    )
""")

cursor.execute("""
    INSERT INTO orders (customer_name,product_name,quantity,price,total) VALUES
        ("JOHN DOE","LAPTOP",2,1500.0,3000.0),
        ("JANE SMITH","SMART PHONE",3,200.0,600.0)
""")

connection.commit()
connection.close()
