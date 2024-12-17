import psycopg2

def create_server_connection():
   # Do not expose your Neon credentials to the browser
    PGHOST='ep-tiny-truth-a28lcfjp.eu-central-1.aws.neon.tech'
    PGDATABASE='datafundamentals'
    PGUSER='datafundamentals_owner'
    PGPASSWORD='ysREpYW2wV4j' 
    
    conn = None
    try:
        conn = psycopg2.connect(database=PGDATABASE, user=PGUSER, password=PGPASSWORD, host=PGHOST, port=5432)
        print("Database conn successful")
    except Error as err:
        print(f"Error: '{err}'")

    return conn


# JOIN
conn = create_server_connection()

cur = conn.cursor()
cur.execute("""SELECT DISTINCT p.customerNumber, p.checkNumber, p.paymentDate, p.amount FROM payments p 
LEFT JOIN sales s ON s.customerNumber=p.customerNumber WHERE s.customerNumber IS NOT NULL ORDER BY p.customerNumber""")

rows = cur.fetchall()
for row in rows:
    print(row)

cur.close()
conn.close()