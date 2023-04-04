from flask import Flask, flash, request, jsonify
import psycopg2
from con import set_connection
from loggerinstance import logger

app = Flask(__name__)


def handle_exceptions(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            logger.exception(str(e))
            return str(e), 500

    return wrapper


@app.route("/v1/create", methods=["POST"])
@handle_exceptions
def create_account():
    cur, conn = set_connection()
    if not cur:
        raise Exception("Failed to connect to database")

    # extract data from the JSON payload
    data = request.json
    holder_name = data.get('holder_name')
    account_type = data.get('account_type')
    balance = data.get('balance')

    if not holder_name or not account_type or not balance:
        raise Exception("Missing required field(s)")

    cur.execute(
        'INSERT INTO bank1(holder_name, account_type, balance)'
        ' VALUES (%s, %s, %s);',
        (holder_name, account_type, balance)
    )
    conn.commit()
    cur.close()
    conn.close()

    logger.info("Account created successfully")
    return "Account created successfully", 201


# READ the details
@app.route("/v1/show", methods=["GET"], endpoint='show_accounts')
@handle_exceptions
def show_accounts():
    cur, conn = set_connection()
    if not cur:
        raise Exception("Failed to connect to database")

    cur.execute("SELECT * FROM bank1")
    data = cur.fetchall()

    conn.commit()
    cur.close()
    conn.close()

    logger.info("Retrieved account list")
    return str(data), 200


@app.route("/v1/withdraw", methods=["PUT"], endpoint='withdrawal')
@handle_exceptions
def withdrawal():
    cur, conn = set_connection()
    if not cur:
        raise Exception("Failed to connect to database")

    srno = request.json.get("srno")
    amount = request.json.get("withdraw_amount")

    if not srno or not amount:
        raise Exception("Missing required field(s)")

    cur.execute("SELECT balance FROM bank1 WHERE srno = %s", (srno,))
    result = cur.fetchone()

    if not result:
        raise Exception("Account not found")

    balance = result[0]

    if int(balance) < int(amount):
        raise Exception("Insufficient balance")

    updated_amt = int(balance) - int(amount)

    cur.execute("""UPDATE bank1
                    SET balance = %s
                    WHERE srno = %s
                """, (updated_amt, srno))
    conn.commit()
    cur.close()
    conn.close()

    response = {
        "withdraw_amount": amount,
        "new_balance": updated_amt
    }

    logger.info("Withdrawal successful")
    return jsonify(response), 200


@app.route("/v1/deposit", methods=["PUT"], endpoint='deposit')
@handle_exceptions
def deposit():
    cur, conn = set_connection()
    if not cur:
        raise Exception("Failed to connect to database")

    srno = request.json.get("srno")
    amount = request.json.get("deposit_amount")
    if not srno or not amount:
        raise Exception("Missing required field(s)")

    cur.execute("SELECT balance FROM bank1 WHERE srno = %s", (srno,))
    result = cur.fetchone()

    if not result:
        raise Exception("Account not found")

    balance = result[0]

    updated_amt = int(balance) + int(amount)

    cur.execute("""UPDATE bank1
                      SET balance = %s
                      WHERE srno = %s
                  """, (updated_amt, srno))

    conn.commit()
    cur.close()
    conn.close()

    response = {
        "deposited_amount": amount,
        "new_balance": updated_amt
    }

    logging.info(f"Deposit of {amount} successful for srno {srno}")
    return jsonify(response), 200


@app.route("/v1/delete/<int:srno>", methods=["DELETE"], endpoint='delete_account')
def delete_account(srno):
    cur, conn = set_connection()
    cur.execute("DELETE FROM bank1 WHERE srno = %s", (srno,))
    conn.commit()
    cur.close()
    conn.close()
    return "Record deleted successfully", 200


if __name__ == "__main__":
    app.run(debug=True, port=5000)
