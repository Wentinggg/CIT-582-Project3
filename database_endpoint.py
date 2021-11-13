from flask import Flask, request, g
from flask_restful import Resource, Api
from sqlalchemy import create_engine, select, MetaData, Table
from flask import jsonify
import json
import eth_account
import algosdk
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import scoped_session
from sqlalchemy.orm import load_only

from models import Base, Order, Log

engine = create_engine('sqlite:///orders.db')
Base.metadata.bind = engine
DBSession = sessionmaker(bind=engine)

app = Flask(__name__)


# These decorators allow you to use g.session to access the database inside the request code
@app.before_request
def create_session():
    g.session = scoped_session(
        DBSession)  # g is an "application global" https://flask.palletsprojects.com/en/1.1.x/api/#application-globals


@app.teardown_appcontext
def shutdown_session(response_or_exc):
    g.session.commit()
    g.session.remove()


"""
-------- Helper methods (feel free to add your own!) -------
"""


def log_message(d):
    # Takes input dictionary d and writes it to the Log table
    add_order = Log(message=json.dumps(d.get('payload')))
    g.session.add(add_order)
    g.session.commit()
    g.session.close()


"""
---------------- Endpoints ----------------
"""


@app.route('/trade', methods=['POST'])
def trade():
    if request.method == "POST":
        content = request.get_json(silent=True)
        print(f"content = {json.dumps(content)}")
        columns = ["sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount", "sell_amount", "platform"]
        fields = ["sig", "payload"]
        # error = False
        for field in fields:
            if not field in content.keys():
                print(f"{field} not received by Trade")
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        error = False
        for column in columns:
            if not column in content['payload'].keys():
                print(f"{column} not received by Trade")
                error = True
        if error:
            print(json.dumps(content))
            log_message(content)
            return jsonify(False)

        # Your code here
        # Note that you can access the database session using g.session
        # check whether “sig” is a valid signature of json.dumps(payload),
        # using the signature algorithm specified by the platform field.
        # Be sure to verify the payload using the sender_pk.

        sig = content['sig']
        message = content['payload']
        sender_pk = content['payload']['sender_pk']
        receiver_pk = content['payload']['receiver_pk']
        buy_currency = content['payload']['buy_currency']
        sell_currency = content['payload']['sell_currency']
        buy_amount = content['payload']['buy_amount']
        sell_amount = content['payload']['sell_amount']
        platform = content['payload']['platform']

        if platform == 'Ethereum':
            eth_encoded_msg = eth_account.messages.encode_defunct(text=message)

            # If the signature verifies
            if eth_account.Account.recover_message(eth_encoded_msg, signature=sig) == sender_pk:
                # store the signature, as well as all of the fields under the ‘payload’
                # in the “Order” table EXCEPT for 'platform’.
                add_order = Order(signature=sig, sender_pk=sender_pk, receiver_pk=receiver_pk,
                                  buy_currency=buy_currency, sell_currency=sell_currency,
                                  buy_amount=buy_amount, sell_amount=sell_amount)
                g.session.add(add_order)
                g.session.commit()
                return jsonify(True)

            # If the signature does not verify
            else:
                # do not insert the order into the “Order” table.
                # Instead, insert a record into the “Log” table,
                # with the message field set to be json.dumps(payload).
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        elif platform == 'Algorand':
            if algosdk.util.verify_bytes(message.encode('utf-8'), sig, sender_pk):
                add_order = Order(signature=sig, sender_pk=sender_pk, receiver_pk=receiver_pk,
                                  buy_currency=buy_currency, sell_currency=sell_currency,
                                  buy_amount=buy_amount, sell_amount=sell_amount)
                g.session.add(add_order)
                g.session.commit()
                return jsonify(True)

            else:
                print(json.dumps(content))
                log_message(content)
                return jsonify(False)

        # The platform must be either “Algorand” or "Ethereum".
        else:
            print(json.dumps(content))
            log_message(content)
            return jsonify(False)


@app.route('/order_book')
def order_book():
    # Your code here
    # Note that you can access the database session using g.session
    # Return a list of all orders in the database.
    # The response should contain a single key “data” that refers to a list of orders formatted as JSON.
    # Each order should be a dict with (at least) the following fields
    # ("sender_pk", "receiver_pk", "buy_currency", "sell_currency", "buy_amount",
    # "sell_amount", “signature”).
    result = {'data': []}

    orders = g.session.query(Order)
    for order in orders:
        one_order = {'sender_pk': order.sender_pk, 'receiver_pk': order.receiver_pk,
                     'buy_currency': order.buy_currency, 'sell_currency': order.sell_currency,
                     'buy_amount': order.buy_amount, 'sell_amount': order.sell_amount,
                     'signature': order.signature}
        result['data'].append(one_order)

    g.session.commit()
    return jsonify(result)


if __name__ == '__main__':
    app.run(port='5002')
