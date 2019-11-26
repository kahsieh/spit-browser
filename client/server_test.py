from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route("/")
def hello():
    return 'hello world!'

@app.route("/allocate", methods=['POST'])
def allocate():
    req = request.get_json(force=True)
    return jsonify({'ip': '127.0.0.1', 'port': 8000, 'num_workers': req['num_workers']})

if __name__ == "__main__":
    app.run()
