from flask import Flask, request, jsonify
app = Flask(__name__)

@app.route("/")
def hello():
    return 'hello world!'

@app.route("/allocate", methods=['POST'])
def allocate():
    #req = request.get_json(force=True)
    info = request.get_json()
    app.logger.warning(info)
    return jsonify({'ip': '127.0.0.1', 'port': 8000, 'info': info})

if __name__ == "__main__":
    app.run()
