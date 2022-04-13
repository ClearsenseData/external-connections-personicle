from application import create_app
import os

app = create_app()
if __name__ == "__main__":
    os.environ['OAUTHLIB_INSECURE_TRANSPORT'] = '1'
    app.run("localhost", port=5000, ssl_context='adhoc', debug=True)
    