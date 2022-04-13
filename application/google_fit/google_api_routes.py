import logging
import traceback
from urllib import response
from flask import request, session, redirect, url_for
from flask import Blueprint
from flask.json import jsonify
from flask.wrappers import Response
from authlib.integrations.flask_client import OAuth
import os
import pprint
from datetime import datetime

import google.oauth2.credentials
import google_auth_oauthlib.flow
from application.okta.helpers import  is_authorized
from application.utils.user_credentials_manager import add_access_token, verify_user_connection
from application.config import GOOGLE_FIT_CONFIG, PROJ_LOC, HOST_CONFIG

oauth = OAuth()

from .google_fit_import_module import initiate_google_fit_data_import

LOG = logging.getLogger(__name__)

APP_SCOPE = [
        "https://www.googleapis.com/auth/fitness.sleep.read",
        "https://www.googleapis.com/auth/fitness.heart_rate.read",
        "https://www.googleapis.com/auth/fitness.reproductive_health.read",
        "https://www.googleapis.com/auth/fitness.body_temperature.read",
        "https://www.googleapis.com/auth/fitness.oxygen_saturation.read",
        "https://www.googleapis.com/auth/fitness.blood_glucose.read",
        "https://www.googleapis.com/auth/fitness.blood_pressure.read",
        "https://www.googleapis.com/auth/fitness.nutrition.read",
        "https://www.googleapis.com/auth/fitness.body.read",
        "https://www.googleapis.com/auth/fitness.location.read",
        "https://www.googleapis.com/auth/fitness.activity.read"
    ]

# from application import app
from application.models.base import db
google_API_routes = Blueprint("google_fit_routes", __name__)

    # LOG.info("Google routes accessed: OK")
    # return "Google routes"

@google_API_routes.route("/google-fit/connection", methods=['GET', 'POST'])
def google_fit_connection():
    user_id = request.args.get("user_id", None)
    personicle_access_token = request.headers.get("Authorization")
    if user_id is None:
        LOG.error("Unauthorised access: Denied")
        return Response("User not logged in", 401)
    
    for k in list(session.keys()):
        session.pop(k)

    session['user_id'] = user_id
    session['personicle_token'] = personicle_access_token
    LOG.info("Google fit authorization for user: {}".format(session['user_id']))
    if verify_user_connection(personicle_user_id=session['user_id'], connection_name='google-fit'):
        LOG.info("User {} has active access token for google fit".format(session['user_id']))
        try:
            resp = initiate_google_fit_data_import(user_id, personicle_access_token)
            success= True
        except Exception as e:
            LOG.error(traceback.format_exc(e))
            resp = {'error': str(e)}
            success = False
        result = jsonify(resp)
        return result
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        os.path.join(PROJ_LOC, GOOGLE_FIT_CONFIG['SECRET_JSON']),
        scopes=APP_SCOPE)

    # Indicate where the API server will redirect the user after the user completes
    # the authorization flow. The redirect URI is required. The value must exactly
    # match one of the authorized redirect URIs for the OAuth 2.0 client, which you
    # configured in the API Console. If this value doesn't match an authorized URI,
    # you will get a 'redirect_uri_mismatch' error.
    flow.redirect_uri = HOST_CONFIG['HOST_ADDRESS'] + GOOGLE_FIT_CONFIG['REDIRECT_URL']
    # Generate URL for request to Google's OAuth 2.0 server.
    # Use kwargs to set optional request parameters.
    authorization_url, state = flow.authorization_url(
        access_type='offline', include_granted_scopes='true', enable_reauth_refresh='true')

    # print(authorization_url)
    # Enable offline access so that you can refresh an access token without
    # re-prompting the user for permission. Recommended for web server apps.
        
    session['state'] = state
    # Enable incremental authorization. Recommended as a best practice.

    return redirect(authorization_url)

@google_API_routes.route("/google-fit/oauth/access_token/", methods=['GET'])
def get_access_token():
    user_id = session.get("user_id", None)
    personicle_token = session.get("personicle_token", None)
    if user_id is None:
        return Response("User not logged in", 401)
    error_resp = request.args.get("error", None)
    if error_resp:
        return Response(error_resp, 401)
    auth_code = request.args.get("code", None)
    
    auth_response = request.url
    if auth_code is None:
        return Response("Error while accessing authorizarion code", 401)

    # print(auth_code, state)
    state = session['state']
    flow = google_auth_oauthlib.flow.Flow.from_client_secrets_file(
        os.path.join(PROJ_LOC, GOOGLE_FIT_CONFIG['SECRET_JSON']),
        scopes=APP_SCOPE,
        state=state)
    flow.redirect_uri = HOST_CONFIG['HOST_ADDRESS'] + GOOGLE_FIT_CONFIG['REDIRECT_URL']

    flow.fetch_token(authorization_response=auth_response)

    # Store the credentials in the session.
    # ACTION ITEM for developers:
    #     Store user's access and refresh tokens in your data store if
    #     incorporating this code into your real app.
    credentials = flow.credentials
    session['credentials'] = {
        'token': credentials.token,
        'refresh_token': credentials.refresh_token,
        'token_uri': credentials.token_uri,
        'client_id': credentials.client_id,
        'client_secret': credentials.client_secret,
        'scopes': credentials.scopes,
        'expires_at': credentials.expiry
    }

    expires_in = (credentials.expiry - datetime.utcnow()).total_seconds()

    pprint.pprint(credentials.__dict__)
    # pprint.pprint(session['credentials'])
    action ,user_record = add_access_token(user_id, service_name='google-fit', access_token=credentials.token, expires_in=expires_in,
                            created_at=datetime.utcnow(), external_user_id=None, refresh_token=credentials.refresh_token)

    try:
        if action == 'add':
            db.session.add(user_record)
        else:
            pass
        result = jsonify(success=True)
        db.session.commit()
    except Exception as e:
        print(e)
        # raise e
        db.session.rollback()
        result = jsonify(success=False)
    # return resp
    # data_import_thread = threading.Thread(target=initiate_google_fit_data_import, args=(user_id,))
    # data_import_thread.start()
    try:
        resp = initiate_google_fit_data_import(user_id, personicle_token)
        result = jsonify(resp)
        return result
    except Exception as e:
        LOG.error(traceback.format_exc())
        return jsonify(success=False, message="Error while initiating download request")
