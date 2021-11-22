from models import ExternalConnections

def add_access_token(personicle_user_id, **kwargs):
    """
    Add an access token for importing user data from external server (e.g. Fitbit)

    Parameters:
    required:
    personicle_user_id: User id for the user in personicle servers
    service_name: Name of the external service
    access_token: Access token value
    expires_in: Time before expiration for the token
    created_at: Time of token creation

    Optional arguments:
    external_user_id: user id for the external API service (e.g. fitbit user id)
    refresh_token: Refresh token value
    """
    required_arguments = ["service_name", "access_token", "expires_in", "created_at"]
    optional_arguments = ["external_user_id", "refresh_token"]

    for i in required_arguments:
        assert i in kwargs, "Missing required parameter: {}".format(i)

    new_record = ExternalConnections(userId=personicle_user_id, service=kwargs['service_name'], access_token=kwargs['access_token'], expires_in=kwargs['expires_in'],
                                        created_at=kwargs['created_at'], external_user_id=kwargs.get('external_user_id', None), refresh_token=kwargs.get("refresh_token", None))
    # check if user already exists
    if len(ExternalConnections.query.filter_by(userId=personicle_user_id, service=kwargs['service_name']).all()) > 0:
    # Update the user record
        return "update", new_record
    else:
    # Add the user record
        return "add", new_record
    # return the record and add it in the calling method