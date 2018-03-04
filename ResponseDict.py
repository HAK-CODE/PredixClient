# ----------------------------------------------------------------------------------
# --------------------------------- AUTHENTICATION ---------------------------------
# ----------------------------------------------------------------------------------
"""
Invalid Authentication
"""
Auth_Login_Invalid = {
    "type": "Authentication",
    "response": {
        "access": "Denied",
        "message": 'Invalid user email or password.'
    }
}


"""
Authentication token expired
"""
Auth_Token_Expired = {
    "type": "Authentication",
    "response": {
        "access": "Denied",
        "message": 'Token expired.'
    }
}


# ----------------------------------------------------------------------------------
# ------------------------------------ PREDIX ---------------------------------------
# ----------------------------------------------------------------------------------
Predix_service_down = {
    "type": "Predix request",
    "response": {
        "access": "Failed",
        "message": "Predix service returned with error."
    }
}