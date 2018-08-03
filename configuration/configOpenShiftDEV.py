## Configuration for OpenShift dev version.  Production will need a separate version.

import os
import logging

class Application(object):
    class Email(object):
        DEBUG_LEVEL = False
#        SMTP_SERVER = 'mail-relay.itd.umich.edu'
        SMTP_SERVER = 'docker.for.mac.localhost:1025'
        SENDER_ADDRESS = '"ArcGIS-Canvas Service Dev" <kartograafr-service-dev@umich.edu>'
        RECIPIENT_AT_DOMAIN = '@umich.edu'
        SUBJECT = 'ArcGIS-Canvas logs for course ID {courseID} (Dev)'

    # directory path for logging may depend on the platform. This setup is for Docker.
    class Logging(object):
        MAIN_LOGGER_NAME = 'kartograafr'
        DIRECTORY = '/var/log/kartograafr'
        COURSE_DIRECTORY = os.path.join(DIRECTORY, 'courses')
        MAIN_LOG_BASENAME = 'main'
        LOG_FILENAME_EXTENSION = '.log'
        DEFAULT_LOG_LEVEL = logging.INFO
    
    # Other settings.
    class General(object):
        # Prefixes to be used in assignment folders.
        ASGN_FOLDER_PREFIX = 'ASGN: '
        # Submission is copy provided to the instructor.
        SUBMISSION_FOLDER_PREFIX = 'GRADEME: '
        # True means to not create a clone folder if there are no items to be cloned.
        SKIP_EMPTY_CLONE=True
        # False means not to copy anything into a folder if there already is something in it.
        ALLOW_MULTIPLE_CLONES=False
        # Remove any item with non-ascii name.  As of 2018/04 clone will fail when constructing url.
        #FILTER_NON_ASCII_TITLE=True
        FILTER_NON_ASCII_TITLE = False
        
class Canvas(object):
    API_BASE_URL = 'https://umich.instructure.com/api/v1/'

    API_AUTHZ_TOKEN = 'NEVEReverWILLyouKNOWmyNAME'

    ACCOUNT_ID = 306  # Test Account
    #TARGET_OUTCOME_ID = 2501  # ArcGIS Mapping Skills
    #TARGET_OUTCOME_ID = 4353  # ArcGIS Mapping Skills
    TARGET_OUTCOME_ID = 4941 # Kart test outcome
    CONFIG_COURSE_ID = 138596
    CONFIG_COURSE_PAGE_NAME = 'course-ids'
    COURSE_ID_SET = set(( # Used iff IDs are not found in the configuration course page defined above
        85489,  # Practice Course for Lance Sloan (LANCE PRACTICE)
        114488,  # First ArcGIS Course (ARCGIS-1)
        135885,  # Another ArcGIS Course (ARCGIS-2)
    ))

class ArcGIS(object):
    ORG_NAME = 'devumich' # For server URL (see below) and appended to ArcGIS usernames (i.e., "user_org")
    SECURITYINFO = {
        'security_type': 'Portal',  # Default: "Portal". "Required option" by bug in some ArcREST versions.
        'org_url': 'https://{}.maps.arcgis.com'.format(ORG_NAME),
        'username': '',
        'password': '',
    }

