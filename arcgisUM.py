# Wrapper around calls to ArcGIS.  Helps with testing and future changes.
# This should also be usable in a Jupyter Notebook.  Some methods aren't used
# by kartograafr as of this writing but are necessary for testing and might 
# be useful for maintenance.

# TODO: Should CaputerStdoutLines be used in more places?
# This sometimes uses a CaptureStdoutLines context manager when calling the 
# SDK since the SDK may write messages to stdout rather than returning them.

import datetime
import logging
import re

import sys
from io import StringIO

logger = logging.getLogger(__name__)

from arcgis.gis import GIS

import dateutil.tz

import util

##### Improved code tracebacks for exceptions
import traceback

def handleError(self, record):  # @UnusedVariable
    traceback.print_stack()
    
logging.Handler.handleError = handleError
#####

TIMEZONE_UTC = dateutil.tz.tzutc()
RUN_START_TIME = datetime.datetime.now(tz=TIMEZONE_UTC)
RUN_START_TIME_FORMATTED = RUN_START_TIME.strftime('%Y%m%d%H%M%S')

# Hold parsed options
options = None

# TODO: required in this module?
courseLogHandlers = dict()
courseLoggers = dict()


# Special exception, mostly used when have captured an error from the SDK.
class ArcgisUMException(RuntimeWarning):
    '''Unexpected result from call to ArcGIS.'''

def getArcGISConnection(securityinfo):
    """
    Get a connection object for ArcGIS based on configuration options
    
    :return: Connection object for the ArcGIS service
    :rtype: arcresthelper.orgtools.orgtools
    :raises: RuntimeError if ArcGIS connection is not valid
    :raises: arcresthelper.common.ArcRestHelperError for ArcREST-related errors
    """

    if not isinstance(securityinfo, dict):
        raise TypeError('Argument securityinfo type should be dict')

    try:
        arcGIS = GIS(securityinfo['org_url'],
                     securityinfo['username'],
                     securityinfo['password']);
    except RuntimeError as exp:
        logger.error("RuntimeError: getArcGISConnection: {}".format(exp))
        raise RuntimeError(str('ArcGIS connection invalid: {}'.format(exp)))
    
    return arcGIS

def getArcGISGroupByTitle(arcGISAdmin, title):
    """
    Given a possible title of a group, search for it in ArcGIS
    and return a Group object if found or None otherwise.

    :param arcGISAdmin: ArcGIS Administration REST service connection object
    :type arcGISAdmin: arcrest.manageorg.administration.Administration
    :param title: Group title to be found
    :type title: str
    :return: ArcGIS Group object or None
    :rtype: Group or None
    """
    searchString = "title:"+title
    logger.debug("group search string: original: {}".format(searchString))

    # quote characters that are special in the group search.
    searchString = searchString.translate(str.maketrans({"?":  r"\?","*":  r"\*"}))

    logger.debug("group search string: escaped: {}".format(searchString))
    
    try:
        gis_groups = arcGISAdmin.groups.search(searchString)
    except RuntimeError as exp:
        logger.error("arcGIS error finding group: {} exception: {}".format(searchString,exp))
        return None
    
    if len(gis_groups) > 0:
        return gis_groups.pop()

    return None


def addUsersToGroup(instructorLog, group, courseUsers):
    """Add new users to the ArcGIS group.  """
    groupNameAndID = util.formatNameAndID(group)
    
    if len(courseUsers) == 0:
        logger.info('No new users to add to ArcGIS Group {}'.format(groupNameAndID))
        return instructorLog

    logger.info('Adding Users to ArcGIS Group: {}: users: {}'.format(groupNameAndID, courseUsers))

    # TODO: simplify
    arcGISFormatUsers = courseUsers
    
    results = group.add_users(arcGISFormatUsers)
    logger.debug("adding: results: {}".format(results))

    usersNotAdded = results.get('notAdded')
    """:type usersNotAdded: list"""
    usersCount = len(arcGISFormatUsers)
    usersCount -= len(usersNotAdded) if usersNotAdded else 0
    logger.debug("aCUTG: usersCount: {}".format(usersCount))
    
    instructorLog += 'Number of users added to group: [{}]\n\n'.format(usersCount)

    if usersNotAdded:
        logger.warning('Warning: Some or all users not added to ArcGIS group {}: {}'.format(groupNameAndID, usersNotAdded))
        instructorLog += 'Users not in group (these users need ArcGIS accounts created for them):\n' + '\n'.join(['* ' + userNotAdded for userNotAdded in usersNotAdded]) + '\n\n' + 'ArcGIS group ID number:\n{}\n\n'.format(group.id)
    instructorLog += '- - - - - - - - - - - - - - - - - - - - - - - - - - - - - -\n'

    logger.info("addUsersToGroup: instructorLog: [{}]".format(instructorLog))
    
    return instructorLog


def createFolderForUser(arcGIS,folder_name,explicit_owner):
    '''create a named folder for a named user'''

    logger.info("For user [{}] create folder [{}] .".format(explicit_owner,folder_name))
    
    with CaptureStdoutLines() as output:
        try:
            folder_return = arcGIS.content.create_folder(folder=folder_name,owner=explicit_owner)
        except  RuntimeError as rte:
            logger.error("Exception: {} creating folder {} for user {}".format(rte,folder_name,explicit_owner))
            return None
    
    # If there is an error log it and return None
    if output:
        logger.info("create folder for user: error output: {}".format(output))
        raise ArcgisUMException("{}: user: [{}] folder_name: [{}]".format(output,explicit_owner,folder_name))

    return folder_return


def getCurrentArcGISMembers(group):
    groupAllMembers = {}

    try:
        groupAllMembers = group.get_members()
    except RuntimeError as exception:
        logger.error('Exception while getting users for ArcGIS group "{}": {}'.format(group, exception))
            
    groupUsers = groupAllMembers.get('users')
    """:type groupUsers: list"""
    return groupUsers


def removeListOfUsersFromArcGISGroup(group, groupNameAndID, groupUsers):
    """Remove only listed users from ArcGIS group."""

    if len(groupUsers) == 0:
        logger.info('No obsolete users to remove from ArcGIS Group {}'.format(groupNameAndID))
        return None

    logger.info('ArcGIS Users to be removed from ArcGIS Group [{}] [{}]'.format(groupNameAndID, ','.join(groupUsers)))
    results = None
    
    try:
            results = group.removeUsersFromGroup(','.join(groupUsers))
    except RuntimeError as exception:
            logger.error('Exception while removing users from ArcGIS group "{}": {}'.format(groupNameAndID, exception))
            return None
            
    usersNotRemoved = results.get('notRemoved')
    """:type usersNotRemoved: list"""
    if usersNotRemoved:
        logger.warning('Warning: Some or all users not removed from ArcGIS group {}: {}'.format(groupNameAndID, usersNotRemoved))
        
    return results


def removeSomeExistingGroupMembers(groupTitle, group,instructorLog,groupUsers):
    """Get list of ArgGIS users to remove from group and call method to remove them."""
    results = ''
    groupNameAndID = util.formatNameAndID(group)
    logger.info('Found ArcGIS group: {}'.format(groupNameAndID))
    instructorLog += 'Updating ArcGIS group: "{}"\n'.format(groupTitle)
    
    if not groupUsers:
        logger.info('Existing ArcGIS group {} does not have users to remove.'.format(groupNameAndID))
    else:
        results = removeListOfUsersFromArcGISGroup(group, groupNameAndID, groupUsers)
        
    return instructorLog, results


def createNewArcGISGroup(arcGIS, groupTags, groupTitle,instructorLog):
    """Create a new ArgGIS group.  Return group and any creation messages."""
    group=None
    
    logger.info('Creating ArcGIS group: "{}"'.format(groupTitle))
    instructorLog += 'Creating ArcGIS group: "{}"\n'.format(groupTitle)
    try:
        group = arcGIS.groups.create(groupTitle,groupTags)
    except RuntimeError as exception:
        logger.exception('Exception while creating ArcGIS group "{}": {}'.format(groupTitle, exception))
    
    return group, instructorLog


# TODO: Have consistent methods for all the formatting of the user lists. 
def formatUsersNamesForArcGIS(suffix,userList):
    """Convert list of user names to the format used in ArcGIS."""
    userList = [user + '_' + suffix for user in userList]
    return userList


def getFoldersForUser(gis,user_name):
    '''get list of folders owned by this user'''
    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("gFFU: {}".format(folders))
    
    return folders


##### Predicates for filtering.
def doesFolderMatchTitle(folder,search_string):
    '''Test if folder is empty and regex matches the title.'''
    logger.debug("dFMT: folder: [{}] folder_match: [{}]".format(folder,search_string))
    
    return re.search(search_string,folder.get('title'))
    

def doesFolderMatchTitleAndIsEmpty(user,folder,search_string):
    '''Test if folder is empty and string matches the title as a prefix.'''
    logger.debug("dFMTAIE: user: [{}] folder: [{}] search_string: [{}]".format(user,folder,search_string))
                          
    if not doesFolderMatchTitle(folder,search_string):
        logger.debug("dFMFAIE: folder title does NOT match.") 
        return False
    
    logger.info("dFMTAIE: folder title matches.")   
    
    # If there are some items return false
    folderItems = user.items(folder=folder)
    logger.debug("dFMTAIE: folderItems: [{}]".format(folderItems))
    
    return len(folderItems) == 0


# TODO: this could be much more pythonic
def deleteMatchingEmptyFoldersForUser(gis, user_name, search_string):
    '''Check user's folder's titles against regular expression (as string) and delete if match and are empty.'''
    logger.debug("dMEFFUF: gis: {} user_name: [{}] match_string: [{}]".format(gis, user_name, search_string))
    
    matching_folders = listMatchingEmptyFoldersForUser(gis, user_name, search_string)

    logger.info("matching empty folders to delete for user: [{}] folders: [{}]".format(user_name, matching_folders))

    for f in matching_folders:
        logger.info("deleting folder: [{}] for user: [{}]".format(f.get('title'), user_name))
        gis.content.delete_folder(f.get('title'), owner=user_name)
               
        
# It would be straightforward to extend this to pass in a predicate for matching.
def listMatchingNonEmptyFoldersForUser(gis, user_name, search_string):
    '''Check user's folder's titles against regular expression (as string) and list if match and have contents.'''
    logger.debug("lMNEFFU: gis: {} user_name: [{}] match_string: [{}]".format(gis, user_name, search_string))

    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("lMNEFFU: all folders {}".format(folders))

    matching_folders = [f for f in folders if not doesFolderMatchTitleAndIsEmpty(user, f, search_string)]

    logger.info("list matching non-empty folders for user: {} folders: {}".format(user_name, matching_folders))

    return matching_folders


def listMatchingEmptyFoldersForUser(gis, user_name, search_string):
    '''Check user's folder's titles against regular expression (as string) and list if match and are empty.'''
    logger.debug("lMEFFU: gis: {} user_name: [{}] match_string: [{}]".format(gis, user_name, search_string))

    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("lMEFFU: all folders {}".format(folders))

    matching_folders = [f for f in folders if doesFolderMatchTitleAndIsEmpty(user, f, search_string)]

    logger.info("list matching folders for user: {} folders: {}".format(user_name, matching_folders))

    return matching_folders


def listMatchingFoldersForUser(gis,user_name,match_string):
    '''Find user's matching folders. Match string must be string version of regular expression.'''
    logger.debug("lMFFU: gis: {} user_name: [{}] match_string: [{}]".format(gis,user_name,match_string))
    
    # assemble the values that won't change.
    folder_match = re.compile(match_string)
    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("lMFFU: all folders {}".format(folders))
 
    matching_folders = [f for f in folders if doesFolderMatchTitle(f,folder_match)]
    
    logger.info("list matching folders for user: {} folders: {}".format(user_name,matching_folders))
    return matching_folders
    
    
def getItemsInFolderForUser(gis,folder_name,user_name):
    logger.debug("gIIFFU: gis: {} user_name: [{}] folder_name: [{}]".format(gis,user_name,folder_name))
    user = gis.users.get(user_name)
    logger.debug('gIIFFU: user: {}'.format(user))
    
    # TODO: wrap for errors
    folderItems = user.items(folder=folder_name)
    return folderItems
    
# TODO: deal with error return
def cloneFolderFromTo(gis,source_folder_name,source_user_name,sink_folder_name,sink_user_name):
    '''clone the items in the source folder to the sink folder and make sink user the owner.'''
    logger.debug("cFFT: gis: {} source_folder_name: [{}] source_user_name: [{}] sink_folder_name: [{}] sink_user_name: [{}]"
          .format(gis,source_folder_name,source_user_name,sink_folder_name,sink_user_name))
    
    try:
        new_folder = createFolderForUser(gis,sink_folder_name,sink_user_name)
    except ArcgisUMException as excp:
        logger.error("Error creating folder: {}".format(str(excp)))
        if re.match(re.escape('[\'Folder already exists.\']'),str(excp)):
            logger.debug("cFFT: caught error creating folder")
        return None
        
    logger.debug("cFFT: new_folder: {}".format(new_folder))

    source_items = getItemsInFolderForUser(gis,source_folder_name,source_user_name)
    logger.debug("cFFT: source_items: {}".format(source_items))
    
    logger.debug("cFFT: new_folder: [{}]".format(new_folder))
    cloned = gis.content.clone_items(source_items,folder=new_folder['title'],copy_data=True)
    logger.debug("cFFT: cloned: [{}]".format(cloned))
    
    return new_folder

class CaptureStdoutLines(list):
    """
    A context manager for capturing the lines sent to stdout as elements
    of a list.  Useful for capturing important output printed by
    poorly-designed API methods.

    Example::
        with CaptureStdoutLines() as output:
            print('Norwegian blue parrot')
        assert output == ['Norwegian blue parrot']

        with CaptureStdoutLines(output) as output:
            print('Venezuelan beaver cheese')
        assert output == ['Norwegian blue parrot', 'Venezuelan beaver cheese']
    """

    def __enter__(self):
        self._originalStdout = sys.stdout
        sys.stdout = self._stdoutStream = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stdoutStream.getvalue().splitlines())
        sys.stdout = self._originalStdout


### end 
