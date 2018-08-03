# Wrapper around calls to ArcGIS.  Helps with testing and future changes.
# This should also be usable in a Jupyter Notebook.  Some methods aren't used
# by kartograafr as of this writing but are necessary for testing and might 
# be useful for maintenance.

# TODO: Should CaptureStdoutLines be used in more places?
# This sometimes uses a CaptureStdoutLines context manager when calling the
# SDK since the SDK may write messages to stdout rather than returning them.

import datetime
import logging
import re
import config

import sys
from io import StringIO

logger = logging.getLogger(__name__)

from arcgis.gis import GIS

import dateutil.tz
import time

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
    """Unexpected result from call to ArcGIS."""

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
                     securityinfo['password'])
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
        logger.debug('No new users to add to ArcGIS Group {}'.format(groupNameAndID))
        return instructorLog

    logger.debug('Adding Users to ArcGIS Group: {}: users: {}'.format(groupNameAndID, courseUsers))

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
    """create a named folder for a named user"""

    logger.debug("cFFU: For user [{}] create folder [{}] .".format(explicit_owner,folder_name))
    
    with CaptureStdoutLines() as output:
        try:
            folder_return = arcGIS.content.create_folder(folder=folder_name,owner=explicit_owner)
        except  RuntimeError as rte:
            logger.debug("Exception: {} creating folder [{}] for user [{}]".format(rte,folder_name,explicit_owner))
            return None
    
    # If there is an error log it and return None
    if output:
        logger.debug("error in create folder for user: [{}] error output: {}".format(explicit_owner,output))
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
    logger.debug('Found ArcGIS group: {}'.format(groupNameAndID))
    instructorLog += 'Updating ArcGIS group: "{}"\n'.format(groupTitle)
    
    if not groupUsers:
        logger.debug('Existing ArcGIS group {} does not have users to remove.'.format(groupNameAndID))
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


def formatUsersNamesForArcGIS(userList):
    """Convert list of user names to the format used in ArcGIS."""
    # check for trivial case

    logger.debug("fUNFAC: userList: {}".format(userList))
    #logger.warning("fUNFAC: traceback: {}".format(traceback.print_stack()))

    if userList == [None] or userList == [None] or len(userList) == 0:
        return []

    suffix=config.ArcGIS.ORG_NAME
    userList = [user + '_' + suffix for user in userList]
    return userList

def getFoldersForUser(gis,user_name):
    """get list of folders owned by this user"""
    logger.debug("gFFU: get folders for {}".format(user_name))
    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("gFFU: back from get folders: {}".format(folders))
    
    return folders


##### Predicates for filtering.
def doesFolderMatchTitle(folder,search_string):
    """Test if folder is empty and regex matches the title."""
    logger.debug("dFMT: folder: [{}] folder_match: [{}]".format(folder.get('title'),search_string))
    
    return re.search(search_string,folder.get('title'))

def isFolderEmpty(folder, user):
    # If there are no items return false
    folderItems = user.items(folder=folder)
    logger.debug("iFE: folderItems: [{}]".format(folderItems))
    return len(folderItems) == 0


def doesFolderMatchTitleAndIsEmpty(user,folder,search_string):
    """Test if folder is empty and string matches the title as a prefix."""
    logger.debug("dFMTAIE: user: [{}] folder: [{}] search_string: [{}]".format(user,folder.get('title'),search_string))
                          
    if not doesFolderMatchTitle(folder,search_string):
        logger.debug("dFMTAIE: folder title does NOT match.")
        return False

    logger.debug("dFMTAIE: folder title matches: [{}].".format(folder))
    logger.debug("dFMTAIE: folder title matches: [{}].".format(folder.get('title')))

    return isFolderEmpty(folder, user)


def doesFolderMatchTitleAndIsNotEmpty(user, folder, search_string):
    """Test if folder is empty and string matches the title as a prefix."""
    logger.debug(
        "dFMTAINE: user: [{}] folder: [{}] search_string: [{}]".format(user, folder.get('title'), search_string))

    if not doesFolderMatchTitle(folder, search_string):
        logger.debug("dFMTAIE: folder title does NOT match.")
        return False

    logger.debug("dFMTAINE: folder title matches: [{}].".format(folder))
    logger.debug("dFMTAINE: folder title matches: [{}].".format(folder.get('title')))

    return not isFolderEmpty(folder, user)


# TODO: this could be much more pythonic
def deleteMatchingEmptyFoldersForUser(gis, user_name, search_string):
    """Check user's folder's titles against regular expression (as string) and delete if match and are empty."""
    logger.debug("dMEFFUF: gis: {} user_name: [{}] match_string: [{}]".format(gis, user_name, search_string))
    
    matching_folders = listMatchingEmptyFoldersForUser(gis, user_name, search_string)

    logger.debug("dMEFFUF: matching empty folders to delete for user: [{}] folders: [{}]".format(user_name, matching_folders))

    for f in matching_folders:
        logger.debug("dMEFFUF: deleting folder: [{}] for user: [{}]".format(f.get('title'), user_name))
        gis.content.delete_folder(f.get('title'), owner=user_name)
               
        
# It would be straightforward to extend this to pass in a predicate for matching.
def listMatchingNonEmptyFoldersForUser(gis, user_name, search_string):
    """Check user's folder's titles against regular expression (as string) and list if match and have contents."""
    logger.debug("lMNEFFU: gis: {} user_name: [{}] match_string: [{}]".format(gis, user_name, search_string))

    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("lMNEFFU: all folders {}".format(folders))

    matching_folders = [f for f in folders if doesFolderMatchTitleAndIsNotEmpty(user, f, search_string)]

    logger.debug("lMNEFFU: list matching non-empty folders for user: {} folders: {}".format(user_name, matching_folders))

    return matching_folders


def listMatchingEmptyFoldersForUser(gis, user_name, search_string):
    """Check user's folder's titles against regular expression (as string) and list if match and are empty."""
    logger.debug("lMEFFU: gis: {} user_name: [{}] match_string: [{}]".format(gis, user_name, search_string))

    user = gis.users.get(user_name)

    folders = user.folders
    logger.debug("lMEFFU: got folders {}".format(folders))

    matching_folders = [f for f in folders if doesFolderMatchTitleAndIsEmpty(user, f, search_string)]

    logger.debug("list matching folders for user: {} folders: {}".format(user_name, matching_folders))

    return matching_folders


def listMatchingFoldersForUser(gis,user_name,match_string):
    """Find user's matching folders. Match string must be string version of regular expression."""
    logger.debug("lMFFU: gis: {} user_name: [{}] match_string: [{}]".format(gis,user_name,match_string))
    
    # assemble the values that won't change.
    folder_match = re.compile(match_string)
    user = gis.users.get(user_name)
    folders = user.folders
    logger.debug("lMFFU: all folders {}".format(folders))
 
    matching_folders = [f for f in folders if doesFolderMatchTitle(f,folder_match)]
    
    logger.debug("list matching folders for user: {} folders: {}".format(user_name,matching_folders))
    return matching_folders
    
    
def getItemsInFolderForUser(gis,folder_name,user_name):
    logger.debug("gIIFFU: user_name: [{}] folder_name: [{}]".format(user_name,folder_name))
    user = gis.users.get(user_name)

    # If there is a problem return an empty list so other user processing can continue.
    folderItems = []
    try:
        # TODO: suppress "arcgis._impl.portalpy - INFO - getting user folders and items"  Not clear how to do that.
        folderItems = user.items(folder=folder_name)

    except ValueError as excp:
        logger.debug("gIIFFU: ValueError exception finding folder items.  Exception: {} user: {}, folder: {}"
                     .format(excp,user_name,folder_name))
    except RuntimeError as excp:
        logger.warning("gIIFFU: Unexpected exception finding folder items.  Exception: {} user: {}, folder: {}"
                     .format(excp,user_name,folder_name))

    return folderItems

# TODO: why can't this be in util?
# TODO: remove ascii check.
def isAscii(s):
    """True if string can be encoded as ascii, false if it can not be."""
    try:
        s.encode('ascii')
        return True
    except UnicodeEncodeError:
        return False


def filterItemsForClone(items):
    """Edit the list of items to clone."""

    filtered = items

    # remove non-ascii
    logger.debug("fIFC: check for non-ascii titles")
    filtered = removeItemsIfTitleIsnotAscii(filtered)

    logger.debug("rIFC: kept: {}".format(filtered))
    return filtered


def removeItemsIfTitleIsnotAscii(items):
    """Filter out items if the title isn't in ascii.  Go through list twice to get good log messages."""

    # See if should check for ascii title.
    if not config.Application.General.FILTER_NON_ASCII_TITLE:
        return items

    # explicit loop to clearly separate logging from removal.
    for item in items:
        if not isAscii(item.get('title')):
            logger.warning("item skipped as has non-ascii name: {}".format(item))

    return [item for item in items if isAscii(item.get('title'))]


def cloneFolderFromTo(gis, source_folder_name, source_user_name, sink_folder_name, sink_user_name,
                      skip_empty_clone=config.Application.General.SKIP_EMPTY_CLONE,
                      allow_multiple_clones=config.Application.General.ALLOW_MULTIPLE_CLONES):
    """clone the items in the source folder to local folder then reassign to the sink folder and make sink user the owner."""
    # TODO: deal with error return
    # plan
    ## make the folder for destination user
    ## clone folder locally as service account
    ## reassign items to sink user folder
    ## delete the temporary service account clone folder

    global source_items

    logger.debug(
        "cFFT:  source_folder_name: [{}] source_user_name: [{}] sink_folder_name: [{}] sink_user_name: [{}]"
        .format(source_folder_name, source_user_name, sink_folder_name, sink_user_name))

    # get list of everything to clone from the source folder
    logger.debug("cFFT: user to copy: [{}] folder to copy: [{}]".format(source_user_name,source_folder_name))
    source_items_full = getItemsInFolderForUser(gis, source_folder_name, source_user_name)
    logger.debug("cFFT: full item list: [{}]".format(source_items_full))

    # may not want to clone everything
    source_items = filterItemsForClone(source_items_full)
    logger.debug("cFFT: using source_items: {}".format(source_items))

    ### Check that should create folder even if there are no items to clone.
    if skip_empty_clone == True and len(source_items) == 0:
        logger.info(
            "cFFT: skip clone as no entries to clone for source_user_name: [{}] source_folder_name: [{}]."
                .format( source_user_name, source_folder_name))
        return None

    new_sink_folder = sink_folder_name

    try:
        new_sink_folder = createFolderForUser(gis, sink_folder_name, sink_user_name)
    except ArcgisUMException as excp:
        # error likely to be folder already exists and that's ok.
        logger.debug("Error creating folder: {} but continue clone".format(str(excp)))

    logger.debug("cFFT: have sink folder: [{}]".format(new_sink_folder))

    ### The folder may have already been created.  Don't (re)copy into a sink folder that already has something in it.
    sink_items = getItemsInFolderForUser(gis, sink_folder_name, sink_user_name)
    logger.debug("cFFT: sink folder item list: [{}]".format(sink_items))
    if allow_multiple_clones != True and len(sink_items) != 0:
        logger.debug("cFFT: skip source items as sink folder already has items: {}".format(sink_folder_name))
        return None

    cloneItemListToFolder(gis, sink_folder_name, sink_user_name, source_items)

    # delete the temporary service account folder.
    logger.debug("cFFT: will delete folder: user: [{}] folder: [{}]".format(gis.properties.user.username,sink_folder_name))
    deleteMatchingEmptyFoldersForUser(gis, gis.properties.user.username, sink_folder_name)

    return sink_folder_name


def cloneItemListToFolder(gis, sink_folder_name, sink_user_name, source_items):
    # Need to clone items into service account space and then reassign the items to the sink user.
    # Processing one by one allows error handling on a per item basis.
    for item in source_items:
        cloneSomeItems(gis, [item], sink_folder_name)

    # Reassign items to the final folder in user space.
    clonedItems = getItemsInFolderForUser(gis, sink_folder_name, gis.properties.user.username)
    reassignItemsToUser(gis, clonedItems, sink_user_name, sink_folder_name)
    logger.debug("cILTF: reassigned: {}".format(clonedItems))


def cloneSomeItems(gis, source_items, clone_folder_name):
    """Clone a (sub)list of items to a folder."""
    logger.info("cSI: cloning: {} to: {}".format(source_items, clone_folder_name))
    start = time.time()
    cloned = []
    try:
        cloned = gis.content.clone_items(source_items, folder=clone_folder_name, copy_data=True,
                                         search_existing_items=False)

        # TODO: unicode char in item name will fail.  ArcGIS should fix.
        # Except clause may need tuning. SystemError gives stack trace but terminates.  Exception is way to broad.
    except Exception as exp:
        logger.warning("cSI: caught unexpected exception: {}".format(exp.__class__.__name__))
        logger.warning("cSI: exception {} while cloning {}".format(exp, source_items))

    finally:
        logger.info("cSI: item clone time: seconds: {:8.2f} cloned return: {}".format(time.time() - start, cloned))

    return cloned


def reassignFolderToUser(gis, source_folder_name, source_user_name, sink_folder_name, sink_user_name):
    """Reassign a folder to a different user.  The folder name may be modified."""

    logger.debug("rFTU: entered:")
    items_in_folder = getItemsInFolderForUser(gis, source_folder_name, source_user_name)

    [logger.debug("rFTU: will reassign item: [{}]".format(item)) for item in items_in_folder]
    reassignItemsToUser(gis,items_in_folder,sink_user_name,sink_folder_name)


def reassignItemsToUser(gis, items, sink_user_name,sink_folder_name):
    """Reassign a list of items to a different user."""
    logger.debug("rITU: entered: sink user: [{}] sink folder: [{}]".format(sink_user_name,sink_folder_name))

    reassigned = [item.reassign_to(sink_user_name, sink_folder_name) for item in items]
    logger.debug("rITU: reassigned: [{}]".format(reassigned))

def tagItems(items,tags):
    """assign these tags to all these items."""
    logger.error("******* tagItems: not implemented.")

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
