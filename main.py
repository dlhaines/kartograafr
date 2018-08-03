## TTD:
### set log level by property AND env variable

# 1) examine canvas page for course configuration
# 1a) list of course ids to consider
# 1b) list of outcome ids to consider

# 2) Process courses and assignments
# 2a) get list of assignments to consider for making ArcGIS groups, assignment folders, and grading submissions
# 2b) update assignment groups and membership and assignment folders
# 2c) clone assignment folders to instructor space.

# configuration additions:
# - prefixes for the folders to label as assigments or submissions
# - add adjustment for expiriation time to allow for easier testing.
# - todo: tag(s) for assignment and submission folders

import argparse
from datetime import datetime
import logging

#from libpasteurize.fixes.fix_unpacking import assignment_source

# convenience function to set logger level for named (sub) module.
def setLoggerLevel(logging,logger_name,logger_level):
    this_logger = logging.getLogger(logger_name)
    this_logger.setLevel(logger_level)

logger = logging.getLogger(__name__)
loggingLevel = None

import sys
import os
import re

import arcgisUM

import dateutil.parser
import dateutil.tz
from datetime import timedelta

from bs4 import BeautifulSoup
from bs4.builder._htmlparser import HTMLParserTreeBuilder

import config

from CanvasAPI import CanvasAPI

# In spite of the lint message the secrets module really is used during import 
# (to change sensitive properties). 
import secrets #@UnusedImport

import util

##### Improved code tracebacks for exceptions
import traceback

def handleError(self, record):  # @UnusedVariable
    traceback.print_stack()
logging.Handler.handleError = handleError
#####

TIMEZONE_UTC = dateutil.tz.tzutc()
RUN_START_TIME = datetime.now(tz=TIMEZONE_UTC)
RUN_START_TIME_FORMATTED = RUN_START_TIME.strftime('%Y%m%d%H%M%S')

COURSE_DICTIONARY = 'courseDictionary'
COURSE_INSTRUCTOR_DICTIONARY = 'courseInstructorDictionary'
COURSE_USER_DICTIONARY = 'courseUserDictionary'

# Hold parsed options
options = None

# Adjustable level to use for all logging
logger.error("loggingLevel: {}".format(loggingLevel))
             
if loggingLevel is None:
    loggingLevel = config.Application.Logging.DEFAULT_LOG_LEVEL

logger = None  # type: logging.Logger
logFormatter = None  # type: logging.Formatter
courseLogHandlers = dict()
courseLoggers = dict()

### Set logging level for particular modules.
### These turn off most logging for these libraries.
setLoggerLevel(logging,'urllib3',logging.INFO)
setLoggerLevel(logging,'arcgis._impl',logging.INFO)

def getCanvasInstance():
    return CanvasAPI(config.Canvas.API_BASE_URL,
                     authZToken=config.Canvas.API_AUTHZ_TOKEN)


def getCourseIDsWithOutcome(canvas, courseIDs, outcome):
    """Get Canvas courses that have assignments marked with outcome indicating there should be a corresponding ArgGIS group."""
    matchingCourseIDs = set()
    for courseID in courseIDs:
        courseOutcomeGroupLinks = \
            canvas.getCoursesOutcomeGroupLinksObjects(courseID)

        # Is it possible to short-circuit this using itertools?
        matchingCourseIDs.update(
            set(courseID for outcomeLink in courseOutcomeGroupLinks
                if outcomeLink.outcome.id == outcome.id)
        )

    return matchingCourseIDs


########  Some simple predicates to check the specific states of assignment.

## Check if the assignment is closed.
def isExpired(expirationTime):
    # allow modifying the expiration time, mostly for testing.
    #useExpirationTime = expirationTime + timedelta(days=config.Application.General.EXPIRATION_SLACK_DAYS)
    #logger.debug("iE: expirationTime: {} use_expirationTime: {}".format(expirationTime,useExpirationTime))

    return (expirationTime < RUN_START_TIME)

# Check if this assignment is relevant to arcgis processing.
# TODO: more pythonic? e.g. outcome.id in list of rubric ids?
def isRightRubric(assignment, outcome):

    try:
        for rubric in assignment.rubric:
            # Rubric may have a name of "None"
            if rubric.outcome_id == outcome.id:
                return True
    except TypeError as expc:
        logger.warning("Exception checking assignment rubic. exception: {} assignment: {}".format(expc,assignment))

    # If get here then isn't ArcGIS assignment.
    return False


# TODO: Check to see if date is recent enough so that should clone to the instructor area.
# TODO: implement clone logic

# TEMP for future testing
cloneCount = 0

# TODO: this is under development
def shouldCloneStudentAssignment(assignment,expirationTime):
    # This will make sure that at least one assignment is cloned (for testing)
    assignment_due_at = assignment.due_at
    logger.debug("sCSA: assignmentDate: [{}]".format(assignment_due_at))
    if assignment_due_at is None:
        logger.debug("sCSA: no due date: [{}]".format(assignment.name))
        return False
    return True


def getCourseAssignmentsWithRequiredOutcomeForProcessing(canvas, courseIDs, outcome):
    """Get relevant assignments and sort into ignore, maintain, or clone for grading."""
    # list of assignments to check membership and folder creation.
    maintainAssignments = []
    
    # list of assignments to clone for grading.
    cloneAssignments = []
    
    for courseID in courseIDs:
        logger.info("gCAWROFP: process courseID: {}".format(courseID))
        courseAssignments = canvas.getCoursesAssignmentsObjects(courseID)

        for assignment in courseAssignments:

            logger.debug("gCAWROFP: assignment: [{}]".format(assignment))

            # skip if not published.
            if not assignment.published:
                logger.debug("gCAWROFP: skip unpublished assignment: {}".format(assignment.name))
                continue

            # skip if assignment is not for arcgis.
            if not isRightRubric(assignment, outcome):
                logger.debug("gCAWROFP: assignment: {} >>> wrong rubric".format(assignment.name))
                continue

            expirationTimestamp = assignment.lock_at or assignment.due_at
            expirationTime = dateutil.parser.parse(expirationTimestamp) if expirationTimestamp else RUN_START_TIME
            
            # If recently closed then should clone.
            if shouldCloneStudentAssignment(assignment,expirationTime):
                logger.debug("gCAWROFP: clone assignment: {}".format(assignment))
                cloneAssignments.append(assignment)
                #continue # When testing may want to create the assignment also.
            
            # Nobody cares about this expired assignment.
            # Clone check is done first since that will be an expired assignment but needs special handling.            
            if isExpired(expirationTime):
                logger.debug("gCAWROFP: skip expired assignment: {}".format(assignment))
                continue
            
            # The assignment is relevant and open so maintain it.
            logger.info("gCAWROFP: process assignment: {} for users and folders".format(assignment))
            maintainAssignments.append(assignment)

    return maintainAssignments,cloneAssignments


# Take two lists and separate out entries only in first list, those only in second list, and those in both.
# Uses sets to do this so duplicate entries will become singular. The order in the list will be arbitrary.

def computeListDifferences(leftList, rightList):
    """Take 2 lists and return 3 lists of entries: only in first, only in seconds, only in both lists.  Element order is not preserved. Duplicates will be compressed."""
    
    leftOnly = list(set(leftList) - set(rightList))
    rightOnly = list(set(rightList) - set(leftList))
    both = list(set(rightList) & set(leftList))
               
    return leftOnly, rightOnly, both

# Look at lists of users already in group and those currently in the course and return new lists
# of only the users that need to be added and need to be removed, so unchanged people remain untouched.

def minimizeUserChanges(groupUsers, courseUsers):
    """Compute minimal changes to ArgGIS group membership so that members who don't need to be changed aren't changed."""
    logger.debug('groupUsers input: {}'.format(groupUsers))
    logger.debug('courseUsers input: {}'.format(courseUsers))
    
    # Based on current Canvas and ArcGIS memberships find obsolete users in ArcGIS group, new users in course,
    # and unchanged members in both.
    minGroupUsers, minCourseUsers, unchangedUsers = computeListDifferences(groupUsers,courseUsers)
    
    logger.info('changedArcGISGroupUsers: {} changedCanvasUsers: {} unchanged Users {}'.format(minGroupUsers,minCourseUsers,unchangedUsers))
  
    return minGroupUsers, minCourseUsers


def assureAssignmentFolderForStudent(arcGIS, course, assignment, group,courseData):
    """Make sure there is an appropriate folder in student space for this assignment."""
    
    ## make sure the course students have a folder for the assignment.
    #logger.info("aAFFS: course: [{}] assignment: [{}]".format(course, assignment))
    student_assignment_folder_name = studentAssignmentFolderName(assignment, courseData)
    logger.info("aAFFS: student_assignment_folder_name: [{}]".format(student_assignment_folder_name))
    
    # Create folders for all the members of the group.
    groupUsers = arcgisUM.getCurrentArcGISMembers(group)
    logger.info("aAFFS: groupUsers: [{}]".format(groupUsers))
    createFolderForUsers(arcGIS, groupUsers, student_assignment_folder_name)
    

def updateGroupUsers(courseUserDictionary, course, instructorLog, groupTitle, group):
    # get the arcgis group members and synchronize them.

    #util.step("** uGU: update group users")
    groupNameAndID = util.formatNameAndID(group)
    groupUsers = arcgisUM.getCurrentArcGISMembers(group)
    
    # TODO: centralize the user name formatting in one file.
    logger.debug('uGU: group users: {}'.format(groupUsers)) ## trim the user names
    groupUsersTrimmed = [re.sub('_\S+$', '', gu) for gu in groupUsers]
    
    logger.debug('uGU: list all ArcGIS users currently in Group {}: ArcGIS Users: {}'.format(groupNameAndID, groupUsers))
    canvasCourseUsers = [user.login_id for user in courseUserDictionary[course.id] if user.login_id is not None]
    logger.debug('uGU: All Canvas users in course for Group {}: Canvas Users: {}'.format(groupNameAndID, canvasCourseUsers))
    
    # Compute the exact sets of users to change so don't change existing users.
    changedArcGISGroupUsers, changedCourseUsers = minimizeUserChanges(groupUsersTrimmed, canvasCourseUsers) # added to avoid undefined variable warning
    
    # fix up the user name format for ArcGIS users names
    #changedArcGISGroupUsers = arcgisUM.formatUsersNamesForArcGIS(config.ArcGIS.ORG_NAME, changedArcGISGroupUsers)
    changedArcGISGroupUsers = arcgisUM.formatUsersNamesForArcGIS(changedArcGISGroupUsers)
    if len(changedArcGISGroupUsers) + len(changedCourseUsers) != 0:
        logger.debug('uGU: Users to remove from ArcGIS: Group {}: ArcGIS Users: {}'.format(groupNameAndID, changedArcGISGroupUsers))
        logger.debug('uGU: Users to add from Canvas course for ArcGIS: Group {}: Canvas Users: {}'.format(groupNameAndID, changedCourseUsers))
    
    # Now update only the users in the group that have changed.
    # get rid of departed users
    instructorLog, results = arcgisUM.removeSomeExistingGroupMembers(groupTitle, group, instructorLog, changedArcGISGroupUsers) # @UnusedVariable
    # add new members.
    instructorLog = arcgisUM.addUsersToGroup(instructorLog, group, arcgisUM.formatUsersNamesForArcGIS(changedCourseUsers))
    
    return instructorLog


def updateAssignmentGroupAndFolders(arcGIS,courseData, course, instructorLog, groupTitle, assignment, group):
    """Add remove / users from group to match Canvas course and create assignment folders for the students."""
    
    courseUserDictionary = courseData[COURSE_USER_DICTIONARY]
    util.step("updating group users")
    instructorLog = updateGroupUsers(courseUserDictionary, course, instructorLog, groupTitle, group)
    util.step("setup assignment folders for students")
    assureAssignmentFolderForStudent(arcGIS, course, assignment, group,courseData)
    
    return instructorLog


#################### Folder name formatting methods


def studentAssignmentFolderName(assignment, courseData, prefix=config.Application.General.ASGN_FOLDER_PREFIX):
    """Standard name for automatically created student assignment folders."""

  #  logger.info("sAFN: stack: {}".format(traceback.print_stack()))

    logger.debug("sAFN: courseData: {}".format(courseData))
    courseDictionary = courseData[COURSE_DICTIONARY]

    logger.debug("sAFN: assignment: [{}]".format(assignment))
    logger.debug("sAFN: assignment.course_id: [{}]".format(assignment.course_id))
    course = courseDictionary[assignment.course_id]
    folderName = '{}{}_{}_{}_{}'.format(prefix,course.name,assignment.name,course.id,assignment.id)

    logger.debug("sAFN: title: [{}]".format(folderName))
    return folderName


def studentSubmissionFolderName(assignment, courseData, student, prefix=config.Application.General.SUBMISSION_FOLDER_PREFIX):
    '''Create folder name for graded submission.  Include course_code and student name.'''

    logger.debug("sSFN: courseData: [{}]".format(courseData))
    courseDictionary = courseData[COURSE_DICTIONARY]

    course = courseDictionary[assignment.course_id]
    course_code = course.course_code
    logger.debug("sSFT: course_code: [{}] course: [{}]".format(course_code,course))

    student_title = studentAssignmentFolderName(assignment, courseData, prefix='')

    submission_folder_title = '{}{}_{}_{}'.format(prefix,course_code,student_title,student)
    logger.info("sSFT: submission_folder_title: [{}]".format(submission_folder_title))
    return submission_folder_title


def arcGisGroupName(assignment, course):
    groupTitle = '%s_%s_%s_%s' % (course.name, course.id, assignment.name, assignment.id)
    return groupTitle


# TODO: what if problem creating folder?
def createFolderForUsers(arcGIS,users,folder_name):
    # create folders for a group of students

    for user in users:
        logger.debug("cFFU: process user: [{}]".format(user))
        try:
            arcgisUM.createFolderForUser(arcGIS,folder_name,user)
        except arcgisUM.ArcgisUMException as exp:
            logger.debug("cFFU: Error creating folder: {} user: {} folder_name: [{}]".format(str(exp),user,folder_name))


def updateArcGISGroupAndStudentFoldersForAssignment(arcGIS, courseData, groupTags, assignment, course,instructorLog):
    """" Make sure there is a corresponding ArcGIS group for this Canvas course and assignment.  Sync up the ArcGIS members with the Canvas course members."""
     
    groupTitle = arcGisGroupName(assignment, course)
    
    group = arcgisUM.getArcGISGroupByTitle(arcGIS, groupTitle)
     
    if group is None:
        group, instructorLog = arcgisUM.createNewArcGISGroup(arcGIS, groupTags, groupTitle,instructorLog)
    
    # if creation didn't work then log that.
    if group is None:
        logger.info('Problem creating or updating ArcGIS group "{}": Missing group object.'.format(groupTitle))
        instructorLog += 'Problem creating or updating ArcGIS group "{}"\n'.format(groupTitle)
    else: 
        # have a group.  Might be new or existing.
        instructorLog = updateAssignmentGroupAndFolders(arcGIS,courseData, course, instructorLog, groupTitle, 
                                                   assignment, group)

    courseLogger = getCourseLogger(course.id, course.name)
    logger.debug("uAGASFFA: update group instructor log: {}".format(instructorLog))
    courseLogger.info(instructorLog)


# For all the assignments and their courses update the ArcGIS group.
def updateArcGISGroupsForAssignments(arcGIS, assignments, courseData):
    """For each assignment listed ensure there is an ArcGIS group corresponding to the Canvas course / assignment."""

    groupTags = ','.join(('kartograafr', 'umich'))
    courseDictionary = courseData[COURSE_DICTIONARY]
    logger.debug("uAGGFA: groupTags: {}".format(groupTags))
    for assignment in assignments:
        course = courseDictionary[assignment.course_id]
        instructorLog = ''
        updateArcGISGroupAndStudentFoldersForAssignment(arcGIS, courseData, groupTags, assignment, course,instructorLog)

## assignment has the course and assignment canvas ids
def cloneTheseAssignments(gis,assignmentsToClone,courseData):
    '''Clone these assignmentsToClone to the instructor content area.'''

    util.step("clone assignments")
    # unpack the course data
    
    courseDictionary = courseData[COURSE_DICTIONARY]
    courseUserDictionary = courseData[COURSE_USER_DICTIONARY]
    courseInstructorDictionary = courseData[COURSE_INSTRUCTOR_DICTIONARY]
    
    if logger.isEnabledFor(logging.DEBUG):
        for c in courseDictionary:
            logger.debug("CTA: course: [{}] content: [{}]".format(c,courseDictionary[c]))
        for i in courseInstructorDictionary:
            logger.debug("CTA: instructor: [{}] content: [{}]".format(i,courseInstructorDictionary[i]))
        for u in courseUserDictionary:
            logger.debug("CTA: user: [{}] content: [{}]".format(u,courseUserDictionary[u]))
    
    for assignment in assignmentsToClone:
        logger.info("cTA: clone assignment: [{}]".format(assignment))

        course_id = assignment.course_id
        instructor_name = courseInstructorDictionary.get(course_id)[0].login_id
        student_names = [u.login_id for u in courseUserDictionary.get(course_id)]

        # Remove instructor if in list of users.  If not there don't worry.
        try:
            student_names.remove(instructor_name)
        except:
            pass

        logger.debug("cTA: filtered instructor: [{}] students: [{}]".format(instructor_name, student_names))

        cloneThisAssignmentForStudents(gis,assignment,instructor_name,student_names,courseData)
    

def cloneThisAssignmentForGrading(gis, assignment, instructor_name, student_name, courseData):
    '''Clone this one student assignment to the instructor content area.'''

    logger.debug("cTAFG assignment: [{}] instructor: [{}] student: [{}]".format(assignment,instructor_name, student_name))

    # source is student assignment folder.
    source_folder_name, source_user = constructStudentAssignmentFolderData(assignment, courseData, student_name)
    logger.debug("cTAFG: student folder: [{}] student name: [{}]".format(source_folder_name,source_user))

    # sink is instructor
    sink_folder_name,sink_user = constructInstructorCloneFolderData(instructor_name, assignment, courseData, student_name)
    logger.debug("cTAFG: sink_user: [{}] sink_folder_name: [{}]".format(sink_user,sink_folder_name))

    cf = arcgisUM.cloneFolderFromTo(gis, source_folder_name, source_user, sink_folder_name, sink_user)
    logger.debug("cTAFG: cf: [{}]".format(cf))


def constructStudentAssignmentFolderData(assignment, courseData, student_name):
    """Generate the name of the student assignment folder"""

    source_folder_name = studentAssignmentFolderName(assignment, courseData)
    #formatUsersNamesForArcGIS
    #source_user = "{}_{}".format(student, config.ArcGIS.ORG_NAME)
    source_user = arcgisUM.formatUsersNamesForArcGIS([student_name])[0]

    logger.debug("cSAFD: source_user: [{}]  source_folder_name: [{}]".format(source_user, source_folder_name))
    return source_folder_name, source_user


#def constructInstructorCloneFolderData(instructor, source_folder_name,source_user):
def constructInstructorCloneFolderData(instructor,assignment, courseData,student_name):
    """Generate name of the instructors clone of assignment folder"""
    # sink will be instructor.
    #sink_folder_name = "{}_{}_{}".format('GRADEME:',source_user,source_folder_name)
    #sink_user = "{}_{}".format(instructor, config.ArcGIS.ORG_NAME)

    sink_folder_name=studentSubmissionFolderName(assignment, courseData, student_name,
                                prefix=config.Application.General.SUBMISSION_FOLDER_PREFIX)
    sink_user = arcgisUM.formatUsersNamesForArcGIS([instructor])[0]

    #logger.info("cICFD: sink_user: [{}] sink_folder_name: [{}]".format(sink_user,sink_folder_name))
    #return sink_folder_name
    return sink_folder_name, sink_user


def cloneThisAssignmentForStudents(gis, assignment, instructor, student_names, courseData):
    '''Clone each students assignment folder to the instructor content area for all these students.'''
    logger.debug("CTA B: instructor: [{}]".format(instructor))
    logger.debug("cTA B: cloning assignment: [{}]".format(assignment))
    logger.debug("cTA B: students: [{}]".format(student_names))

    for student_name in student_names:
        if not student_name is None:
            cloneThisAssignmentForGrading(gis, assignment, instructor, student_name, courseData)
    
    
def getCoursesByID(canvas, courseIDs):
    """Get Canvas course objects for the listed courses."""
    courses = {}
    for courseID in courseIDs:
        logger.info("getCoursesById: courseId: {}".format(courseID))
        courses[courseID] = canvas.getCourseObject(courseID)
    return courses


def getCoursesUsersByID(canvas, courseIDs, enrollmentType=None):
    """Get Canvas course members for specific course.  Can filter by members's Canvas role.

    :param canvas:
    :type canvas: CanvasAPI
    :param courseIDs:
    :type courseIDs: set or list
    :param enrollmentType: (optional) Canvas user enrollment type: 'student', 'teacher', etc.
    :type enrollmentType: str
    :return:
    """
    coursesUsers = {}
    for courseID in courseIDs:
        coursesUsers[courseID] = canvas.getCoursesUsersObjects(courseID, enrollmentType=enrollmentType,
                                                               params={'include[]': 'email'})
    return coursesUsers


def getCourseLogFilePath(courseID):
    """Each course will have a separate sub-log file.  This is the path to that file."""
    return os.path.realpath(os.path.normpath(os.path.join(
        config.Application.Logging.COURSE_DIRECTORY,
        courseID + config.Application.Logging.LOG_FILENAME_EXTENSION,
    )))


def getMainLogFilePath(nameSuffix=None):
    """Return the path/filename of the main log file."""
    mainLogName = config.Application.Logging.MAIN_LOG_BASENAME

    if nameSuffix is not None:
        mainLogName += '-' + str(nameSuffix)

    return os.path.realpath(os.path.normpath(os.path.join(
        config.Application.Logging.DIRECTORY,
        mainLogName + config.Application.Logging.LOG_FILENAME_EXTENSION,
    )))


def logToStdOut():
    """Have log output go to stdout in addition to any file."""
    root = logging.getLogger()
    root.setLevel(loggingLevel)
 
    ch = logging.StreamHandler(sys.stdout)
    #ch.setLevel(logging.DEBUG)
    ch.setLevel(loggingLevel)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)


def getCourseLogger(courseID, courseName):
    """Set up course specific logger.
    
    :param courseID: ID number of the course
    :type courseID: str or int
    :param courseName: Name of the course
    :type courseName: str
    :return: A logging handler for a specific course's log file
    :rtype: logging.FileHandler
    """
    global courseLoggers  # type: dict
 
    courseID = str(courseID)

    if courseID in courseLoggers:
        return courseLoggers[courseID]

    logFormatterFriendly = logging.Formatter('Running at: %(asctime)s\n\n%(message)s', '%I:%M:%S %p on %B %d, %Y')

    logHandlerMain = logging.FileHandler(getMainLogFilePath())
    logHandlerMain.setFormatter(logFormatterFriendly)

    logHandlerCourse = logging.FileHandler(getCourseLogFilePath(courseID))
    logHandlerCourse.setFormatter(logFormatterFriendly)

    courseLogger = logging.getLogger(courseID)  # type: logging.Logger
    courseLogger.setLevel(loggingLevel)
    courseLogger.addHandler(logHandlerMain)
    courseLogger.addHandler(logHandlerCourse)

    courseLoggers[courseID] = courseLogger

    return courseLogger


def getCourseLogHandler(courseID, courseName):
    """Lookup the course specific logger for this course.
    
    :param courseID: ID number of the course
    :type courseID: str or int
    :param courseName: Name of the course
    :type courseName: str
    :return: A logging handler for a specific course's log file
    :rtype: logging.FileHandler
    """
    global logFormatter  # type: logging.Formatter
    global courseLogHandlers  # type: dict

    courseID = str(courseID)

    if courseID in courseLogHandlers:
        return courseLogHandlers[courseID]

    courseLogHandler = logging.FileHandler(getCourseLogFilePath(courseID))
    courseLogHandler.setFormatter(logFormatter)

    courseLogHandlers[courseID] = courseLogHandler

    return courseLogHandler


def closeAllCourseLoggerHandlers():
    global courseLoggers

    for (courseID, courseLogger) in courseLoggers.items():  # type: logging.Logger
        for handler in courseLogger.handlers:  # type: logging.Handler
            handler.close()


def closeAllCourseLogHandlers():
    global courseLogHandlers

    for (courseID, courseLogHandler) in courseLogHandlers.items():
        courseLogHandler.close()


def getCourseIDsFromConfigCoursePage(canvas, courseID):
    """Read hand edited list of Canvas course ids to process from a specific Canvas course page."""
    
    VALID_COURSE_URL_REGEX = '^https://umich\.instructure\.com/courses/[0-9]+$'
    pages = canvas.getCoursesPagesByNameObjects(courseID, 'course-ids')  # type: list of CanvasObject
    courseIDs = None

    if pages:
        configCoursePage = pages.pop()
        configCoursePageTree = BeautifulSoup(configCoursePage.body, builder=HTMLParserTreeBuilder())

        courseURLs = set([a['href'] for a in configCoursePageTree.find_all('a', href=re.compile(VALID_COURSE_URL_REGEX))])
        if courseURLs:
            courseIDs = [int(url.split('/').pop()) for url in courseURLs]

    return courseIDs


def renameLogForCourseID(courseID=-1):
    """Change name of the course specific log file."""
    if courseID == -1:
        raise RuntimeError('Renaming logs requires either a course ID number to rename the log for that course, '
                           'or the None value to rename the main log.')

    if courseID is not None:
        courseID = str(courseID)
        oldLogName = getCourseLogFilePath(courseID)
        newLogName = getCourseLogFilePath(courseID + '-' + RUN_START_TIME_FORMATTED)
    else:
        oldLogName = getMainLogFilePath()
        newLogName = getMainLogFilePath(nameSuffix=RUN_START_TIME_FORMATTED)

    if os.path.isfile(oldLogName) is True:
        os.rename(oldLogName, newLogName)

    return (oldLogName, newLogName)


def emailLogForCourseID(courseID, recipients):
    """Email course information to a list of multiple recipients."""

    import smtplib
    from email.mime.text import MIMEText
    from email.header import Header

    if not isinstance(recipients, list):
        recipients = [recipients]

    courseID = str(courseID)

    logContent = None

    # File may not exist if no changes were made to group.
    if os.path.isfile(getCourseLogFilePath(courseID)) is not True:
        logger.debug('No logfile {} for course: {}'.format(getCourseLogFilePath(courseID),courseID))
        return

    try:
        READ_BINARY_MODE = 'rb'
        logfile = open(getCourseLogFilePath(courseID), mode=READ_BINARY_MODE)
        logContent = logfile.read()
        logfile.close()
    except Exception as exception:
        logger.warning('Exception while trying to read logfile for course {courseID}: {exception}'
                       .format(**locals()))
        return
    
    message = MIMEText(logContent,'plain','utf-8')
    message['From'] = Header(config.Application.Email.SENDER_ADDRESS,'utf-8')
    message['To'] = Header(', '.join(recipients),'utf-8')
    message['Subject'] = Header(config.Application.Email.SUBJECT.format(**locals()),'utf-8')
      
    if options.printEmail is True:
        logger.info("email message: {}".format(message))
    else:
        try:
            server = smtplib.SMTP(config.Application.Email.SMTP_SERVER)
            logger.debug("mail server: " + config.Application.Email.SMTP_SERVER)
            server.set_debuglevel(config.Application.Email.DEBUG_LEVEL)
            server.sendmail(config.Application.Email.SENDER_ADDRESS, recipients, message.as_string())
            server.quit()
            logger.info('Email sent to {recipients} for course {courseID}'.format(**locals()))
        except Exception as exception:
            logger.exception('Failed to send email to {recipients} for course {courseID}.  Exception: {exception}'
                         .format(**locals()))

    try:
        (oldLogName, newLogName) = renameLogForCourseID(courseID)
        logger.info('Renamed course log "{oldLogName}" to "{newLogName}"'.format(**locals()))
    except Exception as exception:
        logger.exception('Failed to rename log file for course {courseID}.  Exception: {exception}'
                         .format(**locals()))


def emailCourseLogs(courseInstructors):
    """ Loop through instructors to email course information to them.
    
    :param courseInstructors: Dictionary of courses to list of their instructors
    :type courseInstructors: dict
    """
    
    logger.info('Preparing to send email to instructors...')

    for courseID, instructors in list(courseInstructors.items()):
        recipients = [instructor.sis_login_id +
                                            config.Application.Email.RECIPIENT_AT_DOMAIN for instructor in instructors]
        emailLogForCourseID(courseID, recipients)


def main():
    """Setup and run Canvas / ArcGIS group sync.
    
    * parse command line arguments.
    * setup loggers.
    * connect to Canvas and  ArcGIS instances.
    * get list of relevant assignments from Canvas courses listed hand-edited Canvas page.
    * update membership of ArcGIS groups corresponding to Canvas course / assignments.
    """
    
    global logger
    global logFormatter
    global options

    logFormatter = util.Iso8601UTCTimeFormatter('%(asctime)s|%(levelname)s|%(name)s|%(message)s')

    logHandler = logging.FileHandler(getMainLogFilePath())
    logHandler.setFormatter(logFormatter)

    logger = logging.getLogger(config.Application.Logging.MAIN_LOGGER_NAME)  # type: logging.Logger
    logger.setLevel(loggingLevel)
    logger.addHandler(logHandler)
    
    # Add logging to stdout for OpenShift.
    logToStdOut()

    util.step("Starting kartograafr")

    argumentParser = argparse.ArgumentParser()

    argumentParser.add_argument('--mail', '--email', dest='sendEmail',
                                action=argparse._StoreTrueAction,
                                help='email all available course logs to instructors, then rename all logs.')

    argumentParser.add_argument('--printMail', '--printEmail', dest='printEmail',
                                action=argparse._StoreTrueAction,
                                help='print emails to log instead of sending them.')

    argumentParser.add_argument('--skipEmptySubmissions', dest='printEmail',
                                action=argparse._StoreTrueAction,
                                help='Do not make submission folders if student assignment folder is empty.')


    options, unknownOptions = argumentParser.parse_known_args()

    logger.info('kart sys args: {} '.format(sys.argv[1:]))

    if unknownOptions:
        unknownOptionMessage = 'unrecognized arguments: %s' % ' '.join(unknownOptions)
        usageMessage = argumentParser.format_usage()

        logger.warning(unknownOptionMessage)
        logger.warning(usageMessage)

        # Also print usage error messages so they will appear in email to sysadmins, sent from crond
        print(unknownOptionMessage)
        print(usageMessage)

    logger.info('{} email to instructors with logs after courses are processed'
                .format('Sending' if options.sendEmail else 'Not sending'))

    logger.info("Application.General settings: {}".format(vars(config.Application.General)))

    ################ process the classes ###############
    ## to make separate method need to avoid the embedded 'return'
    canvas = getCanvasInstance()
    arcGIS = arcgisUM.getArcGISConnection(config.ArcGIS.SECURITYINFO)

    outcomeID = config.Canvas.TARGET_OUTCOME_ID
    logger.info('Config -> Outcome ID to find: {}'.format(outcomeID))

    validOutcome = canvas.getOutcomeObject(outcomeID)

    if validOutcome is None:
        raise RuntimeError('Outcome ID {} was not found'.format(outcomeID))

    logger.info('Config -> Found valid Outcome: {}'.format(validOutcome))

    configCourseID = config.Canvas.CONFIG_COURSE_ID
    configCoursePageName = config.Canvas.CONFIG_COURSE_PAGE_NAME

    logger.info('Config -> Attempting to get course IDs from page '
                '"{configCoursePageName}" of course {configCourseID}...'
                .format(**locals()))

    util.step("get canvas course ids")

    with util.LoggingContext(logger, level=logging.ERROR):
        courseIDs = getCourseIDsFromConfigCoursePage(canvas, configCourseID)

    if courseIDs is None:
        logger.warning('Warning: Config -> Course IDs not found in page '
                       '"{configCoursePageName}" of course {configCourseID}. '
                       'Using default course IDs instead.'
                       .format(**locals()))
        courseIDs = config.Canvas.COURSE_ID_SET
    else:
        logger.info('Config -> Found Course IDs in page '
                    '"{configCoursePageName}" of course {configCourseID}.'
                    .format(**locals()))

    with util.LoggingContext(logger, level=logging.ERROR):
        logger.info('Config -> Course IDs to check for Outcome {}: {}'.format(validOutcome,list(courseIDs)))

    matchingCourseIDs = getCourseIDsWithOutcome(canvas, courseIDs,validOutcome)

    if len(matchingCourseIDs) == 0:
        raise RuntimeError('No Courses linked to Outcome {} were found'.format(validOutcome))

    logger.info('Config -> Found Course IDs for Outcome {}: {}'.format(validOutcome,
                                                                       list(matchingCourseIDs)))

    logger.info('Searching specified Courses for Assignments linked to Outcome {}'.format(validOutcome))

    util.step("**** Get course assignment list and clone assignments list")

    with util.LoggingContext(logger, level=logging.FATAL):
        matchingCourseAssignments,cloneAssignments = getCourseAssignmentsWithRequiredOutcomeForProcessing(canvas, matchingCourseIDs, validOutcome)
    
    logger.debug("main: matchingCourseAssignments: {}".format(matchingCourseAssignments))
    logger.debug("main: cloneAssignments: {}".format(cloneAssignments))

    for m in matchingCourseAssignments:
        logger.info(" >> matchingCourseAssignment: {}".format(m))

    for a in cloneAssignments:
        logger.info(" >> cloneAssignment: {}".format(a))

    # TODO: improve test so return only if there are also no clone assignments found. Maybe more complicated if only one has values? 
    if not matchingCourseAssignments:
        logger.info('No valid Assignments linked to Outcome {} were found'.format(validOutcome))
        return

    logger.info('Found valid assignments linked to Outcome {}: {}'.format(validOutcome,
                                                                    ', '.join(map(str, matchingCourseAssignments))))

    # These are kept together since they often are needed together and otherwise need to be passed separately.
    courseData = {
        COURSE_DICTIONARY:getCoursesByID(canvas, matchingCourseIDs),
        COURSE_USER_DICTIONARY: getCoursesUsersByID(canvas, matchingCourseIDs),
        COURSE_INSTRUCTOR_DICTIONARY: getCoursesUsersByID(canvas, matchingCourseIDs, 'teacher')
        }

    util.step("**** update ArcGIS groups for assignments")
    with util.LoggingContext(logger, level=logging.FATAL):
        updateArcGISGroupsForAssignments(arcGIS, matchingCourseAssignments, courseData)

    util.step("**** clone finished assigments")
    with util.LoggingContext(logger, level=logging.FATAL):
        cloneTheseAssignments(arcGIS,cloneAssignments,courseData)

    ############## end of processing courses

    closeAllCourseLoggerHandlers()

    if options.sendEmail:
        emailCourseLogs(courseData[COURSE_INSTRUCTOR_DICTIONARY])
        
        #emailCourseLogs(courseInstructorDictionary)

    renameLogForCourseID(None)
    
    logger.info("current kartograaf run finished.")


if __name__ == '__main__':
    kartStartTime = datetime.now()
    try:
        main()
    except Exception as exp:
        logger.error("abnormal ending: {}".format(exp))
        traceback.print_exc(exp)
    finally:
        logger.info("Stopping kartograafr.  Duration: {} seconds".format(datetime.now()-kartStartTime))
