import collections
import datetime
import inspect
import itertools
import logging
import multiprocessing
import os
import random
import re
import shutil
import string
import sys
import tarfile
import tempfile
import threading
import time

#################### Source-preparing ####################

def pack_main_tarball(tar_name, pkg_version, build_name, export_dir,
                      destdir='.', logger=None):
    """
    Create the main tarball for a build from a source tree.  The tarball itself
    will be called ${tar_name}-${pkg_version}.tar.gz, and its top-level
    directory will be called ${tar_name}-${pkg_version}.
    """
    def ignore_vcs_dirs(dirname, filenames):
        """
        Ignore files in version control metadata directories like .bzr and .git.
        """
        if dirname.endswith('.bzr') or dirname.endswith('.git'):
            return filenames
        else:
            return []

    tempdir = tempfile.mkdtemp()
    new_topdir = tar_name + '-' + pkg_version
    # build_name corresponds to the name of the main source topdir
    shutil.copytree(
            os.path.join(export_dir, build_name),
            os.path.join(tempdir, new_topdir),
            ignore=ignore_vcs_dirs)

    new_tarball_name = os.path.join(destdir, new_topdir + '.tar.gz')
    new_tarball = tarfile.open(name=new_tarball_name, mode='w:gz')

    logging.getLogger(logger).info('Creating main tarball ' + new_tarball_name)

    pwd = os.getcwd()
    try:
        os.chdir(tempdir)
        new_tarball.add(new_topdir)
    finally:
        new_tarball.close()
        os.chdir(pwd)
        shutil.rmtree(tempdir)

def read_version_file(filename, logger=None):
    """
    Read the first line of a file to use as the version of a package.  If the
    string begins with 'eee-' or ends with either '-dev' or '-devel', those
    prefixes and suffixes will be stripped off.
    """
    with open(filename) as version_file:
        pkg_version = version_file.readline().strip()
    # Strip off '-dev' or '-devel'
    if pkg_version.endswith('-dev'):
        pkg_version = pkg_version[:-4]
    elif pkg_version.endswith('-devel'):
        pkg_version = pkg_version[:-6]
    # Strip off 'eee-'
    if pkg_version.startswith('eee-'):
        pkg_version = pkg_version[4:]
    logging.getLogger(logger).debug('Found version ' + repr(pkg_version) +
                                    ' in file ' + filename)
    return pkg_version

#################### Memo-parsing ####################

def _explode_list(line):
    """
    If a string begins and ends with a matched pair of square braces, return a
    list of its contents split on commas.  Otherwise, return a list with the
    string alone.

    Examples:
        'foo'        -> ['foo']
        '[bar, baz]' -> ['bar', 'baz']
    """
    if line.startswith('[') and line.endswith(']'):
        bits = line.strip('[]').split(',')
        return [bit.strip() for bit in bits if bit.strip()]
    else:
        return [line]

def get_pkg_sources(config, build_name=None):
    """
    Read 'pkg_source' lines from the memo section of a test configuration
    and return a list of URIs.  If build_name is supplied, return only
    those sources that are appropriate for the given build.

    'pkg_source' lines contain the word 'pkg_source' (whose case does
    not matter), followed by the URI of a source needed for building.
    An optional third parameter may be '*', the name of a single build,
    or a comma-delimited list of builds surrounded by square braces.
    If the caller supplies a build name then only sources with a matching,
    missing, or '*' third parameter are returned.

    pkg_source http://www.example.com/sources/foo.tar.gz
    pkg_source http://www.example.com/sources/bar.tar.gz *
    pkg_source http://www.example.com/sources/baz.tar.gz mybuild
    pkg_source http://www.example.com/sources/bop.tar.gz [build1, build2]
    """
    sources = []
    for line in config.get('memo', []):
        try:
            if re.match('pkg_source\s', line.lower()):
                match = re.match('\w+\s+' + '([^\s]+)' +
                                 '(?:\s+([\w\-]+|\[[\w\- ,]*\]|\*))?', line)
                (uri, buildstr) = match.groups()
                if buildstr is None or build_name is None:
                    sources.append(uri)
                else:
                    builds = _explode_list(buildstr)
                    if build_name in builds or '*' in builds:
                        sources.append(uri)
        except ValueError:
            logging.error('Malformed pkg_source line: "' + line.strip() + '"')
    return sources

def get_pkg_builds(config):
    """
    Read 'pkg_build' lines from the memo section of config and return a set of
    tuples that represents all of the build/platform combinations requested.

    'pkg_build' lines consist of five fields:
     - tag:      The literal, but case-insensitive string "pkg_build"
     - dist:     Which distribution to build for (e.g. "centos")
     - release:  Which distribution release to build for (e.g. "5")
     - arch:     Which processor architecture to build for (e.g. "x86_64")
     - build:    The name given to the main source tree in its 'bzr_export'
                 or 'git_clone' line.  This is also referred to as the
                 build's name.

    The last four of these may either be single words consisting of characters
    from the [:word:] character class and periods, or comma-delimited lists of
    such words surrounded by square braces.  Specifying a list in this manner
    adds the Cartesian product of all combinations of list items to the set of
    tuples that is returned.

    For example, the following lines add one, two, and four builds each:

    pkg_build debian lenny    i386           main
    pkg_build centos 5        [x86_64, i386] main
    pkg_build fedora [13, 14] [x86_64, i386] main
    """
    build_tuples = set()
    for line in config.get('memo', []):
        if not re.match('pkg_build\s', line.lower()):
            continue

        match = re.match('\w+\s+' + '([\w.]+|\[[\w. ,]*\])\s+' * 3 +
                         '([\w.\-]+|\[[\w.\- ,]*\])', line)
        if not match:
            logging.error('Malformed pkg_build line: "' + line.strip() + '"')
            continue

        dists  = _explode_list(match.group(1).lower())
        rels   = _explode_list(match.group(2).lower())
        arches = _explode_list(match.group(3).lower())
        srcs   = _explode_list(match.group(4))
        build_tuples.update(itertools.product(dists, rels, arches, srcs))
    return build_tuples

def get_pkg_build_options(config):
    """
    Read 'pkg_build_option' lines from the memo section of config and return a
    dictionary that maps build names to dictionaries of key/value pairs that
    represent options to be parsed by build scripts.

    'pkg_build_option' lines consist of four fields:
     - tag:    The literal, but case-insensitive string "pkg_build_option"
     - build:  The build to which the option applies, or '*' for all of them
     - key:    The name of the build option
     - value:  The value of the build option

    Build options are key/value pairs that apply to one or more builds.  Global
    build options, specified by a '*' instead of a build name, apply to all
    builds.  These are overridden by build-specific options where the 'key'
    field matches a build name.

    The 'build' and 'key' fields may either be single words from the [:word:]
    character class or comma-delimited lists of such words surrounded by square
    braces.  The 'value' field may be any one of the following:
     - A single word made of any characters that are neither whitespace nor
       single or double quotes
     - Any set of non-single-quote characters surrounded by single-quotes
     - Any set of non-double-quote characters surrounded by double-quotes

    Specifying a list for a 'build' or 'key' field has the same effect as
    specifying an option for the Cartesian product of all combinations of
    the build, key, and value.

    Some examples:

    pkg_build_option main        pkg_name           eucalyptus
    pkg_build_option main        specfile_dir       ${vcsdir}/main-spec
    pkg_build_option *           pkg_version_suffix bzr${revno}
    pkg_build_option [main, eee] pkg_release        0.1
    """
    parsed_options = {'*': {}}
    for line in config.get('memo', []):
        if not re.match('pkg_build_option\s', line.lower()):
            continue
        match = re.match('\w+\s+' + '([\w\-]+|\[[\w\- ,]*\]|\*)\s+' +
                         '([\w\-]+|\[[\w\- ,]*\])\s+' +
                         '([^\s"\']+|"[^"]*"|\'[^\']*\')', line)
        if not match:
            logging.error('Malformed pkg_build_option line: "' + line.strip() +
                          '"')
            continue

        pkgs  = _explode_list(match.group(1))
        keys  = _explode_list(match.group(2).lower())
        value = match.group(3).strip('"\'')
        for (pkg, key, value) in itertools.product(pkgs, keys, [value]):
            parsed_options.setdefault(pkg, {})
            parsed_options[pkg][key] = value
    pkgs = list(set([build[3] for build in get_pkg_builds(config)]))
    options = dict(zip(pkgs, [{} for __ in range(len(pkgs))]))
    for key in options.iterkeys():
        # First load global options
        options[key].update(parsed_options['*'])
        # Then override them with package-specific options
        options[key].update(parsed_options[key])
    return options

def subst_vars_in_builder_config(config):
    """
    Given a builder configuration dictionary and an input.txt dictionary,
    substitute shell-style variables wherever they appear in the configuration
    dictionary.

    Variables that are substituted include:
     - randint:  random integer from 0 to 99999
     - date:     today's date in YYYYMMDD format
     - name:     package name
     - version:  package version
     - dist:     target distribution
     - release:  target release
     - arch:     target processor architecture
     - revno:    version control revision number or ID
     - vcsdir:   version control source export directory
     - srcdir:   direct-download source directory
    """
    substitutions = \
            {'randint':  random.randint(0, 99999),
             'date':     datetime.date.today().strftime('%Y%m%d'),
             'name':     config.get('pkg_name', None),
             'version':  config.get('pkg_version', None),
             'dist':     config.get('target_dist', None),
             'release':  config.get('target_release', None),
             'arch':     config.get('target_arch', None),
             'revno':    config.get('revno', None),
             'vcsdir':   config.get('export_dir', None),
             'bzrdir':   config.get('export_dir', None), # legacy
             'srcdir':   config.get('sourcedir', None),
            }
    # Don't substitute variables that lack values
    for key in substitutions.keys():
        if not substitutions[key]:
            del substitutions[key]

    for (key, val) in config.iteritems():
        if type(val) is str:
            config[key] = string.Template(val).safe_substitute(substitutions)
        elif type(val) is list:
            for i in range(len(val)):
                if type(val[i]) is str:
                    val[i] = string.Template(val[i]).safe_substitute(
                            substitutions)

#################### Task management ####################

class Task(object):
    def __init__(self, action, dist=None, release=None, arch=None, pkg=None,
                 id=0, outputfile=None, **kwargs):
        self.action  = action
        self.dist    = dist
        self.release = release
        self.arch    = arch
        self.pkg     = pkg
        self.id      = id

        # Additional args that are left up to individual tasks to interpret
        self.custom_kwargs = dict(kwargs)

        self.__process  = None
        self.__handlers = []
        self.__deps     = []

        self.__lock = threading.Lock()
        self.handler_depth = 0

        # Logger used for the Task itself, not its action
        self.__logger = logging.getLogger('Task-{0}'.format(self.id))
        # File for all the output from this Task's action
        self.__outputfile = outputfile

        # State is one of 'blocked' 'free' 'open' 'done' 'failed' 'canceled'.
        # None is a "semi-valid" representation of an undefined state.  The
        # state begins as None, but by the time all of the tasks that this task
        # depends on have calculated their own states it should not be None for
        # the rest of its lifetime.  If it is, chances are good that there is a
        # dependency cycle or a bug.
        self.__state = None
        self.__valid_states = ['blocked', 'free', 'open', 'done', 'failed',
                               'canceled', None]

    @property
    def state(self):
        return self.__state

    @state.setter
    def state(self, newstate):
        """
        Safely change this task's state and then run all of the handler
        functions registered to state changes.
        """
        with self.__lock:
            if self.__state == newstate:
                return
            if newstate not in self.__valid_states:
                raise ValueError('Cannot change to invalid state "{0}"'
                                 .format(str(newstate)))
            self.__logger.log(
                    (logging.WARN if newstate is None else logging.DEBUG),
                    'state {state} -> {new_state}'
                    .format(id=self.id, state=self.__state, new_state=newstate))
            self.__state = newstate
            if newstate == 'canceled':
                try:
                    self.__process.terminate()
                except AttributeError:
                    # The process has yet to be started
                    pass
        self.__run_handlers()

    def __str__(self):
        params = []
        for param in [self.dist, self.release, self.arch, self.pkg]:
            if param:
                params.append(param)
        return '{action}({params})'.format(action=self.action.__name__,
                                           params=', '.join(params))

    def register_handler(self, handler):
        """
        Register a handler method that gets called when this task changes
        state.  The handler must accept this task as its only argument.

        This method does nothing when called on a handler that is already
        registered.
        """
        with self.__lock:
            if handler not in self.__handlers:
                self.__handlers.append(handler)

    def unregister_handler(self, handler):
        """
        Un-register a handler method that was previously added with the
        register_handler method.

        This method does nothing when called on a handler that is not
        registered.
        """
        with self.__lock:
            if handler in self.__handlers:
                self.__handlers.remove(handler)

    def add_dependency(self, *deps):
        """
        Add one or more tasks to this task's list of dependencies.
        """
        for dep in deps:
            with self.__lock:
                if dep not in self.__deps:
                    self.__deps.append(dep)
                    dep.register_handler(lambda __: self.reread_state())

    def __run_handlers(self):
        """
        Run all of the handler functions registered to changes in state.
        The handler_depth variable tracks recursion depth in case one handler
        calls another.  When handler_depth is nonzero this Task's position in
        task lists and consistency with dependent tasks may not yet be correct,
        so constraint checkers will ignore tasks that have positive
        handler_depths.
        """
        # Iterate over a copy of the list in case something changes it
        with self.__lock:
            self.handler_depth += 1
            handlers_tmp = list(self.__handlers)
        try:
            for handler in handlers_tmp:
                handler(self)
        finally:
            with self.__lock:
                self.handler_depth -= 1

    def reread_state(self):
        """
        Cause this task to reread its state by inspecting the states of its
        dependencies.
        """
        with self.__lock:
            dep_states = [dep.state for dep in self.__deps]
        if None in dep_states:
            # Uh oh, our state is now undefined.  If we aren't still starting
            # up then there is probably a dependency cycle or a bug.
            self.state = None
        elif 'canceled' in dep_states:
            self.state = 'canceled'
        elif 'failed' in dep_states:
            self.state = 'canceled'
        elif 'blocked' in dep_states:
            self.state = 'blocked'
        elif 'free' in dep_states:
            self.state = 'blocked'
        elif 'open' in dep_states:
            self.state = 'blocked'
        else:
            assert ['done'] * len(dep_states) == dep_states
            self.state = 'free'

    def start(self, builder=None):
        """
        Start running this task.  This involves calling self.action in a
        new process.  The state is also set to 'open'.
        """
        self.__logger.info('Starting on {0}'.format(builder.ip))
        self.state = 'open'

        # Feed the action whichever stock parameters it asks for, as well as
        # any custom parameters given in self.custom_kwargs.
        all_kwargs = dict(self.custom_kwargs)
        stock_kwargs = {'dist':    self.dist,
                        'release': self.release,
                        'arch':    self.arch,
                        'pkg':     self.pkg,
                        'task_id': self.id,
                        'builder': builder,
                       }
        for (key, val) in stock_kwargs.iteritems():
            if key in inspect.getargspec(self.action).args:
                all_kwargs[key] = val

        self.__process = multiprocessing.Process(target=self.__bootstrap_action,
                kwargs=all_kwargs,
                name='Process-{0}-{1}'.format(self.id, self.action.__name__))
        self.__process.start()

    def __bootstrap_action(self, **kwargs):
        """
        Redirect all output to self.__outputfile (if it is not None) and then
        run the task method.
        """
        if self.__outputfile:
            if not os.path.exists(os.path.dirname(self.__outputfile)):
                try:
                    os.makedirs(os.path.dirname(self.__outputfile))
                except OSError:
                    # The directory was probably already there
                    pass
            outfile = open(self.__outputfile, 'w', 0)
            sys.stdout = outfile
            sys.stderr = outfile
        return self.action(**kwargs)

    def wait(self, timeout=None):
        """
        Wait up to timeout seconds (forever if timeout is None) for this task's
        action to terminate.  If and when it terminates, set this task's status
        to 'done' if its exit code is 0, or to 'failed' otherwise, and then
        return its exit code.
        """
        try:
            self.__process.join(timeout)
        except RuntimeError:
            # The process has yet to be started; skip it.
            return None
        except AssertionError as e:
            if str(e.args[0]).lower() == 'can only join a started process':
                # The process is only partially started; skip it.
                return None
            else:
                raise e
        except AttributeError:
            # On rare occasions this method gets called before self.__process is
            # assigned due to a race condition.
            return None
        if self.__process.exitcode is None:
            return None
        else:
            if self.__process.exitcode == 0:
                self.state = 'done'
            else:
                self.state = 'failed'
            self.__logger.info(self.state)
            return self.__process.exitcode

class TaskFactory(object):
    def __init__(self, outputfile_dir=None):
        self.__logger = logging.getLogger(self.__class__.__name__)
        self.__lock   = threading.Lock()
        self.__tasks  = []
        self.__last_task_id = 0
        self.__outputfile_dir = outputfile_dir

    def make_task(self, action, dist=None, release=None, arch=None, pkg=None,
                  **kwargs):
        """
        Get a task with a given action, target, and package combination.  If
        such a task already exists, return that one.  Otherwise, create a new
        one with a new task ID and return that.
        """
        with self.__lock:
            prev_tasks = [task for task in self.__tasks if
                          task.action  == action  and task.dist == dist and
                          task.release == release and task.arch == arch and
                          task.pkg     == pkg]
            if len(prev_tasks) == 0:
                self.__last_task_id += 1
                task_id = self.__last_task_id
                outputfile = self.__get_outputfile(action, task_id, dist,
                                                   release, arch, pkg)

                new_task = Task(action, dist=dist, release=release, arch=arch,
                                pkg=pkg, id=task_id, outputfile=outputfile,
                                **kwargs)
                self.__tasks.append(new_task)
                return new_task
            else:
                if len(prev_tasks) > 1:
                    ids = ', '.join([task.id for task in prev_tasks])
                    self.__logger.error('BUG: detected duplicate tasks ' + ids)
                return prev_tasks[0]

    def __get_outputfile(self, action, task_id, dist, release, arch, pkg):
        """
        Figure out where the output file for a new task, if any, should go.
        """
        if self.__outputfile_dir is None:
            return None
        # task-01-build_srpm-centos-5-x86_64-mypkg.log
        filename = 'task-{0:02}-{1}'.format(task_id, action.__name__)
        for val in [dist, release, arch, pkg]:
            if val is not None:
                filename += '-' + val
        filename += '.log'
        return os.path.join(self.__outputfile_dir, filename)

class TaskController(object):
    def __init__(self, tasks, builders):
        self.__logger           = logging.getLogger(self.__class__.__name__)
        self.__lock             = threading.Lock()
        self.__builder_pool     = BuilderPool(builders)
        self.__incomplete_tasks = collections.deque(tasks)
        self.__complete_tasks   = collections.deque()
        self.__free_tasks       = collections.deque()
        for task in self.__incomplete_tasks:
            task.register_handler(self.__on_task_state_change)

    def __state_checker(self):
        """
        Periodically check constraints on the lists of task states and correct
        list membership if any are violated.  Depending on their states, tasks
        should appear in the following lists:

                   free_tasks  incomplete_tasks  complete_tasks
        blocked                       X
        free            X             X
        open                          X
        done                                           X
        failed                                         X
        canceled                                       X
        """
        def chk_in(task, lst, lst_name):
            if task not in lst:
                self.__logger.error(('BUG: Constraint violation: task {0} with '
                                     'state {1} is missing from {2} task list, '
                                     'will be forcibly added')
                                     .format(task.id, task.state, lst_name))
                lst.append(task)

        def chk_not_in(task, lst, lst_name):
            if task in lst:
                self.__logger.error(('BUG: Constraint violation: task {0} with '
                                     'state {1} found in {2} task list, '
                                     'will be forcibly removed')
                                     .format(task.id, task.state, lst_name))
                lst.remove(task)

        while True:
            time.sleep(30)
            with self.__lock:
                all_tasks = set()
                all_tasks.update(self.__incomplete_tasks, self.__complete_tasks)
                for task in all_tasks:
                    if task.handler_depth > 0:
                        # If it's in the middle of running handlers it may be in
                        # the wrong list because the TaskController has yet to
                        # move it.
                        continue
                    elif task.state in ['blocked', 'open']:
                        chk_not_in(task, self.__free_tasks,       'free')
                        chk_in(    task, self.__incomplete_tasks, 'incomplete')
                        chk_not_in(task, self.__complete_tasks,   'complete')
                    elif task.state in ['done', 'failed', 'canceled']:
                        chk_not_in(task, self.__free_tasks,       'free')
                        chk_not_in(task, self.__incomplete_tasks, 'incomplete')
                        chk_in(    task, self.__complete_tasks,   'complete')
                    elif task.state == 'free':
                        chk_in(    task, self.__free_tasks,       'free')
                        chk_in(    task, self.__incomplete_tasks, 'incomplete')
                        chk_not_in(task, self.__complete_tasks,   'complete')
                    else:
                        msg = 'BUG: task {id} has invalid state {state}'.format(
                                id=task.id, state=task.state)
                        self.__logger.error(msg)

    def __deadlock_checker(self):
        """
        A rudimentary check for deadlocking.  If all tasks are blocked, try to
        resolve the deadlock by canceling one of them.
        """
        while True:
            time.sleep(30)
            with self.__lock:
                for task in self.__incomplete_tasks:
                    if task.state != 'blocked' or task.handler_depth > 0:
                        return
                else:
                    # All tasks are blocked
                    killable_task = random.choice(self.__incomplete_tasks)
                    msg = ('BUG: all incomplete tasks ({tasks}) deadlocked; '
                           'canceling random task {killable_id}').format(
                            killable_id=killable_task.id,
                            tasks=', '.join([task.id for task in
                                             self.__incomplete_tasks]))
                    self.__logger.error(msg)
            killable_task.state = 'canceled'

    def __status_printer(self):
        """
        Periodically print a summary of all tasks' states.
        """
        while True:
            time.sleep(300)
            with self.__lock:
                all_tasks = set()
                all_tasks.update(self.__incomplete_tasks, self.__complete_tasks)
                states = [task.state for task in all_tasks]
                done_pct = float(len(self.__complete_tasks)) / len(all_tasks)
                detail_bits = []
                for state in ['free', 'blocked', 'open', 'done', 'failed',
                              'canceled']:
                    if states.count(state) > 0:
                        detail_bits.append(str(states.count(state)) + ' ' +
                                           state)
                if states.count(None) > 0:
                    detail_bits.append(str(states.count(None)) + ' invalid')
                status = ('{complete} of {total} tasks ({pct}%) complete '
                          '({details})').format(pct=int(done_pct * 100),
                        complete=len(self.__complete_tasks),
                        total=len(all_tasks), details=', '.join(detail_bits))
            self.__logger.info(status)

    def run(self):
        """
        Run all tasks as they become unblocked and the hardware to run them
        becomes available.  If all tasks complete successfully, return True.
        Otherwise return False.
        """
        self.__logger.info(str(len(self.__incomplete_tasks)) + ' tasks total:')
        for task in sorted(self.__incomplete_tasks, key=lambda task: task.id):
            self.__logger.info('Task {0:2}: {1}'.format(task.id, str(task)))

        # Make every task read its initial state.  The callbacks this launcher
        # added to all of them will put 'free' tasks into the __free_tasks
        # queue.
        with self.__lock:
            incomplete_tasks_tmp = list(self.__incomplete_tasks)
        for task in incomplete_tasks_tmp:
            if self.__builder_pool.has_compatible_builder(task):
                task.reread_state()
            else:
                self.__logger.error('No compatible builder for task {0}'
                                    .format(task.id))
                task.state = 'canceled'

        for target in [self.__state_checker,  self.__deadlock_checker,
                       self.__status_printer, self.__task_starter]:
            daemon_thread = threading.Thread(target=target)
            daemon_thread.daemon = True
            daemon_thread.start()

        # Harvest tasks as they finish.
        while True:
            with self.__lock:
                if len(self.__incomplete_tasks) == 0:
                    # All done
                    break
                incomplete_tasks_tmp = list(self.__incomplete_tasks)
            for task in incomplete_tasks_tmp:
                if task.state == 'open':
                    task.wait(0)
            time.sleep(1)

        # Clean up
        with self.__lock:
            states = [task.state for task in self.__complete_tasks]
            if len(self.__incomplete_tasks) > 0:
                result = 'canceled'
            elif ['canceled'] * len(states) == states:
                result = 'canceled'
            elif ['done']     * len(states) == states:
                result = 'complete'
            else:
                result = 'failed'

            result_counts = []
            for state in ['done', 'failed', 'canceled']:
                if states.count(state) > 0:
                    result_counts.append(str(states.count(state)) + ' ' + state)
            self.__logger.info('Build {result} ({details})'.format(
                    result=result, details=', '.join(result_counts)))
            if result == 'complete':
                return True
            else:
                return False

    def __task_starter(self):
        """
        Start tasks that are free to run as soon as the hardware to run them
        becomes available.
        """
        while True:
            with self.__lock:
                free_tasks_tmp = list(self.__free_tasks)
            for task in free_tasks_tmp:
                builder = self.__builder_pool.get_compatible_builder(task)
                if builder:
                    task.start(builder=builder)
            time.sleep(5)

    def __on_task_state_change(self, task):
        """
        This handler manages tasks' memberships in the lists of free, complete,
        and incomplete tasks by adding them to or removing them from the
        appropriate lists as their states change.
        """
        with self.__lock:
            if task.state == 'free':
                if task not in self.__free_tasks:
                    self.__free_tasks.append(task)
                    # The task will be processed by the task-starting loop
            if task.state == 'open':
                if task in self.__free_tasks:
                    self.__free_tasks.remove(task)
            if task.state == 'canceled':
                self.__logger.warn('Task {0} canceled'.format(task.id))
            if task.state in ['done', 'failed', 'canceled']:
                if task in self.__incomplete_tasks:
                    self.__incomplete_tasks.remove(task)
                if task not in self.__complete_tasks:
                    self.__complete_tasks.append(task)

class BuilderPool(object):
    def __init__(self, builders):
        self.__logger        = logging.getLogger(self.__class__.__name__)
        self.__lock          = threading.Lock()
        self.__all_builders  = set(builders)
        self.__free_builders = set(builders)

    def get_compatible_builder(self, task):
        """
        Find a builder that is capable of running a task, given the task's
        target distribution, release, and architecture.  If one is available,
        remove it from the BuilderPool and then return it.  Otherwise, return
        None.

        When assigning a builder to a task the BuilderPool will register a state
        change handler with the task that returns the builder to the pool when
        the task completes.
        """
        with self.__lock:
            usable = self.__list_all_compatible_builders(task) & \
                    self.__free_builders
            if len(usable) > 0:
                builder = random.choice(list(usable))
                self.__logger.debug('Assigning builder {0} to task {1}'
                                    .format(builder.ip, task.id))
                self.__free_builders.remove(builder)
                task.register_handler(
                        lambda task: self.__on_task_state_change(builder, task))
                return builder
            else:
                self.__logger.debug('No builder available for task {0}'
                                    .format(task.id))
                return None

    def has_compatible_builder(self, task):
        """
        Return if any builder in the BuilderPool, whether assigned to a task or
        not, is capable of running a task for a given target distribution,
        release, and architecture.
        """
        return len(self.__list_all_compatible_builders(task)) > 0

    def __list_all_compatible_builders(self, task):
        """
        Get a list of all builders, whether assigned to tasks or not, are
        capable of running a task for a given target distribution, release, and
        architecture.
        """
        return set([b for b in self.__all_builders
                    if b.can_build(task.arch, task.dist, task.release)])

    def __on_task_state_change(self, builder, task):
        """
        When a task finishes, return the builder that was assigned to it to the
        pool of available builders.
        """
        if task.state in ['done', 'failed', 'canceled']:
            with self.__lock:
                if builder not in self.__free_builders:
                    self.__logger.debug('Releasing builder ' + builder.ip)
                    self.__free_builders.add(builder)
