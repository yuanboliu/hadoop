<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

# Unix Shell Guide

<!-- MACRO{toc|fromDepth=0|toDepth=3} -->

Much of Apache Hadoop's functionality is controlled via [the shell](CommandsManual.html).  There are several ways to modify the default behavior of how these commands execute.

## Important End-User Environment Variables

Apache Hadoop has many environment variables that control various aspects of the software.  (See `hadoop-env.sh` and related files.)  Some of these environment variables are dedicated to helping end users manage their runtime.

### `HADOOP_CLIENT_OPTS`

This environment variable is used for almost all end-user operations.  It can be used to set any Java options as well as any Apache Hadoop options via a system property definition. For example:

```bash
HADOOP_CLIENT_OPTS="-Xmx1g -Dhadoop.socks.server=localhost:4000" hadoop fs -ls /tmp
```

will increase the memory and send this command via a SOCKS proxy server.

### `HADOOP_CLASSPATH`

  NOTE: Site-wide settings should be configured via a shellprofile entry and permanent user-wide settings should be configured via ${HOME}/.hadooprc using the `hadoop_add_classpath` function. See below for more information.

The Apache Hadoop scripts have the capability to inject more content into the classpath of the running command by setting this environment variable.  It should be a colon delimited list of directories, files, or wildcard locations.

```bash
HADOOP_CLASSPATH=${HOME}/lib/myjars/*.jar hadoop classpath
```

A user can provides hints to the location of the paths via the `HADOOP_USER_CLASSPATH_FIRST` variable.  Setting this to any value will tell the system to try and push these paths near the front.

### Auto-setting of Variables

If a user has a common set of settings, they can be put into the `${HOME}/.hadoop-env` file.  This file is always read to initialize and override any variables that the user may want to customize.  It uses bash syntax, similar to the `.bashrc` file:

For example:

```bash
#
# my custom Apache Hadoop settings!
#

HADOOP_CLIENT_OPTS="-Xmx1g"
```

The `.hadoop-env` file can also be used to extend functionality and teach Apache Hadoop new tricks.  For example, to run hadoop commands accessing the server referenced in the environment variable `${HADOOP_SERVER}`, the following in the `.hadoop-env` will do just that:

```bash

if [[ -n ${HADOOP_SERVER} ]]; then
  HADOOP_CONF_DIR=/etc/hadoop.${HADOOP_SERVER}
fi
```

One word of warning:  not all of Unix Shell API routines are available or work correctly in `.hadoop-env`.  See below for more information on `.hadooprc`.

## Administrator Environment

There are many environment variables that impact how the system operates.  By far, the most important are the series of `_OPTS` variables that control how daemons work.  These variables should contain all of the relevant settings for those daemons.

More, detailed information is contained in `hadoop-env.sh` and the other env.sh files.

Advanced administrators may wish to supplement or do some platform-specific fixes to the existing scripts.  In some systems, this means copying the errant script or creating a custom build with these changes.  Apache Hadoop provides the capabilities to do function overrides so that the existing code base may be changed in place without all of that work.  Replacing functions is covered later under the Shell API documentation.

## Developer and Advanced Administrator Environment

### Shell Profiles

Apache Hadoop allows for third parties to easily add new features through a variety of pluggable interfaces.  This includes a shell code subsystem that makes it easy to inject the necessary content into the base installation.

Core to this functionality is the concept of a shell profile.  Shell profiles are shell snippets that can do things such as add jars to the classpath, configure Java system properties and more.

Shell profiles may be installed in either `${HADOOP_CONF_DIR}/shellprofile.d` or `${HADOOP_HOME}/libexec/shellprofile.d`.  Shell profiles in the `libexec` directory are part of the base installation and cannot be overridden by the user.  Shell profiles in the configuration directory may be ignored if the end user changes the configuration directory at runtime.

An example of a shell profile is in the libexec directory.

### Shell API

Apache Hadoop's shell code has a [function library](./UnixShellAPI.html) that is open for administrators and developers to use to assist in their configuration and advanced feature management.  These APIs follow the standard [Apache Hadoop Interface Classification](./InterfaceClassification.html), with one addition: Replaceable.

The shell code allows for core functions to be overridden. However, not all functions can be or are safe to be replaced.  If a function is not safe to replace, it will have an attribute of Replaceable: No.  If a function is safe to replace, it will have the attribute of Replaceable: Yes.

In order to replace a function, create a file called `hadoop-user-functions.sh` in the `${HADOOP_CONF_DIR}` directory.  Simply define the new, replacement function in this file and the system will pick it up automatically.  There may be as many replacement functions as needed in this file.  Examples of function replacement are in the `hadoop-user-functions.sh.examples` file.

Functions that are marked Public and Stable are safe to use in shell profiles as-is.  Other functions may change in a minor release.

### User-level API Access

In addition to `.hadoop-env`, which allows individual users to override `hadoop-env.sh`, user's may also use `.hadooprc`.  This is called after the Apache Hadoop shell environment has been configured and allows the full set of shell API function calls.

For example:

```bash
hadoop_add_classpath /some/path/custom.jar
```

would go into `.hadooprc`

### Dynamic Subcommands

Utilizing the Shell API, it is possible for third parties to add their own subcommands to the primary Hadoop shell scripts (hadoop, hdfs, mapred, yarn).

Prior to executing a subcommand, the primary scripts will check for the existence of a (scriptname)\_subcommand\_(subcommand) function.  This function gets executed with the parameters set to all remaining command line arguments.  For example, if the following function is defined:

```bash
function yarn_subcommand_hello
{
  echo "$@"
  exit $?
}
```

then executing `yarn --debug hello world I see you` will activate script debugging and call the `yarn_subcommand_hello` function as:

```bash
yarn_subcommand_hello world I see you
```

which will result in the output of:

```bash
world I see you
```

It is also possible to add the new subcommands to the usage output. The `hadoop_add_subcommand` function adds text to the usage output.  Utilizing the standard HADOOP_SHELL_EXECNAME variable, we can limit which command gets our new function.

```bash
if [[ "${HADOOP_SHELL_EXECNAME}" = "yarn" ]]; then
  hadoop_add_subcommand "hello" "Print some text to the screen"
fi
```

This functionality may also be use to override the built-ins.  For example, defining:

```bash
function hdfs_subcommand_fetchdt
{
  ...
}
```

... will replace the existing `hdfs fetchdt` subcommand with a custom one.

Some key environment variables related to Dynamic Subcommands:

* HADOOP\_CLASSNAME

This is the name of the Java class to use when program execution continues.

* HADOOP\_SHELL\_EXECNAME

This is the name of the script that is being executed.  It will be one of hadoop, hdfs, mapred, or yarn.

* HADOOP\_SUBCMD

This is the subcommand that was passed on the command line.

* HADOOP\_SUBCMD\_ARGS

This array contains the argument list after the Apache Hadoop common argument processing has taken place and is the same list that is passed to the subcommand function as arguments.  For example, if `hadoop --debug subcmd 1 2 3` has been executed on the command line, then `${HADOOP_SUBCMD_ARGS[0]}` will be 1 and `hadoop_subcommand_subcmd` will also have $1 equal to 1.  This array list MAY be modified by subcommand functions to add or delete values from the argument list for further processing.

* HADOOP\_SUBCMD\_SECURESERVICE

If this command should/will be executed as a secure daemon, set this to true.

* HADOOP\_SUBCMD\_SECUREUSER

If this command should/will be executed as a secure daemon, set the user name to be used.

* HADOOP\_SUBCMD\_SUPPORTDAEMONIZATION

If this command can be executed as a daemon, set this to true.

* HADOOP\_USER\_PARAMS

This is the full content of the command line, prior to any parsing done. It will contain flags such as `--debug`.  It MAY NOT be manipulated.

The Apache Hadoop runtime facilities require functions exit if no further processing is required.  For example, in the hello example above, Java and other facilities were not required so a simple `exit $?` was sufficient.  However, if the function were to utilize `HADOOP_CLASSNAME`, then program execution must continue so that Java with the Apache Hadoop-specific parameters will be launched against the given Java class. Another example would be in the case of an unrecoverable error.  It is the function's responsibility to print an appropriate message (preferably using the hadoop_error API call) and exit appropriately.
