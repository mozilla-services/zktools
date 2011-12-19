# ***** BEGIN LICENSE BLOCK *****
# Version: MPL 1.1/GPL 2.0/LGPL 2.1
#
# The contents of this file are subject to the Mozilla Public License Version
# 1.1 (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
# http://www.mozilla.org/MPL/
#
# Software distributed under the License is distributed on an "AS IS" basis,
# WITHOUT WARRANTY OF ANY KIND, either express or implied. See the License
# for the specific language governing rights and limitations under the
# License.
#
# The Original Code is Mozilla zktools.
#
# The Initial Developer of the Original Code is Mozilla Foundation.
# Portions created by the Initial Developer are Copyright (C) 2011
# the Initial Developer. All Rights Reserved.
#
# Contributor(s):
# Ben Bangert (bbangert@mozilla.com)
#
# Alternatively, the contents of this file may be used under the terms of
# either the GNU General Public License Version 2 or later (the "GPL"), or
# the GNU Lesser General Public License Version 2.1 or later (the "LGPL"),
# in which case the provisions of the GPL or the LGPL are applicable instead
# of those above. If you wish to allow use of your version of this file only
# under the terms of either the GPL or the LGPL, and not to allow others to
# use your version of this file under the terms of the MPL, indicate your
# decision by deleting the provisions above and replace them with the notice
# and other provisions required by the GPL or the LGPL. If you do not delete
# the provisions above, a recipient may use your version of this file under
# the terms of any one of the MPL, the GPL or the LGPL.
#
# ***** END LICENSE BLOCK *****
"""Zookeeper Configuration

This module provides a :class:`ZkConfig` object which can load itself from
a Zookeeper path, and serialize itself back. Values that appear to be a
boolean, int, decimal, or date/datetime will be automatically coerced
into the appropriate Python objects.

"""
import decimal
import re

import zookeeper

ZOO_OPEN_ACL_UNSAFE = {"perms": 0x1f, "scheme": "world", "id": "anyone"}


CONVERSIONS = {
    re.compile(r'^\d\.\d+$'): decimal.Decimal,
    re.compile(r'^\d+$'): int,
    re.compile(r'^true$', re.IGNORECASE): lambda x: True,
    re.compile(r'^false$', re.IGNORECASE): lambda x: False,
}


class ZkConfig(dict):
    """Zookeeper Configuration object

    This object has the same interface as a normal Python dict, and can
    be loaded or saved back to Zookeeper.

    A shared read/write lock is used so that multiple computers can read
    the configuration, but only a single writer is allowed to write it
    out. This is due to the multiple calls needed to fully read/write
    the entire config tree.

    Example::

        from zktools.connection import ZkConnection
        from zktools.configuration import ZkConfig

        conn = ZkConnection()
        config = ZkConfig(conn, '/Configurations/MyConfig')
        config.load()

        my_info = config['my_host_name']
        # ... etc ...

    """
    def __init__(self, connection, config_path, create_intermediary=False):
        """Create a Zookeeper configuration object

        :param connection: zookeeper connection object
        :type connection: ZkConnection instance
        :param config_path: A Zookeeper node path to where the configuration
                            should be loaded/saved from. Intermediary nodes
                            will not be created unless ``create_intermediary``
                            is ``True``.
        :type config_path: str

        """
        root_node = config_path[config_path.rfind('/'):]
        self.zk = connection
        self.config_path = config_path

        # This happens if the config_path is not a nested node
        if root_node != config_path and not connection.exists(root_node):
            raise Exception("Path to 'config_path' does not exist.")

    def load(self):
        """Load configuration from zookeeper"""
        if not self.zk.exists(self.config_path):
            raise Exception("No existing configuration found at path: %s" %
                            self.config_path)

        # First determine if its a sequence (list), or keys (dict)
        

    def save(self):
        self.zk.set(self.config_path, dumps(self))

    def copy(self):
        """Return a dict of ourself, not a ZkConfig object"""
        return dict(self.items())
