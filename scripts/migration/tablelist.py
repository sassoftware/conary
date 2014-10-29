#!/usr/bin/env python
#
# Copyright (c) SAS Institute Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# list of tables we migrate, in order
TableList = [
    'LatestMirror',
    'CommitLock',
    'Branches',
    'Items',
    'Versions',
    'Labels',
    'LabelMap',
    'Flavors',
    'FlavorMap',
    'FlavorScores',
    'Users',
    'UserGroups',
    'UserGroupMembers',
    'EntitlementGroups',
    'Entitlements',
    'EntitlementOwners',
    'EntitlementAccessMap',
    'Permissions',
    'FileStreams',
    'Nodes',
    'ChangeLogs',
    'Instances',
    'TroveInfo',
    'Dependencies',
    'Metadata',
    'MetadataItems',
    'PGPKeys',
    'PGPFingerprints',
    'Provides',
    'Requires',
    'TroveRedirects',
    'TroveTroves',
    'Dirnames',
    'Basenames',
    'FilePaths',
    'TroveFiles',
    'UserGroupTroves',
    'UserGroupInstancesCache',
    'UserGroupAllPermissions',
    'UserGroupAllTroves',
    'LatestCache'
    ]
