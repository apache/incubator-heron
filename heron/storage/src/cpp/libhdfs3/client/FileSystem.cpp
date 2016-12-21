/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "client/FileSystem.h"

#include <algorithm>
#include <string>
#include <krb5/krb5.h>

#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/Hash.h"
#include "common/SessionConfig.h"
#include "common/Thread.h"
#include "common/Token.h"
#include "common/Unordered.h"
#include "common/WritableUtils.h"

#include "client/DirectoryIterator.h"
#include "client/FileSystemImpl.h"
#include "client/FileSystemKey.h"

// using namespace Hdfs::Internal;

namespace Hdfs {
namespace Internal {

static std::string ExtractPrincipalFromTicketCache(
    const std::string & cachePath) {
    krb5_context cxt = NULL;
    krb5_ccache ccache = NULL;
    krb5_principal principal = NULL;
    krb5_error_code ec = 0;
    std::string errmsg, retval;
    char * priName = NULL;

    if (!cachePath.empty()) {
        if (0 != setenv("KRB5CCNAME", cachePath.c_str(), 1)) {
            THROW(HdfsIOException, "Cannot set env parameter \"KRB5CCNAME\"");
        }
    }

    do {
        if (0 != (ec = krb5_init_context(&cxt))) {
            break;
        }

        if (0 != (ec = krb5_cc_default(cxt, &ccache))) {
            break;
        }

        if (0 != (ec = krb5_cc_get_principal(cxt, ccache, &principal))) {
            break;
        }

        if (0 != (ec = krb5_unparse_name(cxt, principal, &priName))) {
            break;
        }
    } while (0);

    if (!ec) {
        retval = priName;
    } else {
        if (cxt) {
            errmsg = krb5_get_error_message(cxt, ec);
        } else {
            errmsg = "Cannot initialize kerberos context";
        }
    }

    if (priName != NULL) {
        krb5_free_unparsed_name(cxt, priName);
    }

    if (principal != NULL) {
        krb5_free_principal(cxt, principal);
    }

    if (ccache != NULL) {
        krb5_cc_close(cxt, ccache);
    }

    if (cxt != NULL) {
        krb5_free_context(cxt);
    }

    if (!errmsg.empty()) {
        THROW(HdfsIOException,
              "FileSystem: Failed to extract principal from ticket cache: %s",
              errmsg.c_str());
    }

    return retval;
}


static std::string ExtractPrincipalFromToken(const Token & token) {
    std::string realUser, owner;
    std::string identifier = token.getIdentifier();
    WritableUtils cin(&identifier[0], identifier.size());
    char version;

    try {
        version = cin.readByte();

        if (version != 0) {
            THROW(HdfsIOException, "Unknown version of delegation token");
        }

        owner = cin.ReadText();
        cin.ReadText();
        realUser = cin.ReadText();
        return realUser.empty() ? owner : realUser;
    } catch (const std::range_error & e) {
    }

    THROW(HdfsIOException, "Cannot extract principal from token");
}
}

FileSystem::FileSystem(const Config & conf) :
    conf(conf), impl(NULL) {
}

FileSystem::FileSystem(const FileSystem & other) :
    conf(other.conf), impl(NULL) {
    if (other.impl) {
        impl = new FileSystemWrapper(other.impl->filesystem);
    }
}

FileSystem & FileSystem::operator =(const FileSystem & other) {
    if (this == &other) {
        return *this;
    }

    conf = other.conf;

    if (impl) {
        delete impl;
        impl = NULL;
    }

    if (other.impl) {
        impl = new FileSystemWrapper(other.impl->filesystem);
    }

    return *this;
}

FileSystem::~FileSystem() {
    if (impl) {
        try {
            disconnect();
        } catch (...) {
        }
    }
}

void FileSystem::connect() {
    Internal::SessionConfig sconf(conf);
    connect(sconf.getDefaultUri().c_str(), NULL, NULL);
}

/**
 * Connect to hdfs
 * @param uri hdfs connection uri, hdfs://host:port
 */
void FileSystem::connect(const char * uri) {
    connect(uri, NULL, NULL);
}

static FileSystemWrapper * ConnectInternal(const char * uri,
        const std::string & principal, const Token * token, Config & conf) {
    if (NULL == uri || 0 == strlen(uri)) {
        THROW(InvalidParameter, "Invalid HDFS uri.");
    }

    FileSystemKey key(uri, principal.c_str());

    if (token) {
        key.addToken(*token);
    }

    return new FileSystemWrapper(shared_ptr<FileSystemInter>(new FileSystemImpl(key, conf)));
}

/**
 * Connect to hdfs with user or token
 * 	username and token cannot be set at the same time
 * @param uri connection uri.
 * @param username user used to connect to hdfs
 * @param token token used to connect to hdfs
 */
void FileSystem::connect(const char * uri, const char * username, const char * token) {
    AuthMethod auth;
    std::string principal;

    if (impl) {
        THROW(HdfsIOException, "FileSystem: already connected.");
    }

    try {
        SessionConfig sconf(conf);
        auth = RpcAuth::ParseMethod(sconf.getRpcAuthMethod());

        if (token && auth != AuthMethod::SIMPLE) {
            Token t;
            t.fromString(token);
            principal = ExtractPrincipalFromToken(t);
            impl = ConnectInternal(uri, principal, &t, conf);
            impl->filesystem->connect();
            return;
        } else if (username) {
            principal = username;
        }

        if (auth == AuthMethod::KERBEROS) {
            principal = ExtractPrincipalFromTicketCache(sconf.getKerberosCachePath());
        }

        impl = ConnectInternal(uri, principal, NULL, conf);
        impl->filesystem->connect();
    } catch (...) {
        delete impl;
        impl = NULL;
        throw;
    }
}

/**
 * disconnect from hdfs
 */
void FileSystem::disconnect() {
    delete impl;
    impl = NULL;
}

/**
 * To get default number of replication.
 * @return the default number of replication.
 */
int FileSystem::getDefaultReplication() const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getDefaultReplication();
}

/**
 * To get the default number of block size.
 * @return the default block size.
 */
int64_t FileSystem::getDefaultBlockSize() const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getDefaultBlockSize();
}

/**
 * To get the home directory.
 * @return home directory.
 */
std::string FileSystem::getHomeDirectory() const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getHomeDirectory();
}

/**
 * To delete a file or directory.
 * @param path the path to be deleted.
 * @param recursive if path is a directory, delete the contents recursively.
 * @return return true if success.
 */
bool FileSystem::deletePath(const char * path, bool recursive) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->deletePath(path, recursive);
}

/**
 * To create a directory which given permission.
 * @param path the directory path which is to be created.
 * @param permission directory permission.
 * @return return true if success.
 */
bool FileSystem::mkdir(const char * path, const Permission & permission) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->mkdir(path, permission);
}

/**
 * To create a directory which given permission.
 * If parent path does not exits, create it.
 * @param path the directory path which is to be created.
 * @param permission directory permission.
 * @return return true if success.
 */
bool FileSystem::mkdirs(const char * path, const Permission & permission) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->mkdirs(path, permission);
}

/**
 * To get path information.
 * @param path the path which information is to be returned.
 * @return the path information.
 */
FileStatus FileSystem::getFileStatus(const char * path) const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getFileStatus(path);
}

/**
 * Return an array containing hostnames, offset and size of
 * portions of the given file.
 *
 * This call is most helpful with DFS, where it returns
 * hostnames of machines that contain the given file.
 *
 * The FileSystem will simply return an elt containing 'localhost'.
 *
 * @param path path is used to identify an FS since an FS could have
 *          another FS that it could be delegating the call to
 * @param start offset into the given file
 * @param len length for which to get locations for
 */
std::vector<BlockLocation> FileSystem::getFileBlockLocations(const char * path,
        int64_t start, int64_t len) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getFileBlockLocations(path, start, len);
}

/**
 * list the contents of a directory.
 * @param path the directory path.
 * @return Return a iterator to visit all elements in this directory.
 */
DirectoryIterator FileSystem::listDirectory(const char * path)  {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->listDirectory(path, false);
}

/**
 * list all the contents of a directory.
 * @param path The directory path.
 * @return Return a vector of file informations in the directory.
 */
std::vector<FileStatus> FileSystem::listAllDirectoryItems(const char * path) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->listAllDirectoryItems(path, false);
}

/**
 * To set the owner and the group of the path.
 * username and groupname cannot be empty at the same time.
 * @param path the path which owner of group is to be changed.
 * @param username new user name.
 * @param groupname new group.
 */
void FileSystem::setOwner(const char * path, const char * username,
                          const char * groupname) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->filesystem->setOwner(path, username, groupname);
}

/**
 * To set the access time or modification time of a path.
 * @param path the path which access time or modification time is to be changed.
 * @param mtime new modification time.
 * @param atime new access time.
 */
void FileSystem::setTimes(const char * path, int64_t mtime, int64_t atime) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->filesystem->setTimes(path, mtime, atime);
}

/**
 * To set the permission of a path.
 * @param path the path which permission is to be changed.
 * @param permission new permission.
 */
void FileSystem::setPermission(const char * path,
                               const Permission & permission) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->filesystem->setPermission(path, permission);
}

/**
 * To set the number of replication.
 * @param path the path which number of replication is to be changed.
 * @param replication new number of replication.
 * @return return true if success.
 */
bool FileSystem::setReplication(const char * path, short replication) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->setReplication(path, replication);
}

/**
 * To rename a path.
 * @param src old path.
 * @param dst new path.
 * @return return true if success.
 */
bool FileSystem::rename(const char * src, const char * dst) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->rename(src, dst);
}

/**
 * To set working directory.
 * @param path new working directory.
 */
void FileSystem::setWorkingDirectory(const char * path) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->filesystem->setWorkingDirectory(path);
}

/**
 * To get working directory.
 * @return working directory.
 */
std::string FileSystem::getWorkingDirectory() const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getWorkingDirectory();
}

/**
 * To test if the path exist.
 * @param path the path which is to be tested.
 * @return return true if the path exist.
 */
bool FileSystem::exist(const char * path) const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->exist(path);
}

/**
 * To get the file system status.
 * @return the file system status.
 */
FileSystemStats FileSystem::getStats() const {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getFsStats();
}

/**
 * Truncate the file in the indicated path to the indicated size.
 * @param src The path to the file to be truncated
 * @param size The size the file is to be truncated to
 *
 * @return true if and client does not need to wait for block recovery,
 * false if client needs to wait for block recovery.
 */
bool FileSystem::truncate(const char * src, int64_t size) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->truncate(src, size);
}

std::string FileSystem::getDelegationToken(const char * renewer) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getDelegationToken(renewer);
}

/**
 * Get a valid Delegation Token using the default user as renewer.
 *
 * @return Token
 * @throws IOException
 */
std::string FileSystem::getDelegationToken() {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return impl->filesystem->getDelegationToken();
}

/**
 * Renew an existing delegation token.
 *
 * @param token delegation token obtained earlier
 * @return the new expiration time
 * @throws IOException
 */
int64_t FileSystem::renewDelegationToken(const std::string & token) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    return  impl->filesystem->renewDelegationToken(token);
}

/**
 * Cancel an existing delegation token.
 *
 * @param token delegation token
 * @throws IOException
 */
void FileSystem::cancelDelegationToken(const std::string & token) {
    if (!impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->filesystem->cancelDelegationToken(token);
}

}
